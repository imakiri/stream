package src

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/codec"
	"github.com/imakiri/stream/src/observer"
	"github.com/imakiri/stream/src/wire"
)

// ClientConfig configures a client: a viewer that gets the stream from the
// server through the proxy and hands it to a local player such as ffplay.
type ClientConfig struct {
	Proxy         ProxyConfig
	PlayerPort    uint16 // local UDP port the player reads the mpegts stream from
	Fec           FecConfig
	Decoder       DecoderConfig
	Credentials   wire.Credentials
	Verifier      wire.CertificateVerifier
	ServerID      uuid.UUID         // the server's certificate ID; no other certificate may serve the stream
	InputReporter observer.Reporter // optional: speed of what comes from the proxy
}

const (
	// clientHelloRetryInterval is how often a client repeats its hello while
	// it has no working stream key.
	clientHelloRetryInterval = time.Second
	// clientHelloAttemptTimeout starts a new attempt, with a new ephemeral
	// key, when the server did not answer the current one.
	clientHelloAttemptTimeout = 5 * time.Second
	// streamStaleTimeout starts a new handshake when no data packet opened
	// for this long, e.g. the server restarted with a new stream key.
	streamStaleTimeout = 3 * time.Second
)

// Client handshakes with the server, opens the data packets with the stream
// key, restores the stream with FEC and sends it to the player.
type Client struct {
	cfg       ClientConfig
	decoder   *codec.Decoder
	handshake wire.ClientHandshaker

	opener           atomic.Pointer[wire.StreamOpener]
	lastOpenUnixNano atomic.Int64
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.ServerID == uuid.Nil {
		return nil, errors.New("server id is not set")
	}
	decoder, err := codec.NewDecoder(cfg.Fec.DataParts, cfg.Fec.TotalParts, cfg.Decoder.AssemblerSize, cfg.Decoder.DispatcherSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create decoder: %w", err)
	}
	return &Client{
		cfg:     cfg,
		decoder: decoder,
		handshake: &wire.CertificateClientHandshake{
			Credentials: cfg.Credentials,
			Verifier:    cfg.Verifier,
			ServerID:    cfg.ServerID,
		},
	}, nil
}

// Run plays the stream until ctx is done.
func (c *Client) Run(ctx context.Context) error {
	var dialer net.Dialer
	player, err := dialer.DialContext(ctx, "udp4", fmt.Sprintf("127.0.0.1:%d", c.cfg.PlayerPort))
	if err != nil {
		return fmt.Errorf("failed to dial the player: %w", err)
	}
	defer player.Close()
	_ = player.(*net.UDPConn).SetWriteBuffer(socketBufferSize)
	log.Printf("sending the stream to the player at udp/%s", player.RemoteAddr())

	link, err := DialProxy(ctx, c.cfg.Proxy, wire.RoleClient, c.cfg.Credentials.Certificate.ID)
	if err != nil {
		return err
	}
	var input net.Conn = link
	if c.cfg.InputReporter != nil {
		reader, err := observer.NewReader(link, c.cfg.InputReporter, time.Second)
		if err != nil {
			return err
		}
		defer reader.Close()
		input = &readObservedConn{Conn: link, reader: reader}
	}

	decoderIn, decoderOut, err := c.decoder.Decode(ctx)
	if err != nil {
		return fmt.Errorf("failed to start decoder: %w", err)
	}

	go c.keepHandshake(ctx, link)
	go c.receive(ctx, input, decoderIn)
	go c.play(player, decoderOut)

	<-ctx.Done()
	return nil
}

func (c *Client) streamWorks(now time.Time) bool {
	return c.opener.Load() != nil && now.Sub(time.Unix(0, c.lastOpenUnixNano.Load())) < streamStaleTimeout
}

// keepHandshake sends client hellos while the client has no working stream
// key: at start, after the server restarted, after a long outage.
func (c *Client) keepHandshake(ctx context.Context, link net.Conn) {
	var ticker = time.NewTicker(clientHelloRetryInterval)
	defer ticker.Stop()
	var hello []byte
	var helloAt time.Time
	for {
		var now = time.Now()
		if !c.streamWorks(now) {
			if hello == nil || now.Sub(helloAt) > clientHelloAttemptTimeout || c.opener.Load() != nil {
				var err error
				hello, err = c.handshake.NewClientHello()
				if err != nil {
					log.Printf("failed to create client hello: %s", err)
				}
				helloAt = now
				c.opener.Store(nil)
			}
			if hello != nil {
				if _, err := link.Write(hello); err != nil && !errors.Is(err, net.ErrClosed) {
					log.Printf("failed to send client hello: %s", err)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// receive takes server hellos and data packets from the proxy.
func (c *Client) receive(ctx context.Context, link net.Conn, decoderIn chan<- []byte) {
	var buf = make([]byte, packetBufferSize)
	for {
		n, err := link.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("failed to read from proxy: %s", err)
			}
			return
		}
		switch wire.PacketKind(buf[:n]) {
		case wire.KindServerHello:
			opener, err := c.handshake.FinishServerHello(buf[:n], time.Now())
			if err != nil {
				log.Printf("rejected server hello: %s", err)
				continue
			}
			c.lastOpenUnixNano.Store(time.Now().UnixNano())
			c.opener.Store(&opener)
			log.Printf("handshake with server %s, stream key epoch %08x", c.cfg.ServerID, opener.Epoch())
		case wire.KindData:
			var opener = c.opener.Load()
			if opener == nil {
				continue
			}
			payload, err := (*opener).OpenData(buf[:n])
			if err != nil {
				continue // another stream key until the handshake catches up, or forged
			}
			c.lastOpenUnixNano.Store(time.Now().UnixNano())
			select {
			case <-ctx.Done():
				return
			case decoderIn <- payload:
			}
		}
	}
}

// playerDatagramSize is 7 mpegts packets of 188 bytes, the usual datagram of
// mpegts over UDP: the player gets the restored chunks in such pieces rather
// than as one datagram per chunk, which would exceed the MTU of anything but
// loopback and the default buffers of players like ffplay.
const playerDatagramSize = 7 * 188

// play sends the restored stream to the player.
func (c *Client) play(player net.Conn, decoderOut <-chan []byte) {
	for data := range decoderOut {
		for len(data) > 0 {
			var piece = data[:min(len(data), playerDatagramSize)]
			data = data[len(piece):]
			if _, err := player.Write(piece); err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				log.Printf("failed to send to the player: %s", err)
			}
		}
	}
}
