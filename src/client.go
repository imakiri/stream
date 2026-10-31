package src

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	lastOpenUnixNano atomic.Int64 // a server hello or a data packet opened
	// lastDataUnixNano is when the latest data packet opened; zero before
	// the first. A server without input sends none, and the client
	// handshakes again every few seconds.
	lastDataUnixNano atomic.Int64

	// lastHelloSentUnixNano is when the latest client hello went out; the
	// server hello that answers it gives the round trip for the clock offset.
	lastHelloSentUnixNano atomic.Int64
	// serverClockOffsetNanos is the server clock minus the client clock,
	// estimated at the handshake; valid once serverClockKnown is set.
	serverClockOffsetNanos atomic.Int64
	serverClockKnown       atomic.Bool
	e2eDelays              e2eDelays
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
	defer link.Close()
	var input io.ReadWriter = link
	if c.cfg.InputReporter != nil {
		reader, err := observer.NewReader(link, c.cfg.InputReporter, time.Second)
		if err != nil {
			return err
		}
		defer reader.Close()
		input = &readObservedLink{ReadWriter: link, reader: reader}
	}

	decoderIn, decoderOut, err := c.decoder.Decode(ctx)
	if err != nil {
		return fmt.Errorf("failed to start decoder: %w", err)
	}

	go c.keepHandshake(ctx, link)
	go c.receive(ctx, input, decoderIn)
	go c.play(player, decoderOut)
	go c.reportStream(ctx)

	<-ctx.Done()
	return nil
}

func (c *Client) streamWorks(now time.Time) bool {
	return c.opener.Load() != nil && now.Sub(time.Unix(0, c.lastOpenUnixNano.Load())) < streamStaleTimeout
}

// keepHandshake sends client hellos while the client has no working stream
// key: at start, after the server restarted, after a long outage.
func (c *Client) keepHandshake(ctx context.Context, link io.Writer) {
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
				c.lastHelloSentUnixNano.Store(time.Now().UnixNano())
				if _, err := link.Write(hello); err != nil {
					return // closed; the link logs its own send failures
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
// A handshake is logged only when it brings another stream key: while the
// server sends no data the client handshakes every few seconds and gets the
// same key, and reportStream tells that once.
func (c *Client) receive(ctx context.Context, link io.Reader, decoderIn chan<- []byte) {
	var buf = make([]byte, packetBufferSize)
	var loggedEpoch uint32
	var epochLogged bool
	for {
		n, err := link.Read(buf)
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Printf("failed to read from proxy: %s", err)
			continue
		}
		switch wire.PacketKind(buf[:n]) {
		case wire.KindServerHello:
			var receivedAt = time.Now()
			opener, serverClock, err := c.handshake.FinishServerHello(buf[:n], receivedAt)
			if err != nil {
				log.Printf("rejected server hello: %s", err)
				continue
			}
			c.lastOpenUnixNano.Store(receivedAt.UnixNano())
			c.opener.Store(&opener)
			var clock = c.syncServerClock(serverClock, receivedAt)
			if !epochLogged || opener.Epoch() != loggedEpoch {
				log.Printf("handshake with server %s, stream key epoch %08x; %s", c.cfg.ServerID, opener.Epoch(), clock)
				loggedEpoch, epochLogged = opener.Epoch(), true
			}
		case wire.KindData:
			var opener = c.opener.Load()
			if opener == nil {
				continue
			}
			payload, err := (*opener).OpenData(buf[:n])
			if err != nil {
				continue // another stream key until the handshake catches up, or forged
			}
			var openedAt = time.Now().UnixNano()
			c.lastOpenUnixNano.Store(openedAt)
			c.lastDataUnixNano.Store(openedAt)
			select {
			case <-ctx.Done():
				return
			case decoderIn <- payload:
			}
		}
	}
}

// syncServerClock estimates the server clock offset from a server hello
// stamped at serverClock and received at receivedAt, NTP style: the server
// answered half a round trip before receivedAt. The error is the asymmetry of
// the path, at most half the round trip. The round trip is taken from the
// latest client hello, which is right while it is shorter than
// clientHelloRetryInterval. It returns the estimate for the log.
func (c *Client) syncServerClock(serverClock, receivedAt time.Time) string {
	var roundTrip = receivedAt.Sub(time.Unix(0, c.lastHelloSentUnixNano.Load()))
	var offset = serverClock.Add(roundTrip / 2).Sub(receivedAt)
	c.serverClockOffsetNanos.Store(int64(offset))
	c.serverClockKnown.Store(true)
	return fmt.Sprintf("server clock minus this clock %s ± %s (handshake round trip %s)",
		offset.Round(time.Millisecond), (roundTrip / 2).Round(time.Millisecond), roundTrip.Round(time.Millisecond))
}

// serverNow returns the current time by the server clock, if it is known.
func (c *Client) serverNow() (time.Time, bool) {
	if !c.serverClockKnown.Load() {
		return time.Time{}, false
	}
	return time.Now().Add(time.Duration(c.serverClockOffsetNanos.Load())), true
}

// reportStream logs the end-to-end delay every e2eDelayReportInterval while
// data comes, and once when the data stops or comes again.
func (c *Client) reportStream(ctx context.Context) {
	var ticker = time.NewTicker(e2eDelayReportInterval)
	defer ticker.Stop()
	var flow dataFlow
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if change := flow.update(c.lastDataUnixNano.Load(), now); change != "" {
				log.Print(change)
			}
			if samples := c.e2eDelays.take(); len(samples) > 0 {
				log.Print(formatE2EDelays(samples))
			}
		}
	}
}

// playerDatagramSize is 7 mpegts packets of 188 bytes, the usual datagram of
// mpegts over UDP: the player gets the restored chunks in such pieces rather
// than as one datagram per chunk, which would exceed the MTU of anything but
// loopback and the default buffers of players like ffplay.
const playerDatagramSize = 7 * 188

// playerRecoveryTime is how long writes to the player must go without an
// error before it counts as running again: on a connected UDP socket the
// refusal of one datagram fails the next write only, so writes to a closed
// port alternate between failing and passing.
const playerRecoveryTime = time.Second

// play sends the restored stream to the player and takes the end-to-end delay
// of every chunk. A player that is not running makes the writes fail
// (connection refused on loopback); that is logged once, and once more when
// the player takes the stream again.
func (c *Client) play(player net.Conn, decoderOut <-chan codec.DecodedChunk) {
	var playerFailing bool
	var lastFailure time.Time
	for chunk := range decoderOut {
		if serverNow, ok := c.serverNow(); ok {
			c.e2eDelays.add(codec.InputDelay(chunk.InputUnixMilli, serverNow))
		}
		var data = chunk.Data
		for len(data) > 0 {
			var piece = data[:min(len(data), playerDatagramSize)]
			data = data[len(piece):]
			_, err := player.Write(piece)
			switch {
			case errors.Is(err, net.ErrClosed):
				return
			case err != nil:
				if !playerFailing {
					log.Printf("failed to send to the player, is it running? %s; dropping the stream until it takes it", err)
					playerFailing = true
				}
				lastFailure = time.Now()
			case playerFailing && time.Since(lastFailure) > playerRecoveryTime:
				log.Printf("the player takes the stream again")
				playerFailing = false
			}
		}
	}
}
