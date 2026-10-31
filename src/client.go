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
	"github.com/imakiri/stream/src/latencytrace"
	"github.com/imakiri/stream/src/ntpclock"
	"github.com/imakiri/stream/src/observer"
	"github.com/imakiri/stream/src/wire"
)

// ClientConfig configures a client: a viewer that gets the stream from the
// server through the relay and hands it to a local player such as ffplay.
type ClientConfig struct {
	Relay         RelayConfig
	PlayerPort    uint16 // local UDP port the player reads the mpegts stream from
	Fec           FecConfig
	Decoder       DecoderConfig
	Credentials   wire.Credentials
	Verifier      wire.CertificateVerifier
	ServerID      uuid.UUID         // the server's certificate ID; no other certificate may serve the stream
	InputReporter observer.Reporter // optional: speed of what comes from the relay
	// NTPServer gives the time the end-to-end delay is measured against,
	// the one the server stamps the chunks with. Empty, or before the first
	// answer: the server clock estimated at the handshake.
	NTPServer string
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
	clock     *ntpclock.Clock
	decoder   *codec.Decoder
	handshake wire.ClientHandshaker

	session          atomic.Pointer[wire.ClientSession] // the keys of the last handshake; nil while there is none
	lastOpenUnixNano atomic.Int64                       // a server hello or a data packet opened
	// lastDataUnixNano is when the latest data packet opened; zero before
	// the first. A server without input sends none, and the client
	// handshakes again every few seconds.
	lastDataUnixNano atomic.Int64

	// lastHelloSentUnixNano is when the latest client hello went out; the
	// server hello that answers it gives the round trip for the clock offset.
	lastHelloSentUnixNano atomic.Int64
	// serverClock is the server clock estimated at the last handshake;
	// nil before the first.
	serverClock atomic.Pointer[serverClockSync]
	e2eDelays   e2eDelays
	// trace records the stages of the chunks and packets; nil while no
	// trace runs, see StartTrace.
	trace atomic.Pointer[latencytrace.Trace]
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
		clock:   ntpclock.New(cfg.NTPServer),
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

	link, err := DialRelay(ctx, c.cfg.Relay, wire.RoleClient, c.cfg.Credentials.Certificate.ID)
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

	go c.clock.Run(ctx)
	go c.keepHandshake(ctx, link)
	go c.receive(ctx, input, decoderIn)
	go c.play(player, decoderOut)
	go c.reportStream(ctx, link)

	<-ctx.Done()
	return nil
}

func (c *Client) streamWorks(now time.Time) bool {
	return c.session.Load() != nil && now.Sub(time.Unix(0, c.lastOpenUnixNano.Load())) < streamStaleTimeout
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
			if hello == nil || now.Sub(helloAt) > clientHelloAttemptTimeout || c.session.Load() != nil {
				var err error
				hello, err = c.handshake.NewClientHello()
				if err != nil {
					log.Printf("failed to create client hello: %s", err)
				}
				helloAt = now
				c.session.Store(nil)
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

// receive takes server hellos and data packets from the relay.
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
			log.Printf("failed to read from relay: %s", err)
			continue
		}
		switch wire.PacketKind(buf[:n]) {
		case wire.KindServerHello:
			var receivedAt = time.Now()
			session, serverClock, err := c.handshake.FinishServerHello(buf[:n], receivedAt)
			if err != nil {
				log.Printf("rejected server hello: %s", err)
				continue
			}
			c.lastOpenUnixNano.Store(receivedAt.UnixNano())
			c.session.Store(&session)
			var clock = c.syncServerClock(serverClock, receivedAt)
			if epoch := session.Stream.Epoch(); !epochLogged || epoch != loggedEpoch {
				log.Printf("handshake with server %s, stream key epoch %08x; %s", c.cfg.ServerID, epoch, clock)
				loggedEpoch, epochLogged = epoch, true
			}
		case wire.KindData:
			var session = c.session.Load()
			if session == nil {
				continue
			}
			payload, err := session.Stream.OpenData(buf[:n])
			if err != nil {
				continue // another stream key until the handshake catches up, or forged
			}
			c.trace.Load().Packet(latencytrace.ClientReceived, payload)
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

// serverClockSync is the server clock at one moment of this client.
type serverClockSync struct {
	server time.Time // the server clock, no monotonic reading
	local  time.Time // the same moment here, with Go's monotonic reading
}

// syncServerClock estimates the server clock from a server hello stamped at
// serverClock and received at receivedAt, NTP style: the server answered half
// a round trip before receivedAt. The error is the asymmetry of the path, at
// most half the round trip. The round trip is taken from the latest client
// hello, which is right while it is shorter than clientHelloRetryInterval. It
// returns the estimate for the log: against the NTP time when the clock is
// synced, which shows how far the server is off it (the server's own NTP
// failed or is off), else against this machine's clock.
func (c *Client) syncServerClock(serverClock, receivedAt time.Time) string {
	var roundTrip = receivedAt.Sub(time.Unix(0, c.lastHelloSentUnixNano.Load()))
	var sync = serverClockSync{server: serverClock.Add(roundTrip / 2), local: receivedAt}
	c.serverClock.Store(&sync)
	if trueTime, synced := c.clock.At(receivedAt); synced {
		var ntpError, _ = c.clock.Error()
		return fmt.Sprintf("server clock minus true time %s ± %s (handshake round trip %s, %s)",
			sync.server.Sub(trueTime).Round(time.Millisecond), (roundTrip/2 + ntpError).Round(time.Millisecond),
			roundTrip.Round(time.Millisecond), c.clock)
	}
	return fmt.Sprintf("server clock minus this clock %s ± %s (handshake round trip %s)",
		sync.server.Sub(receivedAt.Round(0)).Round(time.Millisecond), (roundTrip / 2).Round(time.Millisecond), roundTrip.Round(time.Millisecond))
}

// serverNow returns the current time by the clock the server stamps the
// chunks with, if it is known. That is the NTP time once the clock is
// synced, as both ends ask NTP; the handshake estimate is off by the
// asymmetry of the path, up to half the round trip. Without NTP it is the
// estimate, counted from the last handshake on the monotonic clock, not on
// this machine's wall clock: a wall clock set after the handshake, as
// Windows time sync does, would shift every delay by the step, and a step of
// more than the delay made the delays negative.
func (c *Client) serverNow() (time.Time, bool) {
	var now = time.Now()
	if trueTime, synced := c.clock.At(now); synced {
		return trueTime, true
	}
	var sync = c.serverClock.Load()
	if sync == nil {
		return time.Time{}, false
	}
	return sync.server.Add(now.Sub(sync.local)), true
}

// reportStream logs the end-to-end delay and the lost chunks every
// e2eDelayReportInterval while data comes, and once when the data stops or
// comes again. It sends the same numbers to the server as a client report,
// also while no data comes, so the server knows the client is there.
func (c *Client) reportStream(ctx context.Context, link io.Writer) {
	var ticker = time.NewTicker(e2eDelayReportInterval)
	defer ticker.Stop()
	var flow dataFlow
	var reportedLost, reportedIncomplete uint64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if change := flow.update(c.lastDataUnixNano.Load(), now); change != "" {
				log.Print(change)
			}
			var lost, incomplete = c.decoder.Losses()
			var report = newClientReport(c.e2eDelays.take(), lost-reportedLost, incomplete-reportedIncomplete)
			reportedLost, reportedIncomplete = lost, incomplete
			if report.Chunks > 0 {
				log.Print(formatStreamReport(report))
			}
			if session := c.session.Load(); session != nil {
				_, _ = link.Write(session.Reports.SealClientReport(report))
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
		c.trace.Load().Chunk(latencytrace.ClientOut, chunk.Session, chunk.CSN, time.Time{})
	}
}

// StartTrace starts writing the latency trace of the client to path, by the
// NTP time, replacing a running one; see package latencytrace.
func (c *Client) StartTrace(path string) error {
	trace, err := latencytrace.Create(path, func() time.Time {
		now, _ := c.clock.Now()
		return now
	})
	if err != nil {
		return err
	}
	c.decoder.SetTrace(trace)
	if previous := c.trace.Swap(trace); previous != nil {
		_ = previous.Close()
	}
	return nil
}

// StopTrace stops the latency trace and writes it out.
func (c *Client) StopTrace() error {
	c.decoder.SetTrace(nil)
	return c.trace.Swap(nil).Close()
}

// TrueNow returns the NTP time and whether the clock is synced; before the
// first sync, or with NTP off, it is the local clock.
func (c *Client) TrueNow() (time.Time, bool) {
	return c.clock.Now()
}
