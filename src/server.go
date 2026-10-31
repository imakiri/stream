package src

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/imakiri/stream/src/codec"
	"github.com/imakiri/stream/src/liveoutput"
	"github.com/imakiri/stream/src/ntpclock"
	"github.com/imakiri/stream/src/observer"
	"github.com/imakiri/stream/src/wire"
)

// FecConfig is the Reed-Solomon split of a chunk; server and clients must
// use the same.
type FecConfig struct {
	DataParts  uint64
	TotalParts uint64
}

type EncoderConfig struct {
	DispatcherTimeout time.Duration
	DispatcherSize    uint64
}

type DecoderConfig struct {
	AssemblerSize  uint64
	DispatcherSize uint64
}

// ServerConfig configures the stream server: the source that takes the
// stream from OBS and sends it to every client through the relay.
type ServerConfig struct {
	Relay           RelayConfig
	StreamInputPort uint16 // local UDP port OBS sends the mpegts stream to
	PacingRate      uint64 // bits per second the data packets are spread at; 0: sent as the encoder hands them over
	Fec             FecConfig
	Encoder         EncoderConfig
	Credentials     wire.Credentials
	Verifier        wire.CertificateVerifier
	OutputReporter  observer.Reporter  // optional: speed of what goes to the relay
	ViewersOutput   *liveoutput.Output // optional: a status line with the clients that watch
	// NTPServer gives the time the chunks and the server hellos are stamped
	// with; the clients measure the end-to-end delay against the same NTP
	// time. Empty: the local clock.
	NTPServer string
}

// Server takes the stream from OBS, adds FEC, seals it with the stream key
// and sends one copy to the relay, which hands it to every client. Clients
// get the stream key from the handshake.
type Server struct {
	cfg       ServerConfig
	clock     *ntpclock.Clock
	encoder   *codec.Encoder
	handshake wire.ServerHandshaker
	sealer    wire.StreamSealer
	viewers   *viewers
}

func NewServer(cfg ServerConfig) (*Server, error) {
	encoder, err := codec.NewEncoder(cfg.Encoder.DispatcherTimeout, cfg.Encoder.DispatcherSize, cfg.Fec.DataParts, cfg.Fec.TotalParts)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %w", err)
	}
	streamKey, err := wire.NewStreamKey()
	if err != nil {
		return nil, fmt.Errorf("failed to create stream key: %w", err)
	}
	sealer, err := wire.NewStreamSealer(streamKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream sealer: %w", err)
	}
	var clock = ntpclock.New(cfg.NTPServer)
	encoder.InputClock = func() time.Time {
		now, _ := clock.Now()
		return now
	}
	return &Server{
		cfg:     cfg,
		clock:   clock,
		encoder: encoder,
		handshake: &wire.CertificateServerHandshake{
			Credentials: cfg.Credentials,
			Verifier:    cfg.Verifier,
			StreamKey:   streamKey,
		},
		sealer:  sealer,
		viewers: newViewers(cfg.ViewersOutput),
	}, nil
}

// Run serves the stream until ctx is done.
func (s *Server) Run(ctx context.Context) error {
	var listenConfig net.ListenConfig
	streamInput, err := listenConfig.ListenPacket(ctx, "udp4", fmt.Sprintf("127.0.0.1:%d", s.cfg.StreamInputPort))
	if err != nil {
		return fmt.Errorf("failed to listen for the stream from OBS: %w", err)
	}
	defer streamInput.Close()
	log.Printf("waiting for the stream from OBS at udp/%s", streamInput.LocalAddr())

	link, err := DialRelay(ctx, s.cfg.Relay, wire.RoleServer, s.cfg.Credentials.Certificate.ID)
	if err != nil {
		return err
	}
	defer link.Close()
	var output io.ReadWriter = link
	if s.cfg.OutputReporter != nil {
		writer, err := observer.NewWriter(link, s.cfg.OutputReporter, time.Second)
		if err != nil {
			return err
		}
		defer writer.Close()
		output = &writeObservedLink{ReadWriter: link, writer: writer}
	}

	encoderIn, encoderOut, err := s.encoder.Encode(ctx)
	if err != nil {
		return fmt.Errorf("failed to start encoder: %w", err)
	}

	go s.clock.Run(ctx)
	go s.readStreamInput(ctx, streamInput, encoderIn)
	go s.sendStream(output, encoderOut)
	go s.serveClientPackets(output)
	go s.watchViewers(ctx)

	<-ctx.Done()
	return nil
}

// readStreamInput feeds the datagrams from OBS to the encoder.
func (s *Server) readStreamInput(ctx context.Context, streamInput net.PacketConn, encoderIn chan<- []byte) {
	for {
		var buf = make([]byte, packetBufferSize)
		n, _, err := streamInput.ReadFrom(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("failed to read the stream from OBS: %s", err)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case encoderIn <- buf[:n]:
		}
	}
}

// sendStream seals every FEC packet into a data packet for the relay, paced
// at cfg.PacingRate. The link drops packets while it is down and logs its
// own failures, so a write error means it is closed.
func (s *Server) sendStream(link io.Writer, encoderOut <-chan []byte) {
	var pacer = newPacer(s.cfg.PacingRate)
	for packet := range encoderOut {
		var data = s.sealer.SealData(packet)
		pacer.wait(len(data))
		if _, err := link.Write(data); err != nil {
			return
		}
	}
}

// serveClientPackets answers the client hellos and takes the client
// reports the relay hands over.
func (s *Server) serveClientPackets(link io.ReadWriter) {
	var buf = make([]byte, packetBufferSize)
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
		case wire.KindClientHello:
			var now = time.Now()
			var serverClock, _ = s.clock.At(now)
			serverHello, clientID, reportKey, err := s.handshake.AnswerClientHello(buf[:n], serverClock)
			if err != nil {
				log.Printf("rejected client hello: %s", err)
				continue
			}
			if _, err := link.Write(serverHello); err != nil {
				return
			}
			s.viewers.handshake(clientID, reportKey, now)
			log.Printf("handshake with client %s", clientID)
		case wire.KindClientReport:
			if err := s.viewers.report(buf[:n], time.Now()); err != nil {
				log.Printf("rejected client report: %s", err)
			}
		}
	}
}

// watchViewers notes the clients that stop reporting and logs the list of
// clients every viewerSummaryInterval while there are any.
func (s *Server) watchViewers(ctx context.Context) {
	var check = time.NewTicker(ClientReportInterval)
	defer check.Stop()
	var summary = time.NewTicker(viewerSummaryInterval)
	defer summary.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-check.C:
			s.viewers.check(now)
		case now := <-summary.C:
			if line, hasViewers := s.viewers.summary(now); hasViewers {
				log.Print(line)
			}
		}
	}
}

// writeObservedLink counts what is written to the link through an observer.
type writeObservedLink struct {
	io.ReadWriter
	writer *observer.Writer
}

func (c *writeObservedLink) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

// readObservedLink counts what is read from the link through an observer.
type readObservedLink struct {
	io.ReadWriter
	reader *observer.Reader
}

func (c *readObservedLink) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
