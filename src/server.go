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
// stream from OBS and sends it to every client through the proxy.
type ServerConfig struct {
	Proxy           ProxyConfig
	StreamInputPort uint16 // local UDP port OBS sends the mpegts stream to
	PacingRate      uint64 // bits per second the data packets are spread at; 0: sent as the encoder hands them over
	Fec             FecConfig
	Encoder         EncoderConfig
	Credentials     wire.Credentials
	Verifier        wire.CertificateVerifier
	OutputReporter  observer.Reporter // optional: speed of what goes to the proxy
}

// Server takes the stream from OBS, adds FEC, seals it with the stream key
// and sends one copy to the proxy, which hands it to every client. Clients
// get the stream key from the handshake.
type Server struct {
	cfg       ServerConfig
	encoder   *codec.Encoder
	handshake wire.ServerHandshaker
	sealer    wire.StreamSealer
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
	return &Server{
		cfg:     cfg,
		encoder: encoder,
		handshake: &wire.CertificateServerHandshake{
			Credentials: cfg.Credentials,
			Verifier:    cfg.Verifier,
			StreamKey:   streamKey,
		},
		sealer: sealer,
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

	link, err := DialProxy(ctx, s.cfg.Proxy, wire.RoleServer, s.cfg.Credentials.Certificate.ID)
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

	go s.readStreamInput(ctx, streamInput, encoderIn)
	go s.sendStream(output, encoderOut)
	go s.answerHandshakes(output)

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

// sendStream seals every FEC packet into a data packet for the proxy, paced
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

// answerHandshakes answers every client hello the proxy hands over.
func (s *Server) answerHandshakes(link io.ReadWriter) {
	var buf = make([]byte, packetBufferSize)
	for {
		n, err := link.Read(buf)
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Printf("failed to read from proxy: %s", err)
			continue
		}
		if wire.PacketKind(buf[:n]) != wire.KindClientHello {
			continue
		}
		serverHello, clientID, err := s.handshake.AnswerClientHello(buf[:n], time.Now())
		if err != nil {
			log.Printf("rejected client hello: %s", err)
			continue
		}
		if _, err := link.Write(serverHello); err != nil {
			return
		}
		log.Printf("handshake with client %s", clientID)
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
