package src

import (
	"context"
	"errors"
	"fmt"
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
	var output net.Conn = link
	if s.cfg.OutputReporter != nil {
		writer, err := observer.NewWriter(link, s.cfg.OutputReporter, time.Second)
		if err != nil {
			return err
		}
		defer writer.Close()
		output = &writeObservedConn{Conn: link, writer: writer}
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

// sendStream seals every FEC packet into a data packet for the proxy.
func (s *Server) sendStream(link net.Conn, encoderOut <-chan []byte) {
	for packet := range encoderOut {
		if _, err := link.Write(s.sealer.SealData(packet)); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("failed to send stream to proxy: %s", err)
		}
	}
}

// answerHandshakes answers every client hello the proxy hands over.
func (s *Server) answerHandshakes(link net.Conn) {
	var buf = make([]byte, packetBufferSize)
	for {
		n, err := link.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("failed to read from proxy: %s", err)
			}
			return
		}
		if wire.PacketKind(buf[:n]) != wire.KindClientHello {
			continue // proxy hello echoes
		}
		serverHello, clientID, err := s.handshake.AnswerClientHello(buf[:n], time.Now())
		if err != nil {
			log.Printf("rejected client hello: %s", err)
			continue
		}
		if _, err := link.Write(serverHello); err != nil {
			log.Printf("failed to answer client %s: %s", clientID, err)
			continue
		}
		log.Printf("handshake with client %s", clientID)
	}
}

// writeObservedConn counts what is written to Conn through an observer.
type writeObservedConn struct {
	net.Conn
	writer *observer.Writer
}

func (c *writeObservedConn) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

// readObservedConn counts what is read from Conn through an observer.
type readObservedConn struct {
	net.Conn
	reader *observer.Reader
}

func (c *readObservedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
