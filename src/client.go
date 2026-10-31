package src

import (
	"bytes"
	"context"
	"fmt"
	"github.com/go-faster/errors"
	"github.com/gofrs/uuid/v5"
	codec2 "github.com/imakiri/stream/src/codec"
	"github.com/imakiri/stream/src/frame"
	"golang.org/x/net/proxy"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

// packetBufferSize fits one packet from OBS or the server (secure.Writer packets are 1084 bytes).
const packetBufferSize = 1500

// ServerNetwork is the transport between the client and the relay server.
// The local side towards OBS is always UDP.
type ServerNetwork string

const (
	ServerNetworkUDP ServerNetwork = "udp"
	ServerNetworkTCP ServerNetwork = "tcp"
)

var UID = uuid.Must(uuid.FromString("1751b2a1-0ffd-44fe-8a2e-d3f153125c43")).Bytes()

type Stat struct {
	ID       uint64
	Sent     int64
	Received int64
}

type Client struct {
	stats *os.File

	serverNetwork ServerNetwork
	proxyAddr     *url.URL // TCP only

	portCaller   uint16
	portListener uint16

	serverPort uint16
	serverAddr string

	acceptedLocalAddrMu *sync.Cond
	acceptedLocalAddr   *net.UDPAddr

	decoder *codec2.Decoder
	encoder *codec2.Encoder

	writer io.WriteCloser
	reader io.ReadCloser

	serverConn   net.Conn // *frame.Conn after the handshake over TCP
	callerConn   *net.UDPConn
	listenerConn *net.UDPConn
}

func (client *Client) connect(ctx context.Context, peerID uuid.UUID) error {
	var ok bool

	{
		var localConfig = net.ListenConfig{
			Control:   nil,
			KeepAlive: 0,
		}

		var listener, err = localConfig.ListenPacket(ctx, "udp4", fmt.Sprintf("127.0.0.1:%d", client.portListener))
		if err != nil {
			return errors.Wrap(err, "connect: localConfig.ListenPacket")
		}

		client.listenerConn, ok = listener.(*net.UDPConn)
		if !ok {
			return errors.New("connect: listener.(*net.UDPConn) is not ok")
		}

		log.Println("local listener at", client.listenerConn.LocalAddr().String())
		client.listenerConn.SetWriteBuffer(20 * (codec2.PacketV2Size + 12))
	}
	{
		var dialer = net.Dialer{
			Timeout:        0,
			Deadline:       time.Time{},
			LocalAddr:      nil,
			FallbackDelay:  0,
			KeepAlive:      0,
			Resolver:       nil,
			Control:        nil,
			ControlContext: nil,
		}

		var caller, err = dialer.DialContext(ctx, "udp4", fmt.Sprintf("127.0.0.1:%d", client.portCaller))
		if err != nil {
			return errors.Wrap(err, "connect: dialer.DialContext")
		}

		var ok bool
		client.callerConn, ok = caller.(*net.UDPConn)
		if !ok {
			return errors.New("connect: caller.(*net.UDPConn) is not ok")
		}

		log.Println("local caller to", client.callerConn.RemoteAddr().String())
		client.callerConn.SetWriteBuffer(20 * (codec2.PacketV2Size + 12))
	}

	var err error
	client.serverConn, err = client.dialServer(ctx)
	if err != nil {
		return err
	}
	var serverConn, callerConn, listenerConn = client.serverConn, client.callerConn, client.listenerConn
	go func() {
		<-ctx.Done()
		serverConn.Close()
		callerConn.Close()
		listenerConn.Close()
	}()

	err = client.handshake(peerID)
	if err != nil {
		return err
	}

	if client.serverNetwork == ServerNetworkTCP {
		// TCP is a byte stream: frame the packets so the reader gets them whole.
		client.serverConn = frame.NewConn(client.serverConn)
	}

	log.Printf("server at %s/%s", client.serverNetwork, client.serverConn.RemoteAddr())
	log.Printf("handshake done")
	return nil
}

// dialServer connects to the relay server over the configured network,
// through the proxy for TCP if one is set.
func (client *Client) dialServer(ctx context.Context) (net.Conn, error) {
	var addr = net.JoinHostPort(client.serverAddr, strconv.Itoa(int(client.serverPort)))
	var dialer = &net.Dialer{}

	switch client.serverNetwork {
	case ServerNetworkUDP:
		conn, err := dialer.DialContext(ctx, "udp4", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial server at udp/%s: %w", addr, err)
		}
		_ = conn.(*net.UDPConn).SetReadBuffer(20 * (codec2.PacketV2Size + 12))
		return conn, nil
	case ServerNetworkTCP:
		var contextDialer proxy.ContextDialer = dialer
		if client.proxyAddr != nil {
			proxyDialer, err := proxy.FromURL(client.proxyAddr, dialer)
			if err != nil {
				return nil, fmt.Errorf("failed to create proxy dialer for %s: %w", client.proxyAddr.Redacted(), err)
			}
			var ok bool
			contextDialer, ok = proxyDialer.(proxy.ContextDialer)
			if !ok {
				return nil, fmt.Errorf("proxy dialer for %s does not support context", client.proxyAddr.Redacted())
			}
		}
		conn, err := contextDialer.DialContext(ctx, "tcp4", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial server at tcp/%s: %w", addr, err)
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetReadBuffer(20 * (codec2.PacketV2Size + 12))
		}
		return conn, nil
	default:
		return nil, fmt.Errorf("unknown server network %q", client.serverNetwork)
	}
}

// handshake sends the hello (UID + peer ID) to the server, unframed on both
// networks, and waits for the server to echo it back.
func (client *Client) handshake(peerID uuid.UUID) error {
	var hello = append(slices.Clone(UID), peerID.Bytes()...)
	if _, err := client.serverConn.Write(hello); err != nil {
		return fmt.Errorf("failed to send handshake to server: %w", err)
	}

	var answer = make([]byte, packetBufferSize)
	var n int
	var err error
	if client.serverNetwork == ServerNetworkTCP {
		n, err = io.ReadFull(client.serverConn, answer[:len(hello)])
	} else {
		n, err = client.serverConn.Read(answer)
	}
	if err != nil {
		return fmt.Errorf("failed to read handshake from server at %s: %w", client.serverConn.RemoteAddr(), err)
	}
	if !bytes.Equal(answer[:n], hello) {
		return errors.Errorf("server answered handshake with %d unexpected bytes", n)
	}
	return nil
}

func (client *Client) routeIn(ctx context.Context) {
	var decoderIn, decoderOut, _ = client.decoder.Decode(ctx)
	//routeIn:
	go func() {
		var buf []byte
		for {
			select {
			case <-ctx.Done():
				return
			default:
				buf = make([]byte, packetBufferSize)
				n, err := client.reader.Read(buf)
				if err != nil {
					log.Printf("routeIn: reader.Read: %v", err)
					return
				}
				select {
				case <-ctx.Done():
					return
				case decoderIn <- buf[:n]:
				}
			}
		}
	}()
	go func() {
		var buf []byte
		var ok bool
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			case buf, ok = <-decoderOut:
				if !ok {
					return
				}
				_, err = client.callerConn.Write(buf)
				if err != nil {
					log.Printf("routeIn: callerConn.Write: %v", err)
					return
				}

				if client.acceptedLocalAddr != nil {
					_, err = client.listenerConn.WriteToUDP(buf, client.acceptedLocalAddr)
					if err != nil {
						log.Printf("routeIn: callerConn.Write: %v", err)
						return
					}
				}
			}
		}
	}()
}

func (client *Client) routeOut(ctx context.Context) {
	var encodeIn, encoderOut, _ = client.encoder.Encode(ctx)
	//routeOut:
	go func() {
		var buf []byte
		var err error
		var addr *net.UDPAddr
		var n int
		for {
			select {
			case <-ctx.Done():
				return
			default:
				buf = make([]byte, packetBufferSize)
				n, addr, err = client.listenerConn.ReadFromUDP(buf)
				if err != nil {
					log.Printf("routeOut: listenerConn.ReadFromUDP: %v", err)
					return
				}

				client.acceptedLocalAddr = addr

				select {
				case <-ctx.Done():
					return
				case encodeIn <- buf[:n]:
				}
			}
		}
	}()
	go func() {
		var buf []byte
		var ok bool
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			case buf, ok = <-encoderOut:
				if !ok {
					return
				}
				_, err = client.writer.Write(buf)
				if err != nil {
					log.Printf("routeOut: writer.Write: %v", err)
					return
				}
			}
		}
	}()
}

type Handler interface {
	// The server connection carries whole packets: one Read or Write is one packet.
	Reader(server net.Conn) (io.ReadCloser, error)
	Writer(server net.Conn) (io.WriteCloser, error)
}

func (client *Client) Run(ctx context.Context, peerID uuid.UUID, handler Handler) error {
	var err error

	err = client.connect(ctx, peerID)
	if err != nil {
		return errors.Wrap(err, "client.connect")
	}

	client.writer, err = handler.Writer(client.serverConn)
	if err != nil {
		return errors.Wrap(err, "writer")
	}

	client.reader, err = handler.Reader(client.serverConn)
	if err != nil {
		return errors.Wrap(err, "reader")
	}

	var i int
	for err = errors.New(""); err != nil; i++ {
		client.stats, err = os.Create(fmt.Sprintf("stats_%d", i))
	}

	go client.routeIn(ctx)
	go client.routeOut(ctx)
	return nil
}

type Config struct {
	Peer struct {
		ID           string
		Mode         string
		PortCaller   uint16
		PortListener uint16
	}
	Server struct {
		Network ServerNetwork // "udp" (default) or "tcp"
		Proxy   string        // proxy URL, e.g. socks5://127.0.0.1:2080; TCP only
		Port    uint16
		Addr    string
	}
	Fec struct {
		DataParts  uint64
		TotalParts uint64
	}
	Encoder struct {
		DispatcherTimeout uint64
		DispatcherSize    uint64
	}
	Decoder struct {
		AssemblerSize  uint64
		DispatcherSize uint64
	}
}

func NewClient(cfg *Config) (*Client, error) {
	var client = new(Client)
	var err error

	client.serverNetwork = cfg.Server.Network
	switch client.serverNetwork {
	case "":
		client.serverNetwork = ServerNetworkUDP
	case ServerNetworkUDP, ServerNetworkTCP:
	default:
		return nil, fmt.Errorf("unknown server network %q, want %q or %q", cfg.Server.Network, ServerNetworkUDP, ServerNetworkTCP)
	}

	if cfg.Server.Proxy != "" {
		if client.serverNetwork != ServerNetworkTCP {
			return nil, fmt.Errorf("server proxy works only over %q, server network is %q", ServerNetworkTCP, client.serverNetwork)
		}
		client.proxyAddr, err = url.Parse(cfg.Server.Proxy)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy url: %w", err)
		}
	}
	client.encoder, err = codec2.NewEncoder(
		time.Duration(cfg.Encoder.DispatcherTimeout)*time.Millisecond,
		cfg.Encoder.DispatcherSize,
		cfg.Fec.DataParts,
		cfg.Fec.TotalParts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "codec.NewEncoder")
	}
	client.decoder, err = codec2.NewDecoder(
		cfg.Fec.DataParts,
		cfg.Fec.TotalParts,
		cfg.Decoder.AssemblerSize,
		cfg.Decoder.DispatcherSize,
	)
	if err != nil {
		return nil, errors.Wrap(err, "codec.NewEncoder")
	}
	client.serverPort = cfg.Server.Port
	client.serverAddr = cfg.Server.Addr
	client.portCaller = cfg.Peer.PortCaller
	client.portListener = cfg.Peer.PortListener
	client.acceptedLocalAddrMu = sync.NewCond(new(sync.Mutex))
	return client, nil
}
