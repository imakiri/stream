package src

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/codec"
	"github.com/imakiri/stream/src/frame"
	"github.com/imakiri/stream/src/wire"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/proxy"
)

// packetBufferSize fits any wire packet and any datagram from OBS.
const packetBufferSize = 1<<16 - 1

// socketBufferSize is the kernel buffer for about 20 packets in flight.
const socketBufferSize = 20 * (codec.PacketV2Size + wire.DataOverhead)

// ProxyNetwork is the transport between a peer and the proxy. The local side
// towards OBS or the player is always UDP.
type ProxyNetwork string

const (
	ProxyNetworkUDP ProxyNetwork = "udp"
	ProxyNetworkTCP ProxyNetwork = "tcp"
)

// ProxyConfig says how to reach the proxy.
type ProxyConfig struct {
	Network    ProxyNetwork // "udp" (default) or "tcp"
	SocksProxy string       // socks5 URL, e.g. socks5://127.0.0.1:2080; tcp only
	Addr       string
	Port       uint16
}

func (cfg ProxyConfig) withDefaults() (ProxyConfig, *url.URL, error) {
	switch cfg.Network {
	case "":
		cfg.Network = ProxyNetworkUDP
	case ProxyNetworkUDP, ProxyNetworkTCP:
	default:
		return cfg, nil, fmt.Errorf("unknown proxy network %q, want %q or %q", cfg.Network, ProxyNetworkUDP, ProxyNetworkTCP)
	}
	if cfg.SocksProxy == "" {
		return cfg, nil, nil
	}
	if cfg.Network != ProxyNetworkTCP {
		return cfg, nil, fmt.Errorf("socks proxy works only over %q, proxy network is %q", ProxyNetworkTCP, cfg.Network)
	}
	socks, err := url.Parse(cfg.SocksProxy)
	if err != nil {
		return cfg, nil, fmt.Errorf("failed to parse socks proxy url: %w", err)
	}
	return cfg, socks, nil
}

const (
	proxyHelloTimeout     = 2 * time.Second
	proxyHelloUDPAttempts = 3 // UDP may lose the hello or its echo
)

// DialProxy connects to the proxy and registers there as role with peerID.
// Over UDP it keeps repeating the registration until ctx is done, see
// sendKeepaliveHellos. Every Read and Write on the returned connection is one
// wire packet; the proxy echoes every proxy hello, and readers skip those by
// kind. The connection is closed when ctx is done.
func DialProxy(ctx context.Context, cfg ProxyConfig, role wire.Role, peerID uuid.UUID) (net.Conn, error) {
	cfg, socks, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	conn, err := dialProxyNetwork(ctx, cfg, socks)
	if err != nil {
		return nil, err
	}

	var hello = wire.MarshalProxyHello(role, peerID)
	var attempts = 1 // a retry after a timeout in the middle of a TCP frame would break the framing
	if cfg.Network == ProxyNetworkUDP {
		attempts = proxyHelloUDPAttempts
	}
	if err := exchangeProxyHello(conn, hello, attempts); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to register at proxy %s/%s: %w", cfg.Network, conn.RemoteAddr(), err)
	}
	log.Printf("registered at proxy %s/%s as %s %s", cfg.Network, conn.RemoteAddr(), role, peerID)

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	if cfg.Network == ProxyNetworkUDP {
		// The proxy knows a UDP peer only by the address its hello came from,
		// and NATs and tunnels change that address: repeat the hello.
		go sendKeepaliveHellos(ctx, conn, hello)
	}
	return conn, nil
}

func dialProxyNetwork(ctx context.Context, cfg ProxyConfig, socks *url.URL) (net.Conn, error) {
	var addr = net.JoinHostPort(cfg.Addr, strconv.Itoa(int(cfg.Port)))
	var dialer = &net.Dialer{}

	switch cfg.Network {
	case ProxyNetworkUDP:
		conn, err := dialer.DialContext(ctx, "udp4", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial proxy at udp/%s: %w", addr, err)
		}
		_ = conn.(*net.UDPConn).SetReadBuffer(socketBufferSize)
		markRealTime(conn)
		return conn, nil
	default: // ProxyNetworkTCP
		var contextDialer proxy.ContextDialer = dialer
		if socks != nil {
			socksDialer, err := proxy.FromURL(socks, dialer)
			if err != nil {
				return nil, fmt.Errorf("failed to create socks dialer for %s: %w", socks.Redacted(), err)
			}
			var ok bool
			contextDialer, ok = socksDialer.(proxy.ContextDialer)
			if !ok {
				return nil, fmt.Errorf("socks dialer for %s does not support context", socks.Redacted())
			}
		}
		conn, err := contextDialer.DialContext(ctx, "tcp4", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial proxy at tcp/%s: %w", addr, err)
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			// No SetReadBuffer here, unlike UDP: a fixed SO_RCVBUF turns off
			// the kernel's window autotuning and caps TCP at buffer/RTT, e.g.
			// about 2.4 Mbit/s with socketBufferSize at 92 ms.
			if socks == nil {
				// through a socks proxy the mark would only reach the local hop to it
				markRealTime(conn)
			}
		}
		// TCP is a byte stream: frame the packets so the reader gets them whole.
		return frame.NewConn(conn), nil
	}
}

// exchangeProxyHello sends the hello and waits for the proxy to echo it.
func exchangeProxyHello(conn net.Conn, hello []byte, attempts int) error {
	var buf = make([]byte, packetBufferSize)
	defer conn.SetReadDeadline(time.Time{})
	var err error
	for range attempts {
		if _, err = conn.Write(hello); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(proxyHelloTimeout))
		var n int
		n, err = conn.Read(buf)
		if err == nil && bytes.Equal(buf[:n], hello) {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("unexpected %d-byte answer", n)
		}
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			return err
		}
	}
	return err
}

// dscpExpeditedForwarding is the DSCP class EF (46) for real-time traffic.
// The internet mostly ignores or clears it, but home routers and Wi-Fi (WMM)
// put such packets into the voice/video queue, ahead of bulk downloads.
const dscpExpeditedForwarding = 46

// markRealTime sets DSCP EF on the packets sent over conn. DSCP is the upper
// 6 bits of the IPv4 TOS byte. Windows ignores the socket option unless a
// Policy-based QoS rule sets DSCP for the executable.
func markRealTime(conn net.Conn) {
	if err := ipv4.NewConn(conn).SetTOS(dscpExpeditedForwarding << 2); err != nil {
		log.Printf("failed to set dscp %d on %s/%s: %s", dscpExpeditedForwarding, conn.LocalAddr().Network(), conn.LocalAddr(), err)
	}
}
