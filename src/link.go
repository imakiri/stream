package src

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/frame"
	"github.com/imakiri/stream/src/wire"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/proxy"
)

// packetBufferSize fits any wire packet and any datagram from OBS.
const packetBufferSize = 1<<16 - 1

// socketBufferSize is the kernel buffer of the UDP sockets to the proxy and
// to the player. The encoder sends the packets of an interleave group, 40 at
// the default settings, back to back, and the receive buffer must take such
// a burst whole: with 20 packets it dropped a few of every group even on
// loopback, and every chunk lost that way held the decoder for its reorder
// window, about 100 ms. 1 MiB holds about ten groups; Linux caps it at
// net.core.rmem_max. The buffer adds no delay while the reader keeps up.
const socketBufferSize = 1 << 20

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

// KeepaliveInterval is how often a peer repeats its proxy hello, over UDP and
// TCP. It must stay well below the proxy's peer timeout (proxy.PeerTimeout)
// and below typical NAT and socks/tunnel UDP session timeouts (from about
// 30 s).
const KeepaliveInterval = 5 * time.Second

// proxyLinkTiming is how a ProxyLink notices a broken connection and dials
// again; tests shorten it.
type proxyLinkTiming struct {
	keepaliveInterval time.Duration
	// echoTimeout breaks a connection the proxy has not echoed a hello on
	// for this long: the proxy is gone, or the network is.
	echoTimeout    time.Duration
	redialMinDelay time.Duration
	redialMaxDelay time.Duration
}

var defaultProxyLinkTiming = proxyLinkTiming{
	keepaliveInterval: KeepaliveInterval,
	echoTimeout:       3 * KeepaliveInterval,
	redialMinDelay:    time.Second,
	redialMaxDelay:    10 * time.Second,
}

// udpReadErrorPause keeps a UDP reader from spinning on a socket error that
// repeats, e.g. while a firewall rejects the traffic.
const udpReadErrorPause = 100 * time.Millisecond

// ProxyLink is a connection to the proxy that outlives network pauses and
// breaks: it registers as role with peerID, repeats the proxy hello every
// KeepaliveInterval and dials again, with backoff, when the proxy stops
// echoing it or a TCP connection fails. Every Read and Write is one wire
// packet.
//
// While the link is down, Write drops the packet and Read waits for the next
// connection: the stream is real time, a late packet is of no use. Hello
// echoes are taken by Read, so the link notices a dead proxy only while
// someone reads, which servers and clients always do.
type ProxyLink struct {
	cfg    ProxyConfig
	socks  *url.URL
	role   wire.Role
	peerID uuid.UUID
	hello  []byte
	timing proxyLinkTiming

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	session *proxySession // nil while the link is down
	up      chan struct{} // closed when a session is published

	// localIP is the local address of the last connection, see dialLocalIP;
	// only keepConnected uses it.
	localIP net.IP
	// sendFailing is set when a send failed and was logged; the next
	// failures are not logged until a send succeeds.
	sendFailing atomic.Bool
}

// proxySession is one connection of a ProxyLink.
type proxySession struct {
	conn             net.Conn
	lastEchoUnixNano atomic.Int64
	broken           chan struct{}
	breakOnce        sync.Once
	breakReason      error
}

func (s *proxySession) fail(reason error) {
	s.breakOnce.Do(func() {
		s.breakReason = reason
		close(s.broken)
	})
}

// DialProxy starts a ProxyLink and returns at once: the link connects in the
// background and keeps connecting until ctx is done or it is closed. It
// fails only on a bad config.
func DialProxy(ctx context.Context, cfg ProxyConfig, role wire.Role, peerID uuid.UUID) (*ProxyLink, error) {
	return dialProxyLink(ctx, cfg, role, peerID, defaultProxyLinkTiming)
}

func dialProxyLink(ctx context.Context, cfg ProxyConfig, role wire.Role, peerID uuid.UUID, timing proxyLinkTiming) (*ProxyLink, error) {
	cfg, socks, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	var l = &ProxyLink{
		cfg:    cfg,
		socks:  socks,
		role:   role,
		peerID: peerID,
		hello:  wire.MarshalProxyHello(role, peerID),
		timing: timing,
		up:     make(chan struct{}),
	}
	l.ctx, l.cancel = context.WithCancel(ctx)
	go l.keepConnected()
	return l, nil
}

func (l *ProxyLink) addr() string {
	return fmt.Sprintf("%s/%s", l.cfg.Network, net.JoinHostPort(l.cfg.Addr, strconv.Itoa(int(l.cfg.Port))))
}

// keepConnected dials, watches the connection and dials again until the link
// is closed.
func (l *ProxyLink) keepConnected() {
	var delay = l.timing.redialMinDelay
	for l.ctx.Err() == nil {
		conn, err := l.dial()
		if err != nil {
			if l.ctx.Err() != nil {
				return
			}
			log.Printf("failed to connect to proxy %s: %s; next attempt in %s", l.addr(), err, delay)
			select {
			case <-l.ctx.Done():
				return
			case <-time.After(delay):
			}
			delay = min(2*delay, l.timing.redialMaxDelay)
			continue
		}
		delay = l.timing.redialMinDelay
		log.Printf("registered at proxy %s as %s %s", l.addr(), l.role, l.peerID)

		var session = &proxySession{conn: conn, broken: make(chan struct{})}
		session.lastEchoUnixNano.Store(time.Now().UnixNano())
		l.publish(session)
		var reason = l.watch(session)
		l.unpublish()
		_ = conn.Close()
		if l.ctx.Err() == nil {
			log.Printf("lost proxy %s: %s; reconnecting", l.addr(), reason)
		}
	}
}

func (l *ProxyLink) dial() (net.Conn, error) {
	conn, err := dialProxyNetwork(l.ctx, l.cfg, l.socks, l.dialLocalIP())
	if err != nil {
		return nil, err
	}
	var attempts = 1 // a retry after a timeout in the middle of a TCP frame would break the framing
	if l.cfg.Network == ProxyNetworkUDP {
		attempts = proxyHelloUDPAttempts
	}
	// exchangeProxyHello may wait for seconds: let Close interrupt it
	var stop = context.AfterFunc(l.ctx, func() { _ = conn.Close() })
	defer stop()
	if err := exchangeProxyHello(conn, l.hello, attempts); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to register: %w", err)
	}
	if addr, ok := conn.LocalAddr().(interface{ AddrPort() netip.AddrPort }); ok && l.socks == nil {
		l.localIP = net.IP(addr.AddrPort().Addr().AsSlice())
	}
	return conn, nil
}

// dialLocalIP returns the local address of the last connection if this
// machine still has it. Dialing from it keeps the stream off a VPN that comes
// up for a while (such as a WARP tunnel): the stream pauses until the VPN is
// down rather than moving into it. On another network the address is gone and
// the link dials from whatever the route gives.
func (l *ProxyLink) dialLocalIP() net.IP {
	if l.localIP == nil {
		return nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && ipNet.IP.Equal(l.localIP) {
			return l.localIP
		}
	}
	return nil
}

// watch sends the keepalive hellos of session and returns why it broke.
func (l *ProxyLink) watch(session *proxySession) error {
	var ticker = time.NewTicker(l.timing.keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return l.ctx.Err()
		case <-session.broken:
			return session.breakReason
		case now := <-ticker.C:
			var silent = now.Sub(time.Unix(0, session.lastEchoUnixNano.Load()))
			if silent > l.timing.echoTimeout {
				return fmt.Errorf("no answer for %s", silent.Round(100*time.Millisecond))
			}
			// A TCP write blocks when the send buffer is full; the echo
			// check above must not wait for it.
			go l.send(session, l.hello)
		}
	}
}

func (l *ProxyLink) publish(session *proxySession) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.session = session
	close(l.up)
}

func (l *ProxyLink) unpublish() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.session = nil
	l.up = make(chan struct{})
}

func (l *ProxyLink) currentSession() (*proxySession, <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.session, l.up
}

// send writes packet to session. A failed TCP write breaks the session; a
// failed UDP write does not, the socket works again once the network does.
func (l *ProxyLink) send(session *proxySession, packet []byte) {
	if _, err := session.conn.Write(packet); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return // keepConnected closed the session
		}
		if l.cfg.Network == ProxyNetworkTCP {
			session.fail(err)
		}
		if l.sendFailing.CompareAndSwap(false, true) {
			log.Printf("failed to send to proxy %s: %s; further failures are not logged until a send succeeds", l.addr(), err)
		}
		return
	}
	if l.sendFailing.CompareAndSwap(true, false) {
		log.Printf("sending to proxy %s again", l.addr())
	}
}

// Write sends p as one packet, or drops it while the link is down. It fails
// only when the link is closed.
func (l *ProxyLink) Write(p []byte) (int, error) {
	if l.ctx.Err() != nil {
		return 0, net.ErrClosed
	}
	if session, _ := l.currentSession(); session != nil {
		l.send(session, p)
	}
	return len(p), nil
}

// Read reads the next packet that is not a hello echo, waiting through
// reconnections. It fails with net.ErrClosed when the link is closed, and with
// io.ErrShortBuffer, the packet dropped, when a TCP frame does not fit into p.
func (l *ProxyLink) Read(p []byte) (int, error) {
	for {
		session, up := l.currentSession()
		if session == nil {
			select {
			case <-l.ctx.Done():
				return 0, net.ErrClosed
			case <-up:
				continue
			}
		}
		n, err := session.conn.Read(p)
		switch {
		case l.ctx.Err() != nil:
			return 0, net.ErrClosed
		case errors.Is(err, io.ErrShortBuffer):
			return 0, err
		case errors.Is(err, net.ErrClosed):
			// keepConnected closed the session: wait for the next one
		case err != nil && l.cfg.Network == ProxyNetworkTCP:
			session.fail(err)
		case err != nil:
			// A UDP socket reports ICMP errors and firewall rejects on
			// read; the socket itself is fine.
			time.Sleep(udpReadErrorPause)
		case wire.PacketKind(p[:n]) == wire.KindProxyHello:
			session.lastEchoUnixNano.Store(time.Now().UnixNano())
		default:
			return n, nil
		}
	}
}

// Close closes the link; blocked Reads return net.ErrClosed.
func (l *ProxyLink) Close() error {
	l.cancel()
	if session, _ := l.currentSession(); session != nil {
		_ = session.conn.Close()
	}
	return nil
}

// dialProxyNetwork connects to the proxy, from localIP if it is set; it is
// not used with a socks proxy, which is reached over loopback or the LAN.
func dialProxyNetwork(ctx context.Context, cfg ProxyConfig, socks *url.URL, localIP net.IP) (net.Conn, error) {
	var addr = net.JoinHostPort(cfg.Addr, strconv.Itoa(int(cfg.Port)))
	var dialer = &net.Dialer{}

	switch cfg.Network {
	case ProxyNetworkUDP:
		if localIP != nil {
			dialer.LocalAddr = &net.UDPAddr{IP: localIP}
		}
		conn, err := dialer.DialContext(ctx, "udp4", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial proxy at udp/%s: %w", addr, err)
		}
		_ = conn.(*net.UDPConn).SetReadBuffer(socketBufferSize)
		markRealTime(conn)
		return conn, nil
	default: // ProxyNetworkTCP
		var contextDialer proxy.ContextDialer = dialer
		if localIP != nil && socks == nil {
			dialer.LocalAddr = &net.TCPAddr{IP: localIP}
		}
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

// exchangeProxyHello sends the hello and waits for the proxy to echo it. Other
// packets before the echo are dropped: once the proxy has registered the peer
// it may route stream data to it ahead of the echo.
func exchangeProxyHello(conn net.Conn, hello []byte, attempts int) error {
	var buf = make([]byte, packetBufferSize)
	defer conn.SetReadDeadline(time.Time{})
	var err error
	for range attempts {
		if _, err = conn.Write(hello); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(proxyHelloTimeout))
		for {
			var n int
			n, err = conn.Read(buf)
			if err != nil || bytes.Equal(buf[:n], hello) {
				break
			}
		}
		if err == nil {
			return nil
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
