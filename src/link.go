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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/frame"
	"github.com/imakiri/stream/src/wire"
	"golang.org/x/net/ipv4"
)

// packetBufferSize fits any wire packet and any datagram from OBS.
const packetBufferSize = 1<<16 - 1

// socketBufferSize is the kernel buffer of the UDP sockets to the relay and
// to the player. The encoder sends the packets of an interleave group, 40 at
// the default settings, back to back, and the receive buffer must take such
// a burst whole: with 20 packets it dropped a few of every group even on
// loopback, and every chunk lost that way held the decoder for its reorder
// window, about 100 ms. 1 MiB holds about ten groups; Linux caps it at
// net.core.rmem_max. The buffer adds no delay while the reader keeps up.
const socketBufferSize = 1 << 20

// RelayNetwork is the transport between a peer and the relay. The local side
// towards OBS or the player is always UDP.
type RelayNetwork string

const (
	RelayNetworkUDP RelayNetwork = "udp"
	RelayNetworkTCP RelayNetwork = "tcp"
)

// RelayConfig says how to reach the relay.
type RelayConfig struct {
	Network RelayNetwork // "udp" (default) or "tcp"
	Addr    string
	Port    uint16
	Proxy   ProxyConfig // the way to the relay through a proxy, if any
}

func (cfg RelayConfig) withDefaults() (RelayConfig, error) {
	switch cfg.Network {
	case "":
		cfg.Network = RelayNetworkUDP
	case RelayNetworkUDP, RelayNetworkTCP:
	default:
		return cfg, fmt.Errorf("unknown relay network %q, want %q or %q", cfg.Network, RelayNetworkUDP, RelayNetworkTCP)
	}
	return cfg, cfg.Proxy.validate(cfg.Network)
}

const (
	relayHelloTimeout     = 2 * time.Second
	relayHelloUDPAttempts = 3 // UDP may lose the hello or its echo
)

// KeepaliveInterval is how often a peer repeats its relay hello, over UDP and
// TCP. It must stay well below the relay's peer timeout (relay.PeerTimeout)
// and below typical NAT and socks/tunnel UDP session timeouts (from about
// 30 s).
const KeepaliveInterval = 5 * time.Second

// relayLinkTiming is how a RelayLink notices a broken connection and dials
// again; tests shorten it.
type relayLinkTiming struct {
	keepaliveInterval time.Duration
	// echoTimeout breaks a connection the relay has not echoed a hello on
	// for this long: the relay is gone, or the network is.
	echoTimeout    time.Duration
	redialMinDelay time.Duration
	redialMaxDelay time.Duration
}

var defaultRelayLinkTiming = relayLinkTiming{
	keepaliveInterval: KeepaliveInterval,
	echoTimeout:       3 * KeepaliveInterval,
	redialMinDelay:    time.Second,
	redialMaxDelay:    10 * time.Second,
}

// udpReadErrorPause keeps a UDP reader from spinning on a socket error that
// repeats, e.g. while a firewall rejects the traffic.
const udpReadErrorPause = 100 * time.Millisecond

// RelayLink is a connection to the relay that outlives network pauses and
// breaks: it registers as role with peerID, repeats the relay hello every
// KeepaliveInterval and dials again, with backoff, when the relay stops
// echoing it or a TCP connection fails. Every Read and Write is one wire
// packet.
//
// While the link is down, Write drops the packet and Read waits for the next
// connection: the stream is real time, a late packet is of no use. Hello
// echoes are taken by Read, so the link notices a dead relay only while
// someone reads, which servers and clients always do.
type RelayLink struct {
	cfg    RelayConfig
	proxy  proxyDialer // nil for a direct connection
	role   wire.Role
	peerID uuid.UUID
	hello  []byte
	timing relayLinkTiming

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	session *relaySession // nil while the link is down
	up      chan struct{} // closed when a session is published

	// localIP is the local address of the last connection, see dialLocalIP;
	// only keepConnected uses it.
	localIP net.IP
	// sendFailing is set when a send failed and was logged; the next
	// failures are not logged until a send succeeds.
	sendFailing atomic.Bool
}

// relaySession is one connection of a RelayLink.
type relaySession struct {
	conn             net.Conn
	lastEchoUnixNano atomic.Int64
	broken           chan struct{}
	breakOnce        sync.Once
	breakReason      error
}

func (s *relaySession) fail(reason error) {
	s.breakOnce.Do(func() {
		s.breakReason = reason
		close(s.broken)
	})
}

// DialRelay starts a RelayLink and returns at once: the link connects in the
// background and keeps connecting until ctx is done or it is closed. It
// fails only on a bad config.
func DialRelay(ctx context.Context, cfg RelayConfig, role wire.Role, peerID uuid.UUID) (*RelayLink, error) {
	return dialRelayLink(ctx, cfg, role, peerID, defaultRelayLinkTiming)
}

func dialRelayLink(ctx context.Context, cfg RelayConfig, role wire.Role, peerID uuid.UUID, timing relayLinkTiming) (*RelayLink, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	proxyDialer, err := cfg.Proxy.open(ctx)
	if err != nil {
		return nil, err
	}
	var l = &RelayLink{
		cfg:    cfg,
		proxy:  proxyDialer,
		role:   role,
		peerID: peerID,
		hello:  wire.MarshalRelayHello(role, peerID),
		timing: timing,
		up:     make(chan struct{}),
	}
	l.ctx, l.cancel = context.WithCancel(ctx)
	go l.keepConnected()
	return l, nil
}

func (l *RelayLink) addr() string {
	var addr = fmt.Sprintf("%s/%s", l.cfg.Network, net.JoinHostPort(l.cfg.Addr, strconv.Itoa(int(l.cfg.Port))))
	if l.proxy != nil {
		addr += " through the " + l.cfg.Proxy.String()
	}
	return addr
}

// keepConnected dials, watches the connection and dials again until the link
// is closed.
func (l *RelayLink) keepConnected() {
	var delay = l.timing.redialMinDelay
	for l.ctx.Err() == nil {
		conn, err := l.dial()
		if err != nil {
			if l.ctx.Err() != nil {
				return
			}
			log.Printf("failed to connect to relay %s: %s; next attempt in %s", l.addr(), err, delay)
			select {
			case <-l.ctx.Done():
				return
			case <-time.After(delay):
			}
			delay = min(2*delay, l.timing.redialMaxDelay)
			continue
		}
		delay = l.timing.redialMinDelay
		log.Printf("registered at relay %s as %s %s", l.addr(), l.role, l.peerID)

		var session = &relaySession{conn: conn, broken: make(chan struct{})}
		session.lastEchoUnixNano.Store(time.Now().UnixNano())
		l.publish(session)
		var reason = l.watch(session)
		l.unpublish()
		_ = conn.Close()
		if l.ctx.Err() == nil {
			log.Printf("lost relay %s: %s; reconnecting", l.addr(), reason)
		}
	}
}

func (l *RelayLink) dial() (net.Conn, error) {
	conn, err := dialRelayNetwork(l.ctx, l.cfg, l.proxy, l.dialLocalIP())
	if err != nil {
		return nil, err
	}
	var attempts = 1 // a retry after a timeout in the middle of a TCP frame would break the framing
	if l.cfg.Network == RelayNetworkUDP {
		attempts = relayHelloUDPAttempts
	}
	// exchangeRelayHello may wait for seconds: let Close interrupt it
	var stop = context.AfterFunc(l.ctx, func() { _ = conn.Close() })
	defer stop()
	if err := exchangeRelayHello(conn, l.hello, attempts); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to register: %w", err)
	}
	if addr, ok := conn.LocalAddr().(interface{ AddrPort() netip.AddrPort }); ok && l.proxy == nil {
		l.localIP = net.IP(addr.AddrPort().Addr().AsSlice())
	}
	return conn, nil
}

// dialLocalIP returns the local address of the last connection if this
// machine still has it. Dialing from it keeps the stream off a VPN that comes
// up for a while (such as a WARP tunnel): the stream pauses until the VPN is
// down rather than moving into it. On another network the address is gone and
// the link dials from whatever the route gives.
func (l *RelayLink) dialLocalIP() net.IP {
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
func (l *RelayLink) watch(session *relaySession) error {
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

func (l *RelayLink) publish(session *relaySession) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.session = session
	close(l.up)
}

func (l *RelayLink) unpublish() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.session = nil
	l.up = make(chan struct{})
}

func (l *RelayLink) currentSession() (*relaySession, <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.session, l.up
}

// send writes packet to session. A failed TCP write breaks the session; a
// failed UDP write does not, the socket works again once the network does.
func (l *RelayLink) send(session *relaySession, packet []byte) {
	if _, err := session.conn.Write(packet); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return // keepConnected closed the session
		}
		if l.cfg.Network == RelayNetworkTCP {
			session.fail(err)
		}
		if l.sendFailing.CompareAndSwap(false, true) {
			log.Printf("failed to send to relay %s: %s; further failures are not logged until a send succeeds", l.addr(), err)
		}
		return
	}
	if l.sendFailing.CompareAndSwap(true, false) {
		log.Printf("sending to relay %s again", l.addr())
	}
}

// Write sends p as one packet, or drops it while the link is down. It fails
// only when the link is closed.
func (l *RelayLink) Write(p []byte) (int, error) {
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
func (l *RelayLink) Read(p []byte) (int, error) {
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
		case err != nil && l.cfg.Network == RelayNetworkTCP:
			session.fail(err)
		case err != nil:
			// A UDP socket reports ICMP errors and firewall rejects on
			// read; the socket itself is fine.
			time.Sleep(udpReadErrorPause)
		case wire.PacketKind(p[:n]) == wire.KindRelayHello:
			session.lastEchoUnixNano.Store(time.Now().UnixNano())
		default:
			return n, nil
		}
	}
}

// Close closes the link; blocked Reads return net.ErrClosed.
func (l *RelayLink) Close() error {
	l.cancel()
	if session, _ := l.currentSession(); session != nil {
		_ = session.conn.Close()
	}
	if l.proxy != nil {
		return l.proxy.Close()
	}
	return nil
}

// dialRelayNetwork connects to the relay, through proxyDialer if it is set
// or else directly, from localIP if that is set.
func dialRelayNetwork(ctx context.Context, cfg RelayConfig, proxyDialer proxyDialer, localIP net.IP) (net.Conn, error) {
	var addr = net.JoinHostPort(cfg.Addr, strconv.Itoa(int(cfg.Port)))
	if proxyDialer != nil {
		// No socket options here: the connection is to a local socks
		// proxy or inside the tunnel's own network stack, so a buffer size or
		// DSCP mark would not reach the packets that cross the internet.
		conn, err := proxyDialer.DialContext(ctx, string(cfg.Network)+"4", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial relay at %s/%s through the %s: %w", cfg.Network, addr, cfg.Proxy, err)
		}
		if cfg.Network == RelayNetworkTCP {
			return frame.NewConn(conn), nil
		}
		return conn, nil
	}

	var dialer = &net.Dialer{}
	switch cfg.Network {
	case RelayNetworkUDP:
		if localIP != nil {
			dialer.LocalAddr = &net.UDPAddr{IP: localIP}
		}
		conn, err := dialer.DialContext(ctx, "udp4", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial relay at udp/%s: %w", addr, err)
		}
		_ = conn.(*net.UDPConn).SetReadBuffer(socketBufferSize)
		markRealTime(conn)
		return conn, nil
	default: // RelayNetworkTCP
		if localIP != nil {
			dialer.LocalAddr = &net.TCPAddr{IP: localIP}
		}
		conn, err := dialer.DialContext(ctx, "tcp4", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial relay at tcp/%s: %w", addr, err)
		}
		_ = conn.(*net.TCPConn).SetNoDelay(true)
		// No SetReadBuffer here, unlike UDP: a fixed SO_RCVBUF turns off
		// the kernel's window autotuning and caps TCP at buffer/RTT, e.g.
		// about 2.4 Mbit/s with socketBufferSize at 92 ms.
		markRealTime(conn)
		// TCP is a byte stream: frame the packets so the reader gets them whole.
		return frame.NewConn(conn), nil
	}
}

// exchangeRelayHello sends the hello and waits for the relay to echo it. Other
// packets before the echo are dropped: once the relay has registered the peer
// it may route stream data to it ahead of the echo.
func exchangeRelayHello(conn net.Conn, hello []byte, attempts int) error {
	var buf = make([]byte, packetBufferSize)
	defer conn.SetReadDeadline(time.Time{})
	var err error
	for range attempts {
		if _, err = conn.Write(hello); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(relayHelloTimeout))
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
