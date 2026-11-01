package src

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// SOCKS5 over UDP (RFC 1928 UDP ASSOCIATE, RFC 1929 username/password):
// x/net/proxy dials TCP only. The client asks the proxy over a TCP control
// connection for a UDP relay port, then sends each datagram there with a
// header naming its destination; the proxy sends the answers back with the
// same header naming their source. The association lives as long as the
// control connection.

const (
	socks5Version            = 5
	socks5MethodNoAuth       = 0
	socks5MethodUserPassword = 2
	socks5MethodNone         = 0xff
	socks5CommandUDP         = 3
	socks5AddrIPv4           = 1
	socks5AddrDomain         = 3
	socks5AddrIPv6           = 4
)

// socks5Replies names the reply codes of RFC 1928, 6.
var socks5Replies = map[byte]string{
	1: "general failure",
	2: "not allowed by the ruleset",
	3: "network unreachable",
	4: "host unreachable",
	5: "connection refused",
	6: "TTL expired",
	7: "command not supported (the proxy has no UDP ASSOCIATE)",
	8: "address type not supported",
}

// dialSocks5UDP opens a UDP association at the SOCKS5 proxy of proxyURL and
// returns a connection whose datagrams go to and come from addr through it.
func dialSocks5UDP(ctx context.Context, proxyURL *url.URL, addr string) (net.Conn, error) {
	header, err := socks5UDPHeader(addr)
	if err != nil {
		return nil, err
	}
	var proxyAddr = proxyURL.Host
	if _, _, err := net.SplitHostPort(proxyAddr); err != nil {
		proxyAddr = net.JoinHostPort(proxyAddr, "1080")
	}
	control, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial socks5 proxy %s: %w", proxyURL.Redacted(), err)
	}
	// the datagrams leave from the address the control connection has, as
	// the request says; a proxy may check they come from it
	var localIP = control.LocalAddr().(*net.TCPAddr).IP
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: localIP})
	if err != nil {
		_ = control.Close()
		return nil, fmt.Errorf("failed to open a udp socket for the socks5 proxy: %w", err)
	}
	relay, err := socks5Associate(ctx, control, proxyURL, udp.LocalAddr().(*net.UDPAddr))
	if err != nil {
		_ = control.Close()
		_ = udp.Close()
		return nil, fmt.Errorf("socks5 proxy %s: %w", proxyURL.Redacted(), err)
	}
	_ = udp.SetReadBuffer(socketBufferSize)
	var conn = &socks5UDPConn{
		control:  control,
		udp:      udp,
		relay:    relay,
		header:   header,
		dest:     socks5Addr(addr),
		readBuf:  make([]byte, packetBufferSize+len(header)+262),
		writeBuf: append(make([]byte, 0, packetBufferSize+len(header)), header...),
	}
	go conn.watchControl()
	return conn, nil
}

// socks5Associate does the handshake and the UDP ASSOCIATE request on the
// control connection, from the udp socket at from, and returns where the
// proxy takes the datagrams.
func socks5Associate(ctx context.Context, control net.Conn, proxyURL *url.URL, from *net.UDPAddr) (netip.AddrPort, error) {
	stop := context.AfterFunc(ctx, func() { _ = control.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		_ = control.SetDeadline(deadline)
	} else {
		_ = control.SetDeadline(time.Now().Add(10 * time.Second))
	}
	defer control.SetDeadline(time.Time{})

	var methods = []byte{socks5MethodNoAuth}
	if proxyURL.User != nil {
		methods = []byte{socks5MethodUserPassword}
	}
	if _, err := control.Write(append([]byte{socks5Version, byte(len(methods))}, methods...)); err != nil {
		return netip.AddrPort{}, err
	}
	var reply = make([]byte, 2)
	if _, err := io.ReadFull(control, reply); err != nil {
		return netip.AddrPort{}, fmt.Errorf("no method reply: %w", err)
	}
	if reply[0] != socks5Version {
		return netip.AddrPort{}, fmt.Errorf("not a socks5 proxy: version %d", reply[0])
	}
	switch reply[1] {
	case socks5MethodNoAuth:
	case socks5MethodUserPassword:
		if proxyURL.User == nil {
			return netip.AddrPort{}, errors.New("the proxy wants a username and password, the url has none")
		}
		var username = proxyURL.User.Username()
		var password, _ = proxyURL.User.Password()
		if len(username) > 255 || len(password) > 255 {
			return netip.AddrPort{}, errors.New("username or password longer than 255 bytes")
		}
		var auth = []byte{1, byte(len(username))}
		auth = append(auth, username...)
		auth = append(auth, byte(len(password)))
		auth = append(auth, password...)
		if _, err := control.Write(auth); err != nil {
			return netip.AddrPort{}, err
		}
		if _, err := io.ReadFull(control, reply); err != nil {
			return netip.AddrPort{}, fmt.Errorf("no authentication reply: %w", err)
		}
		if reply[1] != 0 {
			return netip.AddrPort{}, errors.New("the proxy refused the username and password")
		}
	case socks5MethodNone:
		return netip.AddrPort{}, errors.New("the proxy accepts none of the offered authentication methods")
	default:
		return netip.AddrPort{}, fmt.Errorf("the proxy chose an unknown authentication method %d", reply[1])
	}

	var fromAddr = from.AddrPort()
	var request = []byte{socks5Version, socks5CommandUDP, 0}
	request = appendSocks5Addr(request, fromAddr.Addr().Unmap(), "", fromAddr.Port())
	if _, err := control.Write(request); err != nil {
		return netip.AddrPort{}, err
	}
	var head = make([]byte, 4)
	if _, err := io.ReadFull(control, head); err != nil {
		return netip.AddrPort{}, fmt.Errorf("no reply to UDP ASSOCIATE: %w", err)
	}
	if head[1] != 0 {
		var text, ok = socks5Replies[head[1]]
		if !ok {
			text = "code " + strconv.Itoa(int(head[1]))
		}
		return netip.AddrPort{}, fmt.Errorf("UDP ASSOCIATE refused: %s", text)
	}
	var bound netip.Addr
	switch head[3] {
	case socks5AddrIPv4, socks5AddrIPv6:
		var ip = make([]byte, map[byte]int{socks5AddrIPv4: 4, socks5AddrIPv6: 16}[head[3]])
		if _, err := io.ReadFull(control, ip); err != nil {
			return netip.AddrPort{}, err
		}
		bound, _ = netip.AddrFromSlice(ip)
	case socks5AddrDomain:
		var size = make([]byte, 1)
		if _, err := io.ReadFull(control, size); err != nil {
			return netip.AddrPort{}, err
		}
		var name = make([]byte, size[0])
		if _, err := io.ReadFull(control, name); err != nil {
			return netip.AddrPort{}, err
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", string(name))
		if err != nil || len(ips) == 0 {
			return netip.AddrPort{}, fmt.Errorf("cannot resolve the udp relay %q the proxy named: %v", name, err)
		}
		bound = ips[0]
	default:
		return netip.AddrPort{}, fmt.Errorf("unknown address type %d in the reply", head[3])
	}
	var port = make([]byte, 2)
	if _, err := io.ReadFull(control, port); err != nil {
		return netip.AddrPort{}, err
	}
	bound = bound.Unmap()
	// 0.0.0.0: the proxy means its own address
	if bound.IsUnspecified() {
		bound = control.RemoteAddr().(*net.TCPAddr).AddrPort().Addr().Unmap()
	}
	return netip.AddrPortFrom(bound, binary.BigEndian.Uint16(port)), nil
}

// socks5UDPHeader is the header of each datagram to addr: RSV, FRAG 0,
// then the address, an IP as it is, a name for the proxy to resolve.
func socks5UDPHeader(addr string) ([]byte, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("bad port in %q", addr)
	}
	var ip, _ = netip.ParseAddr(host)
	if !ip.IsValid() && len(host) > 255 {
		return nil, fmt.Errorf("host name %q longer than 255 bytes", host)
	}
	return appendSocks5Addr([]byte{0, 0, 0}, ip.Unmap(), host, uint16(port)), nil
}

// appendSocks5Addr appends ATYP, the address and the port: ip if it is
// valid, else the host name.
func appendSocks5Addr(b []byte, ip netip.Addr, host string, port uint16) []byte {
	switch {
	case ip.Is4():
		b = append(b, socks5AddrIPv4)
		b = append(b, ip.AsSlice()...)
	case ip.Is6():
		b = append(b, socks5AddrIPv6)
		b = append(b, ip.AsSlice()...)
	default:
		b = append(b, socks5AddrDomain, byte(len(host)))
		b = append(b, host...)
	}
	return binary.BigEndian.AppendUint16(b, port)
}

// socks5UDPConn is a UDP association: datagrams to one destination and from
// it, through the proxy's UDP relay.
type socks5UDPConn struct {
	control net.Conn
	udp     *net.UDPConn
	relay   netip.AddrPort // the proxy's UDP relay
	header  []byte
	dest    socks5Addr

	readMu   sync.Mutex
	readBuf  []byte
	writeMu  sync.Mutex
	writeBuf []byte // header, then room for the payload
}

// watchControl ends the association when the proxy closes the control
// connection: the proxy forgets the UDP port then, and reads would only
// time out.
func (c *socks5UDPConn) watchControl() {
	_, _ = io.Copy(io.Discard, c.control)
	_ = c.udp.Close()
}

func (c *socks5UDPConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.writeBuf = append(c.writeBuf[:len(c.header)], b...)
	if _, err := c.udp.WriteToUDPAddrPort(c.writeBuf, c.relay); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Read returns the payload of the next datagram from the proxy's relay; it
// drops datagrams from elsewhere and fragments (FRAG not 0), which a proxy
// is free not to send and this side does not reassemble.
func (c *socks5UDPConn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		n, from, err := c.udp.ReadFromUDPAddrPort(c.readBuf)
		if err != nil {
			return 0, err
		}
		if from.Addr().Unmap() != c.relay.Addr() || from.Port() != c.relay.Port() {
			continue
		}
		var at, ok = socks5UDPPayloadAt(c.readBuf[:n])
		if !ok {
			continue
		}
		return copy(b, c.readBuf[at:n]), nil
	}
}

// socks5UDPPayloadAt returns where the payload starts in a datagram from the
// proxy, false if the datagram is a fragment or too short.
func socks5UDPPayloadAt(datagram []byte) (int, bool) {
	if len(datagram) < 4 || datagram[2] != 0 {
		return 0, false
	}
	var at int
	switch datagram[3] {
	case socks5AddrIPv4:
		at = 4 + 4 + 2
	case socks5AddrIPv6:
		at = 4 + 16 + 2
	case socks5AddrDomain:
		if len(datagram) < 5 {
			return 0, false
		}
		at = 4 + 1 + int(datagram[4]) + 2
	default:
		return 0, false
	}
	if len(datagram) < at {
		return 0, false
	}
	return at, true
}

func (c *socks5UDPConn) Close() error {
	var err = c.control.Close()
	if udpErr := c.udp.Close(); err == nil && !errors.Is(udpErr, net.ErrClosed) {
		err = udpErr
	}
	return err
}

func (c *socks5UDPConn) LocalAddr() net.Addr                { return c.udp.LocalAddr() }
func (c *socks5UDPConn) RemoteAddr() net.Addr               { return c.dest }
func (c *socks5UDPConn) SetDeadline(t time.Time) error      { return c.udp.SetDeadline(t) }
func (c *socks5UDPConn) SetReadDeadline(t time.Time) error  { return c.udp.SetReadDeadline(t) }
func (c *socks5UDPConn) SetWriteDeadline(t time.Time) error { return c.udp.SetWriteDeadline(t) }

// socks5Addr is the destination of a UDP association, as host:port; the
// proxy, not this side, may have resolved the host.
type socks5Addr string

func (socks5Addr) Network() string  { return "udp" }
func (a socks5Addr) String() string { return string(a) }
