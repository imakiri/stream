package src

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/wire"
)

// fakeSocks5UDPProxy is a SOCKS5 proxy on loopback that does RFC 1929
// username/password and UDP ASSOCIATE, as sing-box does: it relays the
// datagrams of the association to their destination and the answers back
// with the source in the header. It returns the proxy's address.
func fakeSocks5UDPProxy(t *testing.T, username, password string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			control, err := listener.Accept()
			if err != nil {
				return
			}
			go serveSocks5UDP(control, username, password)
		}
	}()
	return listener.Addr().String()
}

func serveSocks5UDP(control net.Conn, username, password string) {
	defer control.Close()
	var b = make([]byte, 512)
	if _, err := io.ReadFull(control, b[:2]); err != nil || b[0] != 5 {
		return
	}
	if _, err := io.ReadFull(control, b[:b[1]]); err != nil || !bytes.Contains(b[:b[1]], []byte{2}) {
		_, _ = control.Write([]byte{5, 0xff})
		return
	}
	_, _ = control.Write([]byte{5, 2})
	if _, err := io.ReadFull(control, b[:2]); err != nil {
		return
	}
	var user = make([]byte, b[1])
	_, _ = io.ReadFull(control, user)
	_, _ = io.ReadFull(control, b[:1])
	var pass = make([]byte, b[0])
	_, _ = io.ReadFull(control, pass)
	if string(user) != username || string(pass) != password {
		_, _ = control.Write([]byte{1, 1})
		return
	}
	_, _ = control.Write([]byte{1, 0})
	// request: VER CMD RSV, IPv4 client address and port
	if _, err := io.ReadFull(control, b[:10]); err != nil || b[1] != 3 || b[3] != 1 {
		_, _ = control.Write([]byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return
	}
	defer relay.Close()
	// BND.ADDR 0.0.0.0: the client takes the proxy's own address
	var reply = []byte{5, 0, 0, 1, 0, 0, 0, 0}
	reply = binary.BigEndian.AppendUint16(reply, uint16(relay.LocalAddr().(*net.UDPAddr).Port))
	_, _ = control.Write(reply)
	go func() {
		_, _ = io.Copy(io.Discard, control)
		_ = relay.Close()
	}()
	var client netip.AddrPort
	var buf = make([]byte, 65536)
	for {
		n, from, err := relay.ReadFromUDPAddrPort(buf)
		if err != nil {
			return
		}
		if from.Port() != binary.BigEndian.Uint16(b[8:10]) {
			// an answer from the destination: back to the client with the header
			var header = []byte{0, 0, 0, 1}
			header = append(header, from.Addr().Unmap().AsSlice()...)
			header = binary.BigEndian.AppendUint16(header, from.Port())
			_, _ = relay.WriteToUDPAddrPort(append(header, buf[:n]...), client)
			continue
		}
		client = from
		if n < 10 || buf[3] != 1 {
			continue
		}
		var dest = netip.AddrPortFrom(netip.AddrFrom4([4]byte(buf[4:8])), binary.BigEndian.Uint16(buf[8:10]))
		_, _ = relay.WriteToUDPAddrPort(buf[10:n], dest)
	}
}

// TestRelayLinkThroughSocks5UDP registers a RelayLink over UDP at a fake
// relay through a fake SOCKS5 proxy with UDP ASSOCIATE and a password: the
// relay echoes the relay hello and sends a data packet, which the link must
// read.
func TestRelayLinkThroughSocks5UDP(t *testing.T) {
	var data = []byte{wire.Version, byte(wire.KindData), 1, 2, 3}
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	go func() {
		var buf = make([]byte, packetBufferSize)
		for {
			n, from, err := relay.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			if wire.PacketKind(buf[:n]) == wire.KindRelayHello {
				_, _ = relay.WriteToUDPAddrPort(buf[:n], from)
				_, _ = relay.WriteToUDPAddrPort(data, from)
			}
		}
	}()
	var proxyURL = "socks5://user:pass@" + fakeSocks5UDPProxy(t, "user", "pass")
	// a real proxy instead, e.g. a local sing-box with a socks inbound
	if real := os.Getenv("STREAM_TEST_SOCKS5_URL"); real != "" {
		proxyURL = real
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	link, err := DialRelay(ctx, RelayConfig{
		Network: RelayNetworkUDP,
		Addr:    "127.0.0.1",
		Port:    uint16(relay.LocalAddr().(*net.UDPAddr).Port),
		Proxy:   ProxyConfig{Socks5: &Socks5ProxyConfig{URL: proxyURL}},
	}, wire.RoleClient, uuid.Must(uuid.NewV4()))
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, packetBufferSize)
		if n, err := link.Read(buf); err == nil {
			got <- bytes.Clone(buf[:n])
		}
	}()
	select {
	case packet := <-got:
		if !bytes.Equal(packet, data) {
			t.Fatalf("read %x, want %x", packet, data)
		}
	case <-ctx.Done():
		t.Fatal("no data from the relay through the socks5 udp association")
	}
}

// A wrong password is refused at the handshake, with the reason.
func TestSocks5UDPWrongPassword(t *testing.T) {
	var proxyAddr = fakeSocks5UDPProxy(t, "user", "pass")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialSocks5UDP(ctx, mustParseURL(t, "socks5://user:wrong@"+proxyAddr), "127.0.0.1:9")
	if err == nil {
		_ = conn.Close()
		t.Fatal("no error for a wrong password")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("refused the username and password")) {
		t.Fatalf("error %v, want the password refused", err)
	}
}

func TestSocks5UDPPayloadAt(t *testing.T) {
	for _, c := range []struct {
		name     string
		datagram []byte
		at       int
		ok       bool
	}{
		{"ipv4", []byte{0, 0, 0, 1, 1, 2, 3, 4, 0, 80, 'x'}, 10, true},
		{"domain", []byte{0, 0, 0, 3, 2, 'a', 'b', 0, 80, 'x'}, 9, true},
		{"fragment", []byte{0, 0, 1, 1, 1, 2, 3, 4, 0, 80, 'x'}, 0, false},
		{"short", []byte{0, 0, 0, 1, 1, 2}, 0, false},
	} {
		at, ok := socks5UDPPayloadAt(c.datagram)
		if at != c.at || ok != c.ok {
			t.Errorf("%s: %d %v, want %d %v", c.name, at, ok, c.at, c.ok)
		}
	}
}

func mustParseURL(t *testing.T, text string) *url.URL {
	t.Helper()
	u, err := url.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
