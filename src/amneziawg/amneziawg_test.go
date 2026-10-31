package amneziawg

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/curve25519"
)

// testObfuscation are AmneziaWG obfuscation settings; both sides must have
// the same ones.
const testObfuscation = `Jc = 3
Jmin = 10
Jmax = 50
S1 = 15
S2 = 18
H1 = 1234567
H2 = 2345678
H3 = 3456789
H4 = 4567890
`

type keyPair struct{ private, public string }

func newKeyPair(t *testing.T) keyPair {
	var private = make([]byte, 32)
	if _, err := rand.Read(private); err != nil {
		t.Fatal(err)
	}
	private[0] &= 248
	private[31] = private[31]&127 | 64
	public, err := curve25519.X25519(private, curve25519.Basepoint)
	if err != nil {
		t.Fatal(err)
	}
	return keyPair{base64.StdEncoding.EncodeToString(private), base64.StdEncoding.EncodeToString(public)}
}

func freeUDPPort(t *testing.T) int {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}

func openTunnel(t *testing.T, config string) *Tunnel {
	cfg, err := ParseConfig(strings.NewReader(config))
	if err != nil {
		t.Fatal(err)
	}
	tunnel, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tunnel.Close() })
	return tunnel
}

// openTunnelPair brings up two tunnel ends over loopback, the way the
// Amnezia app's server and client configs look: the server at 10.77.0.1 with
// serverObfuscation, the client at 10.77.0.2 with clientObfuscation.
func openTunnelPair(t *testing.T, serverObfuscation, clientObfuscation string) (server, client *Tunnel) {
	var serverKeys, clientKeys = newKeyPair(t), newKeyPair(t)
	var port = freeUDPPort(t)
	server = openTunnel(t, fmt.Sprintf(`[Interface]
PrivateKey = %s
Address = 10.77.0.1/32
ListenPort = %d
%s
[Peer]
PublicKey = %s
AllowedIPs = 10.77.0.2/32
`, serverKeys.private, port, serverObfuscation, clientKeys.public))
	client = openTunnel(t, fmt.Sprintf(`[Interface]
PrivateKey = %s
Address = 10.77.0.2/32
DNS = 10.77.0.1
MTU = 1280
PostUp = iptables -A FORWARD -i %%i -j ACCEPT
%s
[Peer]
PublicKey = %s
AllowedIPs = 0.0.0.0/0, ::/0
Endpoint = localhost:%d
PersistentKeepalive = 25
`, clientKeys.private, clientObfuscation, serverKeys.public, port))
	return server, client
}

// startUDPEcho echoes datagrams at addr inside the tunnel.
func startUDPEcho(t *testing.T, tunnel *Tunnel, addr string) {
	udpEcho, err := tunnel.ListenUDP(netip.MustParseAddrPort(addr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = udpEcho.Close() })
	go func() {
		var buf = make([]byte, 2048)
		for {
			n, from, err := udpEcho.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = udpEcho.WriteTo(buf[:n], from)
		}
	}()
}

// udpEchoes sends stream-sized datagrams to addr until want echoes came
// back or attempts ran out; the first ones may go before the handshake is
// done.
func udpEchoes(t *testing.T, conn net.Conn, want, attempts int) int {
	var packet = bytes.Repeat([]byte{0xA5}, 1100)
	var buf = make([]byte, 2048)
	var echoes int
	for attempt := 0; attempt < attempts && echoes < want; attempt++ {
		if _, err := conn.Write(packet); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := conn.Read(buf)
		if err == nil && bytes.Equal(buf[:n], packet) {
			echoes++
		}
	}
	return echoes
}

// TestTunnelCarriesUDPAndTCP sends UDP datagrams and a TCP stream from one
// tunnel end to an echo at the other.
func TestTunnelCarriesUDPAndTCP(t *testing.T) {
	var server, client = openTunnelPair(t, testObfuscation, testObfuscation)
	startUDPEcho(t, server, "10.77.0.1:9000")
	tcpEcho, err := server.ListenTCP(netip.MustParseAddrPort("10.77.0.1:9001"))
	if err != nil {
		t.Fatal(err)
	}
	defer tcpEcho.Close()
	go func() {
		for {
			conn, err := tcpEcho.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	udpConn, err := client.DialContext(ctx, "udp4", "10.77.0.1:9000")
	if err != nil {
		t.Fatal(err)
	}
	defer udpConn.Close()
	if echoes := udpEchoes(t, udpConn, 10, 50); echoes < 10 {
		t.Fatalf("got %d udp echoes through the tunnel, want 10", echoes)
	}

	tcpConn, err := client.DialContext(ctx, "tcp4", "10.77.0.1:9001")
	if err != nil {
		t.Fatal(err)
	}
	defer tcpConn.Close()
	var message = bytes.Repeat([]byte("stream "), 10_000)
	go func() { _, _ = tcpConn.Write(message) }()
	_ = tcpConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var got = make([]byte, len(message))
	if _, err := io.ReadFull(tcpConn, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, message) {
		t.Fatal("tcp echo through the tunnel differs")
	}
}

// TestTunnelNeedsTheSameObfuscation checks that the obfuscation settings
// reach the protocol: with other message headers (H1-H4) on one side the
// ends do not understand each other's handshake.
func TestTunnelNeedsTheSameObfuscation(t *testing.T) {
	var otherHeaders = strings.Replace(testObfuscation, "H1 = 1234567", "H1 = 7654321", 1)
	var server, client = openTunnelPair(t, testObfuscation, otherHeaders)
	startUDPEcho(t, server, "10.77.0.1:9000")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	udpConn, err := client.DialContext(ctx, "udp4", "10.77.0.1:9000")
	if err != nil {
		t.Fatal(err)
	}
	defer udpConn.Close()
	if echoes := udpEchoes(t, udpConn, 1, 10); echoes != 0 {
		t.Fatalf("got %d udp echoes with different headers on the two ends, want none", echoes)
	}
}

func TestParseConfig(t *testing.T) {
	var keys = newKeyPair(t)
	var peerKeys = newKeyPair(t)
	cfg, err := ParseConfig(strings.NewReader(fmt.Sprintf(`# exported by the Amnezia app
[Interface]
Address = 10.8.1.2/32, fd00::2/128
DNS = 1.1.1.1, 1.0.0.1, example.lan
PrivateKey = %s
Jc = 4
H1 = 100-200
Table = off

[Peer]
PublicKey = %s
PresharedKey = %s
AllowedIPs = 0.0.0.0/0, ::/0
Endpoint = 203.0.113.7:51820
PersistentKeepalive = 25
`, keys.private, peerKeys.public, peerKeys.public)))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(cfg.Addresses) != "[10.8.1.2 fd00::2]" || fmt.Sprint(cfg.DNSServers) != "[1.1.1.1 1.0.0.1]" || cfg.MTU != defaultMTU {
		t.Errorf("addresses %v, dns %v, mtu %d", cfg.Addresses, cfg.DNSServers, cfg.MTU)
	}
	var privateHex = hexOf(t, keys.private)
	var peerHex = hexOf(t, peerKeys.public)
	if cfg.uapiDevice != "private_key="+privateHex+"\njc=4\nh1=100-200\n" {
		t.Errorf("device uapi:\n%s", cfg.uapiDevice)
	}
	var wantPeer = "public_key=" + peerHex + "\npreshared_key=" + peerHex +
		"\nallowed_ip=0.0.0.0/0\nallowed_ip=::/0\npersistent_keepalive_interval=25\n"
	if len(cfg.peers) != 1 || cfg.peers[0].uapi != wantPeer || cfg.peers[0].endpoint != "203.0.113.7:51820" {
		t.Errorf("peers %+v, want uapi:\n%s", cfg.peers, wantPeer)
	}

	for _, broken := range []string{
		"[Interface]\nAddress = 10.0.0.2/32\n[Peer]\nPublicKey = " + peerKeys.public + "\n",
		"[Interface]\nPrivateKey = " + keys.private + "\n[Peer]\nPublicKey = " + peerKeys.public + "\n",
		"[Interface]\nPrivateKey = " + keys.private + "\nAddress = 10.0.0.2/32\n",
		"[Interface]\nPrivateKey = not-base64\nAddress = 10.0.0.2/32\n",
		"[Wireguard]\n",
	} {
		if _, err := ParseConfig(strings.NewReader(broken)); err == nil {
			t.Errorf("no error for:\n%s", broken)
		}
	}
}

func hexOf(t *testing.T, base64Key string) string {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(key)
}
