package relay

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/frame"
	"github.com/imakiri/stream/src/liveoutput"
	"github.com/imakiri/stream/src/wire"
)

// freeAddr returns a localhost address whose port is free for both udp and tcp.
func freeAddr(t *testing.T) string {
	for range 20 {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		var addr = l.Addr().String()
		_ = l.Close()
		u, err := net.ListenPacket("udp4", addr)
		if err == nil {
			_ = u.Close()
			return addr
		}
	}
	t.Fatal("no free port")
	return ""
}

func startRelay(t *testing.T) (*Relay, string) {
	t.Helper()
	var addr = freeAddr(t)
	var p = New(addr, liveoutput.New(io.Discard))
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p, addr
}

// connectUDP registers a peer over UDP and returns its connection.
func connectUDP(t *testing.T, addr string, role wire.Role, id uuid.UUID) net.Conn {
	t.Helper()
	conn, err := net.Dial("udp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	var hello = wire.MarshalRelayHello(role, id)
	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	var buf = make([]byte, packetBufferSize)
	n, err := conn.Read(buf)
	if err != nil || !bytes.Equal(buf[:n], hello) {
		t.Fatalf("udp relay hello: %q, %v", buf[:n], err)
	}
	return conn
}

// connectTCP registers a peer over TCP and returns its framed connection.
func connectTCP(t *testing.T, addr string, role wire.Role, id uuid.UUID) net.Conn {
	t.Helper()
	raw, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	var conn = frame.NewConn(raw)
	var hello = wire.MarshalRelayHello(role, id)
	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	var buf = make([]byte, packetBufferSize)
	n, err := conn.Read(buf)
	if err != nil || !bytes.Equal(buf[:n], hello) {
		t.Fatalf("tcp relay hello: %q, %v", buf[:n], err)
	}
	return conn
}

func waitPeers(t *testing.T, p *Relay, count int) {
	t.Helper()
	for i := 0; len(p.appendPeers(nil)) < count; i++ {
		if i > 100 {
			t.Fatalf("want %d peers, have %d", count, len(p.appendPeers(nil)))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func packetOfKind(kind wire.Kind, size int, fill byte) []byte {
	var b = bytes.Repeat([]byte{fill}, size)
	b[0], b[1] = wire.Version, byte(kind)
	return b
}

func serverHelloFor(clientID uuid.UUID) []byte {
	var b = packetOfKind(wire.KindServerHello, wire.ServerHelloSize, 5)
	copy(b[2:18], clientID.Bytes())
	return b
}

func expectPacket(t *testing.T, conn net.Conn, want []byte, what string) {
	t.Helper()
	var buf = make([]byte, packetBufferSize)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil || !bytes.Equal(buf[:n], want) {
		t.Fatalf("%s: got %d bytes, %v", what, n, err)
	}
}

func expectNothing(t *testing.T, conn net.Conn, what string) {
	t.Helper()
	var buf = make([]byte, packetBufferSize)
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if n, err := conn.Read(buf); err == nil {
		t.Fatalf("%s: got %d bytes", what, n)
	}
}

// TestRelayRoutesBetweenServerAndClients puts the server on UDP and two
// clients on UDP and TCP, and checks every routing rule.
func TestRelayRoutesBetweenServerAndClients(t *testing.T) {
	var p, addr = startRelay(t)
	var udpClientID, tcpClientID = uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4())
	var server = connectUDP(t, addr, wire.RoleServer, uuid.Must(uuid.NewV4()))
	var udpClient = connectUDP(t, addr, wire.RoleClient, udpClientID)
	var tcpClient = connectTCP(t, addr, wire.RoleClient, tcpClientID)
	waitPeers(t, p, 3)

	// client hello goes to the server only
	var clientHello = packetOfKind(wire.KindClientHello, wire.ClientHelloSize, 1)
	if _, err := tcpClient.Write(clientHello); err != nil {
		t.Fatal(err)
	}
	expectPacket(t, server, clientHello, "client hello at server")
	expectNothing(t, udpClient, "client hello at another client")

	// server hello goes to the client it names
	var serverHello = serverHelloFor(tcpClientID)
	if _, err := server.Write(serverHello); err != nil {
		t.Fatal(err)
	}
	expectPacket(t, tcpClient, serverHello, "server hello at its client")
	expectNothing(t, udpClient, "server hello at another client")

	// data goes from the server to every client
	for i := range 10 {
		var data = packetOfKind(wire.KindData, 1086, byte(i))
		if _, err := server.Write(data); err != nil {
			t.Fatal(err)
		}
		expectPacket(t, udpClient, data, "data at udp client")
		expectPacket(t, tcpClient, data, "data at tcp client")
	}

	// data from a client goes nowhere
	if _, err := udpClient.Write(packetOfKind(wire.KindData, 1086, 0xee)); err != nil {
		t.Fatal(err)
	}
	expectNothing(t, tcpClient, "client data at another client")
	expectNothing(t, server, "client data at server")

	// a client report goes to the server only
	var report = clientReportFrom(udpClientID)
	if _, err := udpClient.Write(report); err != nil {
		t.Fatal(err)
	}
	expectPacket(t, server, report, "client report at server")
	expectNothing(t, tcpClient, "client report at another client")
}

func clientReportFrom(clientID uuid.UUID) []byte {
	var b = packetOfKind(wire.KindClientReport, wire.ClientReportSize, 7)
	copy(b[2:18], clientID.Bytes())
	return b
}

// TestRelayFollowsUDPPeerToNewPort repeats a client's relay hello from another
// socket, as after a NAT rebinding or a tunnel exit change, and checks that
// packets for the client go to the new address from then on.
func TestRelayFollowsUDPPeerToNewPort(t *testing.T) {
	var p, addr = startRelay(t)
	var clientID = uuid.Must(uuid.NewV4())
	var oldConn = connectUDP(t, addr, wire.RoleClient, clientID)
	var server = connectUDP(t, addr, wire.RoleServer, uuid.Must(uuid.NewV4()))
	var newConn = connectUDP(t, addr, wire.RoleClient, clientID)
	waitPeers(t, p, 2)

	var data = packetOfKind(wire.KindData, 1086, 7)
	if _, err := server.Write(data); err != nil {
		t.Fatal(err)
	}
	expectPacket(t, newConn, data, "data at new address")
	expectNothing(t, oldConn, "data at old address")
}

// TestRelayDropsSilentUDPPeer checks that a UDP peer without keepalives is
// removed after the timeout, while one that keeps saying hello stays.
func TestRelayDropsSilentUDPPeer(t *testing.T) {
	var addr = freeAddr(t)
	var p = New(addr, liveoutput.New(io.Discard))
	p.peerTimeout = 1500 * time.Millisecond
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	connectUDP(t, addr, wire.RoleClient, uuid.Must(uuid.NewV4()))
	var aliveID = uuid.Must(uuid.NewV4())
	var alive = connectUDP(t, addr, wire.RoleClient, aliveID)

	for range 6 {
		time.Sleep(500 * time.Millisecond)
		if _, err := alive.Write(wire.MarshalRelayHello(wire.RoleClient, aliveID)); err != nil {
			t.Fatal(err)
		}
	}
	var peers = p.appendPeers(nil)
	if len(peers) != 1 || peers[0].id != aliveID {
		t.Fatalf("want only the peer with keepalives, got %d peers", len(peers))
	}
}

// TestRelayKeepsOneServer checks that a newly registered server replaces the
// previous one.
func TestRelayKeepsOneServer(t *testing.T) {
	var p, addr = startRelay(t)
	connectUDP(t, addr, wire.RoleServer, uuid.Must(uuid.NewV4()))
	var newServerID = uuid.Must(uuid.NewV4())
	connectTCP(t, addr, wire.RoleServer, newServerID)
	waitPeers(t, p, 1)
	time.Sleep(50 * time.Millisecond)
	var peers = p.appendPeers(nil)
	if len(peers) != 1 || peers[0].id != newServerID {
		t.Fatalf("want only the new server, got %d peers", len(peers))
	}
}

// TestRelayEchoesTCPKeepalive checks that a repeated hello over TCP comes
// back: the peer checks its link by the echoes.
func TestRelayEchoesTCPKeepalive(t *testing.T) {
	_, addr := startRelay(t)
	var id = uuid.Must(uuid.NewV4())
	var conn = connectTCP(t, addr, wire.RoleClient, id)
	var hello = wire.MarshalRelayHello(wire.RoleClient, id)
	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	expectPacket(t, conn, hello, "keepalive echo")
}

// TestRelayDropsSilentTCPPeer checks that a TCP peer without keepalives is
// disconnected after the timeout.
func TestRelayDropsSilentTCPPeer(t *testing.T) {
	var addr = freeAddr(t)
	var p = New(addr, liveoutput.New(io.Discard))
	p.peerTimeout = 500 * time.Millisecond
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var conn = connectTCP(t, addr, wire.RoleClient, uuid.Must(uuid.NewV4()))
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var buf = make([]byte, packetBufferSize)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("silent tcp peer was not disconnected")
	}
	if peers := p.appendPeers(nil); len(peers) != 0 {
		t.Fatalf("%d peers left", len(peers))
	}
}
