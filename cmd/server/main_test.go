package main

import (
	"bytes"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/gosuri/uilive"
	"github.com/imakiri/stream/src"
	"github.com/imakiri/stream/src/frame"
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

func hello(peerID uuid.UUID) []byte {
	return append(slices.Clone(src.UID), peerID.Bytes()...)
}

// TestServerRelaysBetweenUDPAndTCPPeers connects one peer over UDP and one
// over TCP and checks that packets cross in both directions intact.
func TestServerRelaysBetweenUDPAndTCPPeers(t *testing.T) {
	var addr = freeAddr(t)
	var server = NewServer(addr, uilive.New())
	if err := server.Serve(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	var buf = make([]byte, packetBufferSize)

	udpConn, err := net.Dial("udp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer udpConn.Close()
	_ = udpConn.SetDeadline(time.Now().Add(5 * time.Second))
	var udpHello = hello(uuid.Must(uuid.NewV4()))
	if _, err := udpConn.Write(udpHello); err != nil {
		t.Fatal(err)
	}
	n, err := udpConn.Read(buf)
	if err != nil || !bytes.Equal(buf[:n], udpHello) {
		t.Fatalf("udp handshake: %q, %v", buf[:n], err)
	}

	tcpRaw, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tcpRaw.Close()
	_ = tcpRaw.SetDeadline(time.Now().Add(5 * time.Second))
	var tcpHello = hello(uuid.Must(uuid.NewV4()))
	if _, err := tcpRaw.Write(tcpHello); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(tcpRaw, buf[:len(tcpHello)]); err != nil || !bytes.Equal(buf[:len(tcpHello)], tcpHello) {
		t.Fatalf("tcp handshake: %q, %v", buf[:len(tcpHello)], err)
	}
	var tcpConn = frame.NewConn(tcpRaw)

	// The TCP peer registers in its own goroutine after answering the hello.
	for i := 0; len(server.appendPeers(nil)) < 2; i++ {
		if i > 100 {
			t.Fatal("tcp peer not registered")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i := range 10 {
		var packet = bytes.Repeat([]byte{byte(i)}, 1084)
		if _, err := udpConn.Write(packet); err != nil {
			t.Fatal(err)
		}
		n, err := tcpConn.Read(buf)
		if err != nil || !bytes.Equal(buf[:n], packet) {
			t.Fatalf("udp->tcp packet %d: %d bytes, %v", i, n, err)
		}

		packet = bytes.Repeat([]byte{byte(100 + i)}, 1084)
		if _, err := tcpConn.Write(packet); err != nil {
			t.Fatal(err)
		}
		n, err = udpConn.Read(buf)
		if err != nil || !bytes.Equal(buf[:n], packet) {
			t.Fatalf("tcp->udp packet %d: %d bytes, %v", i, n, err)
		}
	}
}

// udpHello dials the server over UDP and completes the handshake for peerID.
func udpHello(t *testing.T, addr string, peerID uuid.UUID) net.Conn {
	t.Helper()
	conn, err := net.Dial("udp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	var h = hello(peerID)
	if _, err := conn.Write(h); err != nil {
		t.Fatal(err)
	}
	var buf = make([]byte, packetBufferSize)
	n, err := conn.Read(buf)
	if err != nil || !bytes.Equal(buf[:n], h) {
		t.Fatalf("udp handshake: %q, %v", buf[:n], err)
	}
	return conn
}

// TestServerFollowsUDPPeerToNewPort repeats a peer's hello from another
// socket, as after a NAT rebinding or a tunnel exit change, and checks that
// packets for the peer go to the new address from then on.
func TestServerFollowsUDPPeerToNewPort(t *testing.T) {
	var addr = freeAddr(t)
	var server = NewServer(addr, uilive.New())
	if err := server.Serve(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	var movingID = uuid.Must(uuid.NewV4())
	var oldConn = udpHello(t, addr, movingID)
	defer oldConn.Close()
	var sender = udpHello(t, addr, uuid.Must(uuid.NewV4()))
	defer sender.Close()

	var newConn = udpHello(t, addr, movingID)
	defer newConn.Close()

	var packet = bytes.Repeat([]byte{7}, 1084)
	if _, err := sender.Write(packet); err != nil {
		t.Fatal(err)
	}
	var buf = make([]byte, packetBufferSize)
	n, err := newConn.Read(buf)
	if err != nil || !bytes.Equal(buf[:n], packet) {
		t.Fatalf("new address: %d bytes, %v", n, err)
	}
	_ = oldConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if n, err := oldConn.Read(buf); err == nil {
		t.Fatalf("old address still gets packets: %d bytes", n)
	}
}

// TestServerDropsSilentUDPPeer checks that a UDP peer without keepalives is
// removed after the timeout, while one that keeps saying hello stays.
func TestServerDropsSilentUDPPeer(t *testing.T) {
	var addr = freeAddr(t)
	var server = NewServer(addr, uilive.New())
	server.udpPeerTimeout = 1500 * time.Millisecond
	if err := server.Serve(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	var silent = udpHello(t, addr, uuid.Must(uuid.NewV4()))
	defer silent.Close()
	var aliveID = uuid.Must(uuid.NewV4())
	var alive = udpHello(t, addr, aliveID)
	defer alive.Close()

	for range 6 {
		time.Sleep(500 * time.Millisecond)
		if _, err := alive.Write(hello(aliveID)); err != nil {
			t.Fatal(err)
		}
	}
	var peers = server.appendPeers(nil)
	if len(peers) != 1 || peers[0].id != aliveID {
		t.Fatalf("want only the peer with keepalives, got %d peers", len(peers))
	}
}
