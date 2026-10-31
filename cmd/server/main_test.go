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
