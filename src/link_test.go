package src

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/wire"
)

// TestProxyLinkRedialsWhenProxyStopsEchoing runs a fake UDP proxy that stops
// echoing the hellos, and checks the link dials again, from a new socket, and
// reads packets there.
func TestProxyLinkRedialsWhenProxyStopsEchoing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close()
	var echoing atomic.Bool
	echoing.Store(true)
	var helloFrom = make(chan *net.UDPAddr, 100)
	go func() {
		var buf = make([]byte, packetBufferSize)
		for {
			n, addr, err := fake.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if wire.PacketKind(buf[:n]) != wire.KindProxyHello {
				continue
			}
			helloFrom <- addr
			if echoing.Load() {
				_, _ = fake.WriteToUDP(buf[:n], addr)
			}
		}
	}()

	var port = uint16(fake.LocalAddr().(*net.UDPAddr).Port)
	link, err := dialProxyLink(ctx, ProxyConfig{Addr: "127.0.0.1", Port: port}, wire.RoleClient, uuid.Must(uuid.NewV4()), proxyLinkTiming{
		keepaliveInterval: 50 * time.Millisecond,
		echoTimeout:       200 * time.Millisecond,
		redialMinDelay:    50 * time.Millisecond,
		redialMaxDelay:    200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	// Read takes the echoes, which is how the link sees the proxy alive.
	var packets = make(chan []byte, 10)
	go func() {
		var buf = make([]byte, packetBufferSize)
		for {
			n, err := link.Read(buf)
			if err != nil {
				return
			}
			packets <- bytes.Clone(buf[:n])
		}
	}()

	var first = <-helloFrom
	echoing.Store(false)
	var deadline = time.After(5 * time.Second)
	var second *net.UDPAddr
	for second == nil {
		select {
		case addr := <-helloFrom:
			if addr.Port != first.Port {
				second = addr
			}
		case <-deadline:
			t.Fatal("the link did not dial again")
		}
	}
	echoing.Store(true)

	// send to where the latest hello came from: the socket in use
	var data = []byte{wire.Version, byte(wire.KindData), 1, 2, 3}
	for {
		for drained := false; !drained; {
			select {
			case addr := <-helloFrom:
				second = addr
			default:
				drained = true
			}
		}
		if _, err := fake.WriteToUDP(data, second); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-packets:
			if !bytes.Equal(got, data) {
				t.Fatalf("read %x, want %x", got, data)
			}
			return
		case <-time.After(100 * time.Millisecond):
			// the new socket may not be published yet
		case <-deadline:
			t.Fatal("the link read nothing after dialing again")
		}
	}
}

// TestProxyLinkDropsWritesWhileDown checks that Write neither fails nor
// blocks while there is no proxy, and fails once the link is closed.
func TestProxyLinkDropsWritesWhileDown(t *testing.T) {
	addr, port := freeProxyAddr(t)
	link, err := DialProxy(context.Background(), ProxyConfig{Network: ProxyNetworkTCP, Addr: addr, Port: port}, wire.RoleServer, uuid.Must(uuid.NewV4()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := link.Write([]byte{wire.Version, byte(wire.KindData)}); err != nil {
		t.Fatalf("write while down: %v", err)
	}
	_ = link.Close()
	if _, err := link.Write([]byte{wire.Version, byte(wire.KindData)}); err == nil {
		t.Fatal("write after close did not fail")
	}
	var buf = make([]byte, packetBufferSize)
	if _, err := link.Read(buf); err == nil {
		t.Fatal("read after close did not fail")
	}
}
