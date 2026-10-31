package src

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestHelloEchoFilterConnDropsEchoesOnly(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	raw, err := net.Dial("udp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))

	var hello = append(bytes.Clone(UID), bytes.Repeat([]byte{1}, 16)...)
	var conn = &helloEchoFilterConn{Conn: raw, hello: hello}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	var buf = make([]byte, 64)
	_, client, err := server.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range [][]byte{hello, []byte("first"), hello, hello, []byte("second")} {
		if _, err := server.WriteToUDP(p, client); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"first", "second"} {
		n, err := conn.Read(buf)
		if err != nil || string(buf[:n]) != want {
			t.Fatalf("got %q, %v, want %q", buf[:n], err, want)
		}
	}
}
