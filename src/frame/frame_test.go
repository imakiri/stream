package frame

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
)

// TestConnKeepsPacketBoundaries checks that packets written one by one come
// out one by one, even when the stream delivers them in arbitrary pieces.
func TestConnKeepsPacketBoundaries(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	var packets = [][]byte{
		bytes.Repeat([]byte{1}, 1084),
		{},
		bytes.Repeat([]byte{2}, 7),
		bytes.Repeat([]byte{3}, MaxPayloadSize),
	}

	go func() {
		var w = NewConn(client)
		for _, p := range packets {
			if _, err := w.Write(p); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	var r = NewConn(server)
	var buf = make([]byte, MaxPayloadSize)
	for i, want := range packets {
		n, err := r.Read(buf)
		if err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
		if !bytes.Equal(buf[:n], want) {
			t.Fatalf("packet %d: got %d bytes, want %d", i, n, len(want))
		}
	}
}

func TestConnReadShortBufferSkipsFrame(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		var w = NewConn(client)
		_, _ = w.Write([]byte("too long"))
		_, _ = w.Write([]byte("ok"))
	}()

	var r = NewConn(server)
	var buf = make([]byte, 4)
	if _, err := r.Read(buf); !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("want io.ErrShortBuffer, got %v", err)
	}
	n, err := r.Read(buf)
	if err != nil || string(buf[:n]) != "ok" {
		t.Fatalf("got %q, %v", buf[:n], err)
	}
}

func TestConnWriteRejectsOversizedPayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	if _, err := NewConn(client).Write(make([]byte, MaxPayloadSize+1)); err == nil {
		t.Fatal("want error")
	}
}
