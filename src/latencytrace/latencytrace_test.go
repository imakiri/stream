package latencytrace_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/imakiri/stream/src/codec"
	"github.com/imakiri/stream/src/latencytrace"
)

// TestPacketHeader checks that the trace reads the session, chunk and piece
// of a data packet, and the block and index of a parity packet, where
// codec.Packet puts them.
func TestPacketHeader(t *testing.T) {
	// one chunk of 2 pieces in a block that closes after 1 chunk: 2 data
	// packets, then 1 parity packet (index 2)
	encoder, err := codec.NewEncoder(time.Millisecond, 6, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in, out, err := encoder.Encode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	in <- make([]byte, codec.PacketPayloadSize+1)
	var packets [][]byte
	for range 3 {
		select {
		case p := <-out:
			packets = append(packets, p)
		case <-time.After(time.Second):
			t.Fatalf("%d packets a second on, want 3", len(packets))
		}
	}

	var path = filepath.Join(t.TempDir(), "trace.tsv")
	var at = time.UnixMicro(1_793_000_000_123_456)
	trace, err := latencytrace.Create(path, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range packets {
		trace.Packet(latencytrace.ServerSent, p)
	}
	if err := trace.Close(); err != nil {
		t.Fatal(err)
	}
	var nilTrace *latencytrace.Trace
	nilTrace.Packet(latencytrace.ServerSent, packets[0]) // records nothing, no panic

	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var session = binary.LittleEndian.Uint64(packets[0][8:16])
	var want = fmt.Sprintf("server.sent\t%x\t1\t0\t1793000000123456\nserver.sent\t%x\t1\t1\t1793000000123456\nserver.sent\t%x\t1\t%d\t1793000000123456\n",
		session, session, session, latencytrace.ParityPsnBase+2)
	if string(text) != want {
		t.Fatalf("trace:\n%s\nwant:\n%s", text, strings.TrimSpace(want))
	}
}
