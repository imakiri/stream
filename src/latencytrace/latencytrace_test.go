package latencytrace_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/imakiri/stream/src/codec"
	"github.com/imakiri/stream/src/latencytrace"
)

// TestPacketHeader checks that the trace reads the session, chunk and index
// of a packet where codec.PacketV2 puts them.
func TestPacketHeader(t *testing.T) {
	var path = filepath.Join(t.TempDir(), "trace.tsv")
	var at = time.UnixMicro(1_793_000_000_123_456)
	trace, err := latencytrace.Create(path, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	packet, _ := codec.NewPacketV2(codec.KindData, 77, 9, 0xabc, []byte("x"))
	trace.Packet(latencytrace.ServerSent, packet.Marshal())
	trace.Chunk(latencytrace.ServerInput, 0xabc, 77, at.Add(-time.Millisecond))
	if err := trace.Close(); err != nil {
		t.Fatal(err)
	}
	var nilTrace *latencytrace.Trace
	nilTrace.Packet(latencytrace.ServerSent, packet.Marshal()) // records nothing, no panic

	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var want = "server.sent\tabc\t77\t9\t1793000000123456\nserver.input\tabc\t77\t-1\t1793000000122456\n"
	if string(text) != want {
		t.Fatalf("trace:\n%s\nwant:\n%s", text, strings.TrimSpace(want))
	}
}
