package src

import (
	"testing"

	"github.com/imakiri/stream/src/amneziawg"
	"github.com/imakiri/stream/src/codec"
	"github.com/imakiri/stream/src/wire"
)

// TestStreamPacketFitsTheTunnel checks that the largest FEC packet, sealed
// for the relay, fits the UDP payload inside an AmneziaWG tunnel of the
// default MTU (IPv4 and UDP headers off), so no packet is fragmented.
func TestStreamPacketFitsTheTunnel(t *testing.T) {
	const ipv4UDPHeaders = 20 + 8
	if size, limit := codec.PacketMaxSize+wire.DataOverhead, amneziawg.DefaultMTU-ipv4UDPHeaders; size > limit {
		t.Fatalf("a sealed stream packet is %d bytes, the tunnel takes %d", size, limit)
	}
}
