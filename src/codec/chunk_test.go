package codec

import (
	"crypto/rand"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestChunk(t *testing.T) {
	const size = 50 * PacketV2Size
	var data = make([]byte, size)
	n, _ := rand.Read(data)
	require.EqualValues(t, size, n)

	const csn = 3
	const kind = 1
	const parts = 8
	var chunk, rem = NewChunk(parts, csn, kind, data)
	require.NotNil(t, rem)

	chunk, rem = NewChunk(parts, csn, kind, data[:parts*PacketV2DataSize])
	require.Nil(t, rem)
	require.EqualValues(t, parts, chunk.parts)
	require.EqualValues(t, csn, chunk.csn)
	require.EqualValues(t, kind, chunk.kind)
	require.EqualValues(t, parts*PacketV2DataSize, chunk.size)
	require.EqualValues(t, data[:parts*PacketV2DataSize], chunk.data)

	chunk, rem = NewChunk(parts, csn, kind, data[:PacketV2DataSize])
	require.Nil(t, rem)
	require.EqualValues(t, parts, chunk.parts)
	require.EqualValues(t, csn, chunk.csn)
	require.EqualValues(t, kind, chunk.kind)
	require.EqualValues(t, PacketV2DataSize, chunk.size)
	require.EqualValues(t, parts, chunk.Parts())
	require.EqualValues(t, data[:PacketV2DataSize], chunk.data)

	const total = 10
	var payload = chunk.Marshal(total)
	require.EqualValues(t, total, len(payload))
	t.Log(payload)

	payload = payload[:parts] // Drop extra allocated parts

	chunk = new(Chunk)
	var ok = chunk.Unmarshal(payload)
	require.True(t, ok)

	require.EqualValues(t, parts, chunk.parts)
	require.EqualValues(t, csn, chunk.csn)
	require.EqualValues(t, kind, chunk.kind)
	require.EqualValues(t, PacketV2DataSize, chunk.size)
	require.EqualValues(t, parts, chunk.Parts())
	require.EqualValues(t, data[:PacketV2DataSize], chunk.data)
}

// TestInputDelayWraps checks the delay across the wrap of the 32-bit
// millisecond clock.
func TestInputDelayWraps(t *testing.T) {
	var now = time.UnixMilli(1<<32 + 5)
	if got := InputDelay(1<<32-10, now); got != 15*time.Millisecond {
		t.Fatalf("delay across the wrap: %s, want 15ms", got)
	}
	if got := InputDelay(uint32(now.UnixMilli())+20, now); got != -20*time.Millisecond {
		t.Fatalf("input after now: %s, want -20ms", got)
	}
}
