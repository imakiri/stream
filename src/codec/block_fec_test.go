package codec

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"
)

// testBlock is the packets of one block of the test stream and the chunks
// that went into it.
type testBlock struct {
	packets []*Packet
	raw     [][]byte
	chunks  [][]byte
}

// encodeTestBlocks runs the encoder by hand: a block per entry of sizes,
// each a chunk of that many bytes of random data, parityPercent parity.
func encodeTestBlocks(t *testing.T, parityPercent uint64, sizes ...[]int) []testBlock {
	t.Helper()
	encoder, err := NewEncoder(10*time.Millisecond, 6, 4, parityPercent)
	if err != nil {
		t.Fatal(err)
	}
	encoder.session = testSession
	encoder.csn = 1
	encoder.resultQueue = make(chan []byte, 4096)
	var ctx = context.Background()
	var blocks []testBlock
	for _, block := range sizes {
		var b testBlock
		for _, size := range block {
			var data = make([]byte, size)
			_, _ = rand.Read(data)
			b.chunks = append(b.chunks, data)
			var now = time.Now()
			if !encoder.sendChunk(ctx, aggregate{data: data, startedAt: now, closedAt: now}) {
				t.Fatal("sendChunk")
			}
		}
		if !encoder.closeBlock(ctx) {
			t.Fatal("closeBlock")
		}
		for len(encoder.resultQueue) > 0 {
			var raw = <-encoder.resultQueue
			var p = new(Packet)
			if !p.Unmarshal(raw) {
				t.Fatal("the encoder made a packet that does not parse")
			}
			b.raw = append(b.raw, raw)
			b.packets = append(b.packets, p)
		}
		blocks = append(blocks, b)
	}
	return blocks
}

// feed starts a decoder and sends it the packets keep lets through.
func feed(t *testing.T, decoder *Decoder, blocks []testBlock, keep func(block int, p *Packet) bool) (<-chan DecodedChunk, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	in, out, err := decoder.Decode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range blocks {
		for j, p := range b.packets {
			if keep(i, p) {
				in <- b.raw[j]
			}
		}
	}
	return out, cancel
}

// expectChunks reads chunks from out and checks they are want, in order.
func expectChunks(t *testing.T, out <-chan DecodedChunk, want [][]byte) {
	t.Helper()
	for i, data := range want {
		select {
		case chunk := <-out:
			if !bytes.Equal(chunk.Data, data) {
				t.Fatalf("chunk %d (csn %d): data differs", i, chunk.CSN)
			}
		case <-time.After(time.Second):
			t.Fatalf("chunk %d of %d not out a second on", i+1, len(want))
		}
	}
}

func expectNothing(t *testing.T, out <-chan DecodedChunk, wait time.Duration) {
	t.Helper()
	select {
	case chunk := <-out:
		t.Fatalf("chunk %d out, want none", chunk.CSN)
	case <-time.After(wait):
	}
}

func isData(p *Packet) bool { return p.kind == PacketKindData }

func descriptorOf(t *testing.T, p *Packet) shardDescriptor {
	t.Helper()
	d, ok := readShardDescriptor(p.shard)
	if !ok {
		t.Fatal("data packet without a descriptor")
	}
	return d
}

func TestPacketSizes(t *testing.T) {
	blocks := encodeTestBlocks(t, 33, []int{6*PacketPayloadSize - ChunkHeaderSize, 100})
	var full, short int
	for i, raw := range blocks[0].raw {
		if len(raw) > PacketMaxSize {
			t.Fatalf("packet %d is %d bytes, over PacketMaxSize %d", i, len(raw), PacketMaxSize)
		}
		if len(raw) == PacketMaxSize {
			full++
		} else {
			short++
		}
	}
	// 6 full data packets + 1 short one (the 100 byte chunk) and 3 parity
	if full != 6+3 || short != 1 {
		t.Fatalf("full packets %d, short %d; want 9 and 1", full, short)
	}
	var corrupt = bytes.Clone(blocks[0].raw[0])
	corrupt[PacketHeaderSize+20] ^= 1
	if new(Packet).Unmarshal(corrupt) {
		t.Fatal("a packet with a wrong hash parses")
	}
}

func TestBlockRoundTrip(t *testing.T) {
	var sizes = []int{1, 1318 - ChunkHeaderSize, 1318 - ChunkHeaderSize + 1, 3000, 6*PacketPayloadSize - ChunkHeaderSize}
	blocks := encodeTestBlocks(t, 33, sizes, sizes[:2])
	decoder, _ := NewDecoder(12)
	out, cancel := feed(t, decoder, blocks, func(int, *Packet) bool { return true })
	defer cancel()
	expectChunks(t, out, append(blocks[0].chunks, blocks[1].chunks...))
	if lost, _ := decoder.Losses(); lost != 0 {
		t.Fatalf("lost %d chunks", lost)
	}
}

// A chunk whose data packets all came goes on before its block's parity.
func TestChunkGoesOnBeforeItsBlockParity(t *testing.T) {
	blocks := encodeTestBlocks(t, 33, []int{3000, 3000})
	decoder, _ := NewDecoder(12)
	out, cancel := feed(t, decoder, blocks, func(_ int, p *Packet) bool {
		return isData(p) && descriptorOf(t, p).csn == 1
	})
	defer cancel()
	expectChunks(t, out, blocks[0].chunks[:1])
}

// The short last piece of a chunk is restored from parity and trimmed to its
// length, and so is a chunk lost whole.
func TestBlockRestoresLostPieces(t *testing.T) {
	blocks := encodeTestBlocks(t, 50, []int{3000, 700, 3000})
	for _, tc := range []struct {
		name string
		drop func(d shardDescriptor) bool
	}{
		{"short last piece", func(d shardDescriptor) bool { return d.csn == 1 && d.piece == d.pieces-1 }},
		{"whole chunk", func(d shardDescriptor) bool { return d.csn == 2 }},
		{"a piece of every chunk", func(d shardDescriptor) bool { return d.piece == 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder, _ := NewDecoder(12)
			out, cancel := feed(t, decoder, blocks, func(_ int, p *Packet) bool {
				return !isData(p) || !tc.drop(descriptorOf(t, p))
			})
			defer cancel()
			expectChunks(t, out, blocks[0].chunks)
		})
	}
}

// With all parity lost the chunks go on from their data packets.
func TestBlockWithoutParity(t *testing.T) {
	blocks := encodeTestBlocks(t, 33, []int{3000, 100, 5000})
	decoder, _ := NewDecoder(12)
	out, cancel := feed(t, decoder, blocks, func(_ int, p *Packet) bool { return isData(p) })
	defer cancel()
	expectChunks(t, out, blocks[0].chunks)
}

// More losses than parity: the complete chunks of the block go on, and the
// lost ones are given up as soon as blockReorderTolerance packets of the next
// block have come, without waiting for a reorder deadline.
func TestBlockBeyondParityIsGivenUpAtTheNextBlock(t *testing.T) {
	blocks := encodeTestBlocks(t, 33, []int{3000, 3000, 3000}, []int{3000, 500})
	decoder, _ := NewDecoder(12)
	out, cancel := feed(t, decoder, blocks, func(block int, p *Packet) bool {
		// k 9, m 3: csn 2 loses its 3 data packets and one parity packet goes too
		return block != 0 || !(isData(p) && descriptorOf(t, p).csn == 2 || p.kind == PacketKindParity && p.index == p.k)
	})
	defer cancel()
	expectChunks(t, out, [][]byte{blocks[0].chunks[0], blocks[0].chunks[2], blocks[1].chunks[0], blocks[1].chunks[1]})
	lost, incomplete := decoder.Losses()
	if lost != 1 || incomplete != 0 {
		t.Fatalf("lost %d, incomplete %d; want 1 and 0 (no packet of csn 2 came)", lost, incomplete)
	}
}

// A slight reordering: the first packets of the next block come before the
// last ones (the parity) of a block with a loss, which still restores it.
func TestBlockTakesLatePacketsWithinTheTolerance(t *testing.T) {
	blocks := encodeTestBlocks(t, 33, []int{3000, 3000}, []int{3000, 3000})
	var first, next = blocks[0], blocks[1]
	var k = int(first.packets[len(first.packets)-1].k)
	decoder, _ := NewDecoder(12)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in, out, err := decoder.Decode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// block 1 without its first data packet and parity, then
	// blockReorderTolerance-1 packets of block 2, then block 1's parity
	for _, raw := range first.raw[1:k] {
		in <- raw
	}
	for _, raw := range next.raw[:blockReorderTolerance-1] {
		in <- raw
	}
	for _, raw := range first.raw[k:] {
		in <- raw
	}
	for _, raw := range next.raw[blockReorderTolerance-1:] {
		in <- raw
	}
	expectChunks(t, out, append(first.chunks, next.chunks...))
}

// A chunk some of whose packets came, too few to restore, counts as
// incomplete.
func TestDecoderCountsIncompleteChunks(t *testing.T) {
	blocks := encodeTestBlocks(t, 33, []int{3000, 3000, 3000}, []int{3000, 3000})
	decoder, _ := NewDecoder(12)
	out, cancel := feed(t, decoder, blocks, func(block int, p *Packet) bool {
		if block != 0 || !isData(p) {
			return block != 0
		}
		return descriptorOf(t, p).piece == 0
	})
	defer cancel()
	expectChunks(t, out, blocks[1].chunks)
	lost, incomplete := decoder.Losses()
	if lost != 3 || incomplete != 3 {
		t.Fatalf("lost %d, incomplete %d; want 3 and 3", lost, incomplete)
	}
}

// One decoder fed by two encoder runs in a row, like a sender restarted while
// the receiver kept running: the second run starts again from csn 1.
func TestDecoderFollowsRestartedEncoder(t *testing.T) {
	first := encodeTestBlocks(t, 33, []int{3000, 100})
	second := encodeTestBlocks(t, 33, []int{200, 4000})
	for _, p := range second[0].packets {
		p.session = testSession + 1
	}
	for i, p := range second[0].packets {
		second[0].raw[i] = p.Marshal()
	}
	decoder, _ := NewDecoder(12)
	out, cancel := feed(t, decoder, append(first, second...), func(int, *Packet) bool { return true })
	defer cancel()
	expectChunks(t, out, append(first[0].chunks, second[0].chunks...))
}

// When nothing comes after a lost chunk, the chunks after it wait only
// ReorderDeadline; without the deadline they wait for newer chunks.
func TestReorderDeadline(t *testing.T) {
	blocks := encodeTestBlocks(t, 33, []int{100, 100, 100})
	var keep = func(_ int, p *Packet) bool { return isData(p) && descriptorOf(t, p).csn != 2 }

	const deadline = 30 * time.Millisecond
	decoder, _ := NewDecoder(12)
	decoder.ReorderDeadline = deadline
	var start = time.Now()
	out, cancel := feed(t, decoder, blocks, keep)
	expectChunks(t, out, blocks[0].chunks[:1])
	expectChunks(t, out, blocks[0].chunks[2:])
	if took := time.Since(start); took < deadline/2 {
		t.Fatalf("chunk 3 out after %v, before the deadline %v", took, deadline)
	}
	cancel()

	decoder, _ = NewDecoder(12)
	out, cancel = feed(t, decoder, blocks, keep)
	defer cancel()
	expectChunks(t, out, blocks[0].chunks[:1])
	expectNothing(t, out, 200*time.Millisecond)
}

// A trickle of input (small packets more often than the chunk's max age, as
// the sound comes between video frames) must not hold a chunk open: it goes
// out max age after its first byte.
func TestEncoderSendsAChunkAtItsMaxAgeUnderATrickle(t *testing.T) {
	const maxAge = 20 * time.Millisecond
	encoder, err := NewEncoder(maxAge, 6, 4, 33)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in, out, err := encoder.Encode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var start = time.Now()
	go func() {
		var tick = time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				select {
				case in <- make([]byte, 100):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	select {
	case <-out:
	case <-time.After(time.Second):
		t.Fatal("no packet a second into a trickle: the chunk waits for the input to pause")
	}
	// first byte at ~5 ms, sent at ~5 ms + maxAge; generous for a loaded machine
	if took := time.Since(start); took > 5*time.Millisecond+maxAge+30*time.Millisecond {
		t.Fatalf("first packet after %v, want about %v", took, 5*time.Millisecond+maxAge)
	}
}

// A block that does not fill is closed (its parity sent) block_chunks times
// the chunk max age after its first chunk.
func TestEncoderClosesABlockThatDoesNotFill(t *testing.T) {
	encoder, err := NewEncoder(5*time.Millisecond, 6, 4, 33)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in, out, err := encoder.Encode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	in <- make([]byte, 100)
	var kinds []uint8
	var deadline = time.After(time.Second)
	for len(kinds) < 2 {
		select {
		case raw := <-out:
			var p Packet
			if !p.Unmarshal(raw) {
				t.Fatal("invalid packet")
			}
			kinds = append(kinds, p.kind)
		case <-deadline:
			t.Fatalf("packets %v a second on, want a data packet and its parity", kinds)
		}
	}
	if kinds[0] != PacketKindData || kinds[1] != PacketKindParity {
		t.Fatalf("packets %v, want data then parity", kinds)
	}
}

func TestInputDelayWraps(t *testing.T) {
	var now = time.UnixMilli(1<<32 + 5)
	if d := InputDelay(uint32((1<<32 - 3)), now); d != 8*time.Millisecond {
		t.Fatalf("delay across the wrap %v, want 8ms", d)
	}
}
