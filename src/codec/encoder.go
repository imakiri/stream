package codec

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/imakiri/stream/src/latencytrace"
	"github.com/klauspost/reedsolomon"
)

// Encoder cuts the input stream into chunks and the chunks into packets with
// Reed-Solomon parity over blocks of chunks; see packet.go for the layout.
type Encoder struct {
	// InputClock is the clock of the encoder's time stamps: each chunk
	// carries the time its first byte arrived, which the clients compare
	// with their own clock for the end-to-end delay, and Trace's events.
	// time.Now if nil. Set it before Encode.
	InputClock func() time.Time
	// Trace records when each chunk and packet passes the stages of the
	// encoder; nil records nothing. Set it before Encode.
	Trace *latencytrace.Trace

	aggregator struct {
		mu *sync.Mutex
		// maxAge is how long a chunk may wait for more input after its
		// first byte; then it is sent as it is. The timer runs while buf
		// holds data.
		maxAge time.Duration
		timer  *time.Timer
		buf    []byte
		// bufStartedAt is when the first byte now in buf arrived
		bufStartedAt time.Time
	}
	block struct {
		// chunks is how many chunks a block takes before it is closed;
		// maxAge how long after its first chunk it is closed with fewer
		chunks uint64
		maxAge time.Duration
		// parityPercent: a block of k data packets gets
		// ceil(k*parityPercent/100) parity packets
		parityPercent uint64
		codes         map[[2]int]reedsolomon.Encoder

		// the open block: id is the csn of its first chunk, shards its
		// data shards so far (each ShardSize long, zero padded)
		id     uint64
		shards [][]byte
		count  uint64
	}
	csn     uint64
	session uint64 // random per Encode call, see Packet.session

	aggregateQueue chan []byte
	encodeQueue    chan aggregate
	resultQueue    chan []byte
}

// NewEncoder makes an encoder of chunks of at most chunkPackets packets of
// data, sent at most chunkMaxAge after their first byte came in, with
// parityPercent parity over blocks of blockChunks chunks; a block that does
// not fill is closed blockChunks*chunkMaxAge after its first chunk.
func NewEncoder(chunkMaxAge time.Duration, chunkPackets, blockChunks, parityPercent uint64) (*Encoder, error) {
	switch {
	case chunkPackets == 0 || chunkPackets > 255:
		return nil, fmt.Errorf("chunk packets %d: want 1 to 255", chunkPackets)
	case blockChunks == 0:
		return nil, errors.New("block chunks cannot be zero")
	case parityPercent == 0 || parityPercent > 100:
		return nil, fmt.Errorf("parity percent %d: want 1 to 100", parityPercent)
	case chunkPackets*blockChunks*(100+parityPercent)/100+1 > maxBlockShards:
		return nil, fmt.Errorf("a block of %d chunks of %d packets with %d%% parity is over %d packets", blockChunks, chunkPackets, parityPercent, maxBlockShards)
	}
	var encoder = new(Encoder)
	encoder.aggregator.mu = new(sync.Mutex)
	encoder.aggregator.buf = make([]byte, 0, chunkPackets*PacketPayloadSize-ChunkHeaderSize)
	encoder.aggregator.maxAge = chunkMaxAge
	encoder.aggregator.timer = time.NewTimer(time.Hour)
	encoder.aggregator.timer.Stop()
	encoder.block.chunks = blockChunks
	encoder.block.maxAge = time.Duration(blockChunks) * chunkMaxAge
	encoder.block.parityPercent = parityPercent
	encoder.block.codes = make(map[[2]int]reedsolomon.Encoder)
	return encoder, nil
}

// IncomingSize is the input a full chunk takes.
func (encoder *Encoder) IncomingSize() uint64 {
	return uint64(cap(encoder.aggregator.buf))
}

// aggregate is the data of one chunk, when its first byte arrived and when
// it was closed.
type aggregate struct {
	data      []byte
	startedAt time.Time
	closedAt  time.Time
}

func (encoder *Encoder) now() time.Time {
	if encoder.InputClock != nil {
		return encoder.InputClock()
	}
	return time.Now()
}

func (encoder *Encoder) aggregatorFlush(ctx context.Context) {
	encoder.aggregator.mu.Lock()
	defer encoder.aggregator.mu.Unlock()

	var agData = make([]byte, len(encoder.aggregator.buf))
	copy(agData, encoder.aggregator.buf)
	encoder.aggregator.buf = encoder.aggregator.buf[0:0]
	select {
	case <-ctx.Done():
		return
	case encoder.encodeQueue <- aggregate{data: agData, startedAt: encoder.aggregator.bufStartedAt, closedAt: encoder.now()}:
		return
	}
}

// send puts a packet out; false if ctx is done.
func (encoder *Encoder) send(ctx context.Context, packet *Packet) bool {
	var b = packet.Marshal()
	encoder.Trace.Packet(latencytrace.ServerEncoderOut, b)
	select {
	case <-ctx.Done():
		return false
	case encoder.resultQueue <- b:
		return true
	}
}

// parityCount is the parity packets of a block of k data packets.
func (encoder *Encoder) parityCount(k int) int {
	return (k*int(encoder.block.parityPercent) + 99) / 100
}

// code returns the Reed-Solomon code of k data and m parity shards, made
// once per pair.
func (encoder *Encoder) code(k, m int) (reedsolomon.Encoder, error) {
	if rs, ok := encoder.block.codes[[2]int{k, m}]; ok {
		return rs, nil
	}
	rs, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, err
	}
	encoder.block.codes[[2]int{k, m}] = rs
	return rs, nil
}

// sendChunk puts the chunk's data packets out in the open block, opening
// one if none is; false if ctx is done.
func (encoder *Encoder) sendChunk(ctx context.Context, input aggregate) bool {
	var chunk = NewChunk(encoder.csn, KindData, input.data)
	chunk.inputUnixMilli = uint32(input.startedAt.UnixMilli())
	var b = chunk.Marshal()
	var pieces = (len(b) + PacketPayloadSize - 1) / PacketPayloadSize
	if encoder.block.count == 0 {
		encoder.block.id = encoder.csn
	}
	if encoder.Trace != nil {
		encoder.Trace.Chunk(latencytrace.ServerInput, encoder.session, encoder.csn, input.startedAt)
		encoder.Trace.Chunk(latencytrace.ServerChunk, encoder.session, encoder.csn, input.closedAt)
		encoder.Trace.Chunk(latencytrace.ServerShards, encoder.session, encoder.csn, encoder.now())
	}
	for piece := 0; piece < pieces; piece++ {
		var bytes = b[piece*PacketPayloadSize : min(len(b), (piece+1)*PacketPayloadSize)]
		var shard = make([]byte, ShardSize)
		shardDescriptor{csn: encoder.csn, piece: uint8(piece), pieces: uint8(pieces), length: uint16(len(bytes))}.put(shard)
		copy(shard[ShardDescriptorSize:], bytes)
		var packet = &Packet{
			kind:    PacketKindData,
			index:   uint16(len(encoder.block.shards)),
			session: encoder.session,
			block:   encoder.block.id,
			shard:   shard[:ShardDescriptorSize+len(bytes)],
		}
		encoder.block.shards = append(encoder.block.shards, shard)
		if !encoder.send(ctx, packet) {
			return false
		}
	}
	encoder.block.count++
	encoder.csn++
	return true
}

// closeBlock puts the parity packets of the open block out and starts a new
// one; false if ctx is done.
func (encoder *Encoder) closeBlock(ctx context.Context) bool {
	if encoder.block.count == 0 {
		return true
	}
	var k = len(encoder.block.shards)
	var m = encoder.parityCount(k)
	var shards = encoder.block.shards
	encoder.block.shards = nil
	encoder.block.count = 0
	rs, err := encoder.code(k, m)
	if err != nil {
		log.Printf("encode: block %d: reedsolomon.New(%d, %d): %v", encoder.block.id, k, m, err)
		return true
	}
	for range m {
		shards = append(shards, make([]byte, ShardSize))
	}
	if err = rs.Encode(shards); err != nil {
		log.Printf("encode: block %d: rs.Encode: %v", encoder.block.id, err)
		return true
	}
	for i := k; i < k+m; i++ {
		var packet = &Packet{
			kind:    PacketKindParity,
			index:   uint16(i),
			k:       uint16(k),
			m:       uint16(m),
			session: encoder.session,
			block:   encoder.block.id,
			shard:   shards[i],
		}
		if !encoder.send(ctx, packet) {
			return false
		}
	}
	return true
}

// encode sends each chunk's data packets as it comes and closes a block when
// it has block.chunks chunks or its first chunk is block.maxAge old.
func (encoder *Encoder) encode(ctx context.Context) {
	defer close(encoder.resultQueue)
	encoder.csn = 1
	var blockTimer = time.NewTimer(time.Hour)
	blockTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-blockTimer.C:
			if !encoder.closeBlock(ctx) {
				return
			}
		case input, ok := <-encoder.encodeQueue:
			if !ok {
				encoder.closeBlock(ctx)
				return
			}
			if encoder.block.count == 0 {
				blockTimer.Reset(encoder.block.maxAge)
			}
			if !encoder.sendChunk(ctx, input) {
				return
			}
			if encoder.block.count == encoder.block.chunks {
				blockTimer.Stop()
				if !encoder.closeBlock(ctx) {
					return
				}
			}
		}
	}
}

// aggregate collects the input into chunks: a chunk goes on when it is full
// or when its first byte is aggregator.maxAge old, whichever comes first. The
// age counts from the first byte, not from the last input: a trickle (the
// sound's small packets every few ms between video frames) would otherwise
// hold a chunk open far longer.
func (encoder *Encoder) aggregate(ctx context.Context) {
	defer close(encoder.encodeQueue)
	var data []byte
	var sep int
	for {
		select {
		case <-ctx.Done():
			return
		case <-encoder.aggregator.timer.C:
			if len(encoder.aggregator.buf) > 0 {
				encoder.aggregatorFlush(ctx)
			}
		case data = <-encoder.aggregateQueue:
			var arrivedAt = encoder.now()
			for len(data) > 0 {
				if len(encoder.aggregator.buf) == 0 {
					encoder.aggregator.bufStartedAt = arrivedAt
					encoder.aggregator.timer.Reset(encoder.aggregator.maxAge)
				}
				sep = min(cap(encoder.aggregator.buf)-len(encoder.aggregator.buf), len(data))
				encoder.aggregator.buf = append(encoder.aggregator.buf, data[:sep]...)
				data = data[sep:]
				if len(encoder.aggregator.buf) == cap(encoder.aggregator.buf) {
					encoder.aggregator.timer.Stop()
					encoder.aggregatorFlush(ctx)
				}
			}
		}
	}
}

// newSession returns a random non-zero session ID.
func newSession() (uint64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, err
		}
		if session := binary.LittleEndian.Uint64(b[:]); session != 0 {
			return session, nil
		}
	}
}

// Encode starts the encoder under a new session ID. The in channel is never
// closed by the encoder, so writers may keep sending until ctx is done; out is
// closed once the encoder has stopped after ctx is done.
func (encoder *Encoder) Encode(ctx context.Context) (in chan<- []byte, out <-chan []byte, err error) {
	encoder.session, err = newSession()
	if err != nil {
		return nil, nil, errors.Wrap(err, "newSession")
	}
	encoder.resultQueue = make(chan []byte, maxBlockShards)
	encoder.encodeQueue = make(chan aggregate, 64)
	encoder.aggregateQueue = make(chan []byte, 512)

	go encoder.aggregate(ctx)
	go encoder.encode(ctx)
	return encoder.aggregateQueue, encoder.resultQueue, nil
}
