package codec

import (
	"bytes"
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"github.com/imakiri/stream/src/latencytrace"
	"github.com/klauspost/reedsolomon"
)

// openBlock collects the packets of a block being received (the current or
// the previous one, see Decoder.assembler); the layout is in packet.go.
type openBlock struct {
	id uint64
	// k and m are the block's data and parity packets; 0 until a parity
	// packet tells them
	k, m int
	// shards by index: data shards as they came (short), parity full
	shards [][]byte
	found  int
	// restored: the parity has been used, every chunk that could be made
	// is out; later packets of the block are surplus
	restored bool
	// givenUp: given up (or never opened); its chunks not out are lost
	givenUp bool
	// pieces of each chunk of the block by csn, and the chunks sent on
	pieces    map[uint64][][]byte
	delivered map[uint64]bool
}

func newOpenBlock() openBlock {
	return openBlock{shards: make([][]byte, maxBlockShards), pieces: make(map[uint64][][]byte), delivered: make(map[uint64]bool), givenUp: true}
}

func (b *openBlock) reset(id uint64) {
	b.id = id
	b.givenUp = id == 0
	b.k, b.m = 0, 0
	clear(b.shards)
	b.found = 0
	b.restored = false
	clear(b.pieces)
	clear(b.delivered)
}

// dispatchItem is what the assembler hands the dispatcher: a chunk, or the
// news that every chunk of session before csn lostBefore that has not come
// will not come.
type dispatchItem struct {
	chunk      *Chunk
	session    uint64
	lostBefore uint64
}

type Decoder struct {
	// ReorderDeadline, when not zero, is how long restored chunks wait for
	// a missing chunk before it: then it is given up and they go on. The
	// assembler gives a block's missing chunks up as soon as a few packets
	// of the next block have come (all of the block was sent before it); the
	// deadline is for when nothing comes after the loss. Without it a
	// missing chunk is given up only when dispatcherSize newer chunks have
	// come. Set it before Decode.
	ReorderDeadline time.Duration

	// trace records when each chunk is restored; nil records nothing. It
	// may be set while the decoder runs, see SetTrace.
	trace atomic.Pointer[latencytrace.Trace]

	assembler struct {
		// current is the newest block packets came of; previous the one
		// before it, still taking packets that come late until current has
		// blockReorderTolerance packets, see takePacket
		current, previous openBlock
		// session is the encoder run the packets are taken from; packets of
		// previousSession are late leftovers of a restarted sender.
		session         uint64
		previousSession uint64
		codes           map[[2]int]reedsolomon.Encoder

		ok  *uint64
		err *uint64
	}
	dispatcher struct {
		session  uint64
		size     uint64
		first    uint64
		last     uint64
		awaiting uint64
		chunks   []*Chunk
		// waitingSince is when restored chunks started to wait for a
		// missing one before them; zero while none wait.
		waitingSince time.Time

		ok  *uint64
		err *uint64
	}

	// lostChunks counts the chunks the dispatcher skipped in the stream;
	// incompleteChunks those of them some packets of which came, too few
	// to restore. See Losses.
	lostChunks       atomic.Uint64
	incompleteChunks atomic.Uint64

	argumentQueue chan []byte
	assemblyQueue chan *Packet
	dispatchQueue chan dispatchItem
	resultQueue   chan DecodedChunk
}

// Losses returns the chunks lost since the decoder started: lost counts the
// chunks skipped in the stream, incomplete those of them that came with too
// few packets to restore (the others did not come at all).
func (decoder *Decoder) Losses() (lost, incomplete uint64) {
	return decoder.lostChunks.Load(), decoder.incompleteChunks.Load()
}

// SetTrace starts recording when chunks are restored to trace, or stops with
// nil.
func (decoder *Decoder) SetTrace(trace *latencytrace.Trace) {
	decoder.trace.Store(trace)
}

// DecodedChunk is the data of one restored chunk, in stream order.
type DecodedChunk struct {
	Data []byte
	// Session and CSN name the chunk, as the latency trace does.
	Session, CSN uint64
	// InputUnixMilli is when the chunk reached the encoder, by the sender's
	// clock; see InputDelay.
	InputUnixMilli uint32
}

func decoded(chunk *Chunk) DecodedChunk {
	return DecodedChunk{Data: chunk.Data(), Session: chunk.session, CSN: chunk.csn, InputUnixMilli: chunk.inputUnixMilli}
}

// NewDecoder makes a decoder that puts restored chunks back in order in a
// window of dispatcherSize chunks.
func NewDecoder(dispatcherSize uint64) (decoder *Decoder, err error) {
	if dispatcherSize == 0 {
		return nil, errors.New("dispatcherSize cannot be zero")
	}
	decoder = new(Decoder)
	decoder.dispatcher.size = dispatcherSize
	decoder.dispatcher.awaiting = 1
	decoder.dispatcher.chunks = make([]*Chunk, decoder.dispatcher.size)
	decoder.dispatcher.ok = new(uint64)
	decoder.dispatcher.err = new(uint64)

	decoder.assembler.current = newOpenBlock()
	decoder.assembler.previous = newOpenBlock()
	decoder.assembler.codes = make(map[[2]int]reedsolomon.Encoder)
	decoder.assembler.ok = new(uint64)
	decoder.assembler.err = new(uint64)
	return decoder, nil
}

// resetDispatcher starts ordering chunks of a new session from the first
// news of it: chunk csn, or the chunks before csn given up. Chunks restored
// from parity come after the chunks behind them, so the first chunk seen is
// not necessarily the first one sent: if csn is within the reorder window from
// the start, the encoder has just started and csn 1 is awaited; otherwise the
// decoder joined a running stream and starts from csn, losing at most the few
// before it.
func (decoder *Decoder) resetDispatcher(session, csn uint64) {
	var awaiting uint64 = 1
	if csn > decoder.dispatcher.size {
		awaiting = csn
	}
	log.Printf("dispatch: new session %x awaiting csn %d", session, awaiting)
	decoder.dispatcher.session = session
	decoder.dispatcher.awaiting = awaiting
	decoder.dispatcher.first = awaiting - 1
	decoder.dispatcher.last = awaiting - 1
	clear(decoder.dispatcher.chunks)
	decoder.dispatcher.waitingSince = time.Time{}
}

// dropSkippedChunks drops the chunks in the window a slot collision has
// already skipped past: older than the awaited one, they can never go on.
func (decoder *Decoder) dropSkippedChunks() {
	for at, held := range decoder.dispatcher.chunks {
		if held != nil && held.csn < decoder.dispatcher.awaiting {
			decoder.dispatcher.chunks[at] = nil
		}
	}
}

// waiting tells whether restored chunks wait in the window for a missing one.
func (decoder *Decoder) waiting() bool {
	for _, held := range decoder.dispatcher.chunks {
		if held != nil && held.csn >= decoder.dispatcher.awaiting {
			return true
		}
	}
	return false
}

// sendInOrder sends on the held chunks that are in order from awaiting.
// False if ctx is done.
func (decoder *Decoder) sendInOrder(ctx context.Context) bool {
	for {
		var at = decoder.dispatcher.awaiting % decoder.dispatcher.size
		var next = decoder.dispatcher.chunks[at]
		if next == nil || next.csn != decoder.dispatcher.awaiting {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case decoder.resultQueue <- decoded(next):
			decoder.dispatcher.last = next.csn
			decoder.dispatcher.awaiting++
			decoder.dispatcher.chunks[at] = nil
		}
	}
}

// giveUpMissing skips the chunks missing before the oldest chunk waiting in
// the window and sends on what then is in order. False if ctx is done.
func (decoder *Decoder) giveUpMissing(ctx context.Context) bool {
	var oldest *Chunk
	for _, held := range decoder.dispatcher.chunks {
		if held != nil && held.csn >= decoder.dispatcher.awaiting && (oldest == nil || held.csn < oldest.csn) {
			oldest = held
		}
	}
	if oldest == nil {
		return true
	}
	decoder.lostChunks.Add(oldest.csn - decoder.dispatcher.awaiting)
	decoder.dispatcher.awaiting = oldest.csn
	return decoder.sendInOrder(ctx)
}

// giveUpBefore sends on the held chunks before csn bound in order, skips the
// missing ones, and then what is in order from bound. False if ctx is done.
func (decoder *Decoder) giveUpBefore(ctx context.Context, bound uint64) bool {
	for decoder.dispatcher.awaiting < bound {
		var at = decoder.dispatcher.awaiting % decoder.dispatcher.size
		var held = decoder.dispatcher.chunks[at]
		if held != nil && held.csn == decoder.dispatcher.awaiting {
			select {
			case <-ctx.Done():
				return false
			case decoder.resultQueue <- decoded(held):
				decoder.dispatcher.chunks[at] = nil
			}
		} else {
			decoder.lostChunks.Add(1)
		}
		decoder.dispatcher.awaiting++
	}
	decoder.dispatcher.last = max(decoder.dispatcher.last, bound-1)
	decoder.dispatcher.first = max(decoder.dispatcher.first, decoder.dispatcher.last)
	decoder.dropSkippedChunks()
	return decoder.sendInOrder(ctx)
}

// takeChunk puts a restored chunk in order: sends it on if it is the awaited
// one, else holds it in the window. False if ctx is done.
func (decoder *Decoder) takeChunk(ctx context.Context, chunk *Chunk) bool {
	if chunk.csn == 0 {
		log.Println("dispatch: dropping invalid chunk: csn == 0")
		atomic.AddUint64(decoder.dispatcher.err, 1)
		return true
	}
	if chunk.session != decoder.dispatcher.session {
		decoder.resetDispatcher(chunk.session, chunk.csn)
	}
	if chunk.csn <= decoder.dispatcher.last {
		log.Printf("dispatch: detached chunk: csn: %d", chunk.csn)
		atomic.AddUint64(decoder.dispatcher.err, 1)
		return true
	}
	decoder.dispatcher.first = max(decoder.dispatcher.first, chunk.csn)
	var at = chunk.csn % decoder.dispatcher.size
	if chunk.csn != decoder.dispatcher.awaiting {
		var held = decoder.dispatcher.chunks[at]
		if held != nil && held.csn != chunk.csn {
			// the window is full: give up what is missing before the
			// chunk holding the slot
			if !decoder.giveUpBefore(ctx, held.csn) {
				return false
			}
			atomic.AddUint64(decoder.dispatcher.err, 1)
		}
		if chunk.csn != decoder.dispatcher.awaiting {
			decoder.dispatcher.chunks[at] = chunk
			atomic.AddUint64(decoder.dispatcher.ok, 1)
			return true
		}
	}
	decoder.dispatcher.chunks[at] = chunk
	atomic.AddUint64(decoder.dispatcher.ok, 1)
	return decoder.sendInOrder(ctx)
}

func (decoder *Decoder) dispatch(ctx context.Context) {
	defer close(decoder.resultQueue)
	var deadline = time.NewTimer(time.Hour)
	deadline.Stop()
	var deadlineC <-chan time.Time
	for {
		deadline.Stop()
		deadlineC = nil
		if decoder.ReorderDeadline > 0 {
			decoder.dropSkippedChunks()
			if !decoder.waiting() {
				decoder.dispatcher.waitingSince = time.Time{}
			} else {
				if decoder.dispatcher.waitingSince.IsZero() {
					decoder.dispatcher.waitingSince = time.Now()
				}
				deadline.Reset(time.Until(decoder.dispatcher.waitingSince.Add(decoder.ReorderDeadline)))
				deadlineC = deadline.C
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineC:
			if !decoder.giveUpMissing(ctx) {
				return
			}
			// the chunks still waiting now wait for the next gap
			decoder.dispatcher.waitingSince = time.Time{}
		case item, ok := <-decoder.dispatchQueue:
			if !ok {
				return
			}
			switch {
			case item.chunk != nil:
				if !decoder.takeChunk(ctx, item.chunk) {
					return
				}
			case item.lostBefore != 0:
				if item.session != decoder.dispatcher.session {
					decoder.resetDispatcher(item.session, item.lostBefore)
				}
				if !decoder.giveUpBefore(ctx, item.lostBefore) {
					return
				}
				decoder.dispatcher.waitingSince = time.Time{}
			}
		}
	}
}

// startSession drops everything collected so far and takes packets of session
// from now on.
func (decoder *Decoder) startSession(session uint64) {
	log.Printf("assembly: new session %x", session)
	decoder.assembler.previousSession = decoder.assembler.session
	decoder.assembler.session = session
	decoder.assembler.current.reset(0)
	decoder.assembler.previous.reset(0)
}

// code returns the Reed-Solomon code of k data and m parity shards, made
// once per pair.
func (decoder *Decoder) code(k, m int) (reedsolomon.Encoder, error) {
	if rs, ok := decoder.assembler.codes[[2]int{k, m}]; ok {
		return rs, nil
	}
	rs, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, err
	}
	decoder.assembler.codes[[2]int{k, m}] = rs
	return rs, nil
}

// send hands an item to the dispatcher; false if ctx is done.
func (decoder *Decoder) send(ctx context.Context, item dispatchItem) bool {
	select {
	case <-ctx.Done():
		return false
	case decoder.dispatchQueue <- item:
		return true
	}
}

// takePiece files a data shard's piece under its chunk and sends the chunk
// on once it has all its pieces. False if ctx is done.
func (decoder *Decoder) takePiece(ctx context.Context, block *openBlock, shard []byte) bool {
	d, ok := readShardDescriptor(shard)
	if !ok {
		atomic.AddUint64(decoder.assembler.err, 1)
		return true
	}
	if block.delivered[d.csn] {
		return true
	}
	var pieces = block.pieces[d.csn]
	if pieces == nil {
		pieces = make([][]byte, d.pieces)
		block.pieces[d.csn] = pieces
	}
	if len(pieces) != int(d.pieces) {
		atomic.AddUint64(decoder.assembler.err, 1)
		return true
	}
	pieces[d.piece] = d.pieceOf(shard)
	for _, piece := range pieces {
		if piece == nil {
			return true
		}
	}
	block.delivered[d.csn] = true
	var chunk = new(Chunk)
	if !chunk.Unmarshal(bytes.Join(pieces, nil)) || chunk.csn != d.csn {
		log.Printf("assembly: chunk %d: its pieces do not make a chunk", d.csn)
		atomic.AddUint64(decoder.assembler.err, 1)
		return true
	}
	chunk.session = decoder.assembler.session
	decoder.trace.Load().Chunk(latencytrace.ClientRestored, chunk.session, chunk.csn, time.Time{})
	return decoder.send(ctx, dispatchItem{chunk: chunk})
}

// restoreBlock restores the missing data shards of the block from parity and
// sends on the chunks they complete. False if ctx is done.
func (decoder *Decoder) restoreBlock(ctx context.Context, block *openBlock) bool {
	block.restored = true
	rs, err := decoder.code(block.k, block.m)
	if err != nil {
		log.Printf("restore: block %d: reedsolomon.New(%d, %d): %v", block.id, block.k, block.m, err)
		return true
	}
	var shards = make([][]byte, block.k+block.m)
	var missing []int
	for i := range shards {
		switch {
		case block.shards[i] == nil:
			if i < block.k {
				missing = append(missing, i)
			}
		case i < block.k:
			// the code counts every data shard ShardSize long
			shards[i] = make([]byte, ShardSize)
			copy(shards[i], block.shards[i])
		default:
			shards[i] = block.shards[i]
		}
	}
	if len(missing) == 0 {
		return true
	}
	if err = rs.ReconstructData(shards); err != nil {
		log.Printf("restore: block %d: rs.ReconstructData: %v", block.id, err)
		atomic.AddUint64(decoder.assembler.err, 1)
		return true
	}
	for _, i := range missing {
		if !decoder.takePiece(ctx, block, shards[i]) {
			return false
		}
	}
	return true
}

// blockReorderTolerance is how many packets of the next block may come
// before the last late packet of a block. The encoder sends all of a block
// before the next one, so on a link that keeps the order a packet of the next
// block means nothing more of this one comes; a few packets of slack keep a
// slight reordering from costing a whole block.
const blockReorderTolerance = 4

// giveUpBlock gives the block up: its chunks that have not gone on are lost,
// all of the chunks before csn next. False if ctx is done.
func (decoder *Decoder) giveUpBlock(ctx context.Context, block *openBlock, next uint64) bool {
	if block.givenUp {
		return true
	}
	block.givenUp = true
	for csn := range block.pieces {
		if !block.delivered[csn] {
			decoder.incompleteChunks.Add(1)
		}
	}
	return decoder.send(ctx, dispatchItem{session: decoder.assembler.session, lostBefore: next})
}

// takePacket files a packet in its block: the current one, the one before it
// (a late packet), or a new one, which makes the current one previous and
// gives the previous one up. False if ctx is done.
func (decoder *Decoder) takePacket(ctx context.Context, packet *Packet) bool {
	if packet.session != decoder.assembler.session {
		if decoder.assembler.session != 0 && packet.session == decoder.assembler.previousSession {
			// late packet of the sender run before the restart
			return true
		}
		decoder.startSession(packet.session)
	}
	var current, previous = &decoder.assembler.current, &decoder.assembler.previous
	var block *openBlock
	switch {
	case packet.block == current.id:
		block = current
	case packet.block > current.id:
		if !decoder.giveUpBlock(ctx, previous, current.id) {
			return false
		}
		decoder.assembler.current, decoder.assembler.previous = decoder.assembler.previous, decoder.assembler.current
		current.reset(packet.block)
		block = current
	case packet.block == previous.id && !previous.givenUp:
		block = previous
	default:
		// late packet of a block given up already
		atomic.AddUint64(decoder.assembler.err, 1)
		return true
	}
	if !decoder.addPacket(ctx, block, packet) {
		return false
	}
	if block == current && current.found >= blockReorderTolerance {
		return decoder.giveUpBlock(ctx, previous, current.id)
	}
	return true
}

// addPacket files a packet in block. False if ctx is done.
func (decoder *Decoder) addPacket(ctx context.Context, block *openBlock, packet *Packet) bool {
	if block.restored || block.shards[packet.index] != nil {
		// surplus parity of a restored block, or a duplicate datagram
		return true
	}
	if packet.kind == PacketKindParity {
		if block.k != 0 && (block.k != int(packet.k) || block.m != int(packet.m)) {
			log.Printf("assembly: block %d: parity says %d+%d, earlier %d+%d", block.id, packet.k, packet.m, block.k, block.m)
			atomic.AddUint64(decoder.assembler.err, 1)
			return true
		}
		block.k, block.m = int(packet.k), int(packet.m)
	} else if block.k != 0 && int(packet.index) >= block.k {
		atomic.AddUint64(decoder.assembler.err, 1)
		return true
	}
	block.shards[packet.index] = packet.shard
	block.found++
	atomic.AddUint64(decoder.assembler.ok, 1)
	if packet.kind == PacketKindData && !decoder.takePiece(ctx, block, packet.shard) {
		return false
	}
	if block.k != 0 && block.found >= block.k {
		return decoder.restoreBlock(ctx, block)
	}
	return true
}

func (decoder *Decoder) assembly(ctx context.Context) {
	defer close(decoder.dispatchQueue)
	for {
		select {
		case <-ctx.Done():
			return
		case packet, ok := <-decoder.assemblyQueue:
			if !ok {
				return
			}
			if !decoder.takePacket(ctx, packet) {
				return
			}
		}
	}
}

func (decoder *Decoder) decode(ctx context.Context) {
	defer close(decoder.assemblyQueue)
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-decoder.argumentQueue:
			var packet = new(Packet)
			if !packet.Unmarshal(data) {
				log.Printf("decode: dropping invalid packet: %d bytes", len(data))
				continue
			}
			// the caller may reuse data
			packet.shard = bytes.Clone(packet.shard)
			select {
			case <-ctx.Done():
				return
			case decoder.assemblyQueue <- packet:
			}
		}
	}
}

// Decode starts the decoder. The in channel is never closed by the decoder,
// so writers may keep sending until ctx is done; out is closed once the
// decoder has stopped after ctx is done.
func (decoder *Decoder) Decode(ctx context.Context) (in chan<- []byte, out <-chan DecodedChunk, err error) {
	decoder.resultQueue = make(chan DecodedChunk, 64)
	decoder.dispatchQueue = make(chan dispatchItem, 64)
	decoder.assemblyQueue = make(chan *Packet, maxBlockShards)
	decoder.argumentQueue = make(chan []byte, maxBlockShards)

	go decoder.dispatch(ctx)
	go decoder.assembly(ctx)
	go decoder.decode(ctx)
	return decoder.argumentQueue, decoder.resultQueue, nil
}
