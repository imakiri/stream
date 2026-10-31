package codec

import (
	"context"
	"github.com/go-faster/errors"
	"github.com/klauspost/reedsolomon"
	"log"
	"slices"
	"sync/atomic"
)

// assemblyLine collects the packets of one chunk. Lines form a ring indexed by
// csn % len(lines); a line is reused when a packet of a newer csn lands on it.
type assemblyLine struct {
	csn   uint64
	found uint64
	// restored is set once the line had enough packets and went to the
	// restorer; the remaining parity packets of this csn are then surplus.
	restored bool
	packets  []*PacketV2
}

func (line *assemblyLine) reuse(csn uint64) {
	line.csn = csn
	line.found = 0
	line.restored = false
	clear(line.packets)
}

type Decoder struct {
	assembler struct {
		lines []assemblyLine
		// maxCsn is the newest csn seen in the session; packets older than
		// the ring (csn + len(lines) <= maxCsn) can no longer have a line.
		maxCsn uint64
		// session is the encoder run the packets are taken from; packets of
		// previousSession are late leftovers of a restarted sender.
		session         uint64
		previousSession uint64

		ok  *uint64
		err *uint64
	}
	restorer struct {
		rs    reedsolomon.Encoder
		total uint64
		data  uint64

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

		ok  *uint64
		err *uint64
	}

	argumentQueue chan []byte
	assemblyQueue chan *PacketV2
	restoreQueue  chan []*PacketV2
	dispatchQueue chan *Chunk
	resultQueue   chan []byte
}

func NewDecoder(dataParts, totalParts, assemblerLines, dispatcherSize uint64) (decoder *Decoder, err error) {
	if dataParts == 0 {
		return nil, errors.New("dataParts cannot be zero")
	}
	if totalParts < 2 {
		return nil, errors.New("totalParts cannot be less than 2")
	}
	if assemblerLines == 0 {
		return nil, errors.New("assemblerLines cannot be zero")
	}

	decoder = new(Decoder)
	decoder.dispatcher.size = dispatcherSize
	decoder.dispatcher.awaiting = 1
	decoder.dispatcher.chunks = make([]*Chunk, decoder.dispatcher.size)
	decoder.dispatcher.ok = new(uint64)
	decoder.dispatcher.err = new(uint64)

	decoder.restorer.rs, err = reedsolomon.New(int(dataParts), int(totalParts)-int(dataParts))
	if err != nil {
		return nil, err
	}
	decoder.restorer.data = dataParts
	decoder.restorer.total = totalParts
	decoder.restorer.ok = new(uint64)
	decoder.restorer.err = new(uint64)

	decoder.assembler.lines = make([]assemblyLine, assemblerLines)
	for i := range decoder.assembler.lines {
		decoder.assembler.lines[i].packets = make([]*PacketV2, totalParts)
	}
	decoder.assembler.ok = new(uint64)
	decoder.assembler.err = new(uint64)

	return decoder, nil
}

// resetDispatcher starts ordering chunks of a new session. Interleaved chunks
// complete out of order, so the first chunk seen is not necessarily the first
// one sent: if it is within the reorder window from the start, the encoder has
// just started and csn 1 is awaited; otherwise the decoder joined a running
// stream and starts from this chunk, losing at most the few before it.
func (decoder *Decoder) resetDispatcher(chunk *Chunk) {
	var awaiting uint64 = 1
	if chunk.csn > decoder.dispatcher.size {
		awaiting = chunk.csn
	}
	log.Printf("dispatch: new session %x awaiting csn %d", chunk.session, awaiting)
	decoder.dispatcher.session = chunk.session
	decoder.dispatcher.awaiting = awaiting
	decoder.dispatcher.first = awaiting - 1
	decoder.dispatcher.last = awaiting - 1
	clear(decoder.dispatcher.chunks)
}

func (decoder *Decoder) dispatch(ctx context.Context) {
	defer close(decoder.resultQueue)
	var chunk *Chunk
	var ok bool
dispatch:
	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok = <-decoder.dispatchQueue:
			if !ok {
				return
			}
			if chunk == nil {
				log.Println("dispatch: dropping invalid chunk: nil chunk")
				atomic.AddUint64(decoder.dispatcher.err, 1)
				continue dispatch
			}

			if chunk.csn == 0 {
				log.Println("dispatch: dropping invalid chunk: csn == 0")
				atomic.AddUint64(decoder.dispatcher.err, 1)
				continue dispatch
			}

			if chunk.session != decoder.dispatcher.session {
				decoder.resetDispatcher(chunk)
			}

			if chunk.csn <= decoder.dispatcher.last {
				log.Printf("dispatch: detached chunk: csn: %d", chunk.csn)
				atomic.AddUint64(decoder.dispatcher.err, 1)
				continue dispatch
			}

			decoder.dispatcher.first = max(decoder.dispatcher.first, chunk.csn)

			if chunk.csn != decoder.dispatcher.awaiting {
				var at = chunk.csn % decoder.dispatcher.size
				if decoder.dispatcher.chunks[at] != nil {
					log.Printf("dispatch: lost: csn: %d", decoder.dispatcher.awaiting)
					atomic.AddUint64(decoder.dispatcher.err, 1)
					select {
					case <-ctx.Done():
						return
					case decoder.resultQueue <- decoder.dispatcher.chunks[at].Data():
						decoder.dispatcher.last = decoder.dispatcher.chunks[at].csn
						decoder.dispatcher.awaiting = decoder.dispatcher.chunks[at].csn + 1
					}
					decoder.dispatcher.chunks[at] = chunk
				} else {
					decoder.dispatcher.chunks[at] = chunk
					atomic.AddUint64(decoder.dispatcher.ok, 1)
					continue dispatch
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case decoder.resultQueue <- chunk.Data():
					decoder.dispatcher.last = chunk.csn
					decoder.dispatcher.awaiting++
				}
			}

			var length = decoder.dispatcher.first - decoder.dispatcher.last
			if length == 0 {
				atomic.AddUint64(decoder.dispatcher.ok, 1)
				continue dispatch
			}

			for i := decoder.dispatcher.last; i < decoder.dispatcher.first; i++ {
				var at = (i + 1) % decoder.dispatcher.size
				if decoder.dispatcher.chunks[at] == nil {
					atomic.AddUint64(decoder.dispatcher.ok, 1)
					continue dispatch
				}
				if decoder.dispatcher.chunks[at].csn != decoder.dispatcher.awaiting {
					atomic.AddUint64(decoder.dispatcher.ok, 1)
					continue dispatch
				}

				select {
				case <-ctx.Done():
					return
				case decoder.resultQueue <- decoder.dispatcher.chunks[at].Data():
					decoder.dispatcher.last = decoder.dispatcher.chunks[at].csn
					decoder.dispatcher.awaiting++
					decoder.dispatcher.chunks[at] = nil
				}
			}
			atomic.AddUint64(decoder.dispatcher.ok, 1)
		}
	}
}

func (decoder *Decoder) restore(ctx context.Context) {
	defer close(decoder.dispatchQueue)
	var packets []*PacketV2
	var data = make([][]byte, decoder.restorer.total)
	var chunk *Chunk
	var session uint64
	var err error
	var ok bool
restore:
	for {
		select {
		case <-ctx.Done():
			return
		case packets, ok = <-decoder.restoreQueue:
			if !ok {
				return
			}
			for i := range data {
				data[i] = packets[i].Data()
				if packets[i] != nil {
					session = packets[i].session
				}
			}

			err = decoder.restorer.rs.ReconstructData(data)
			if err != nil {
				log.Printf("restore: rs.ReconstructData(data): err %v", err)
				atomic.AddUint64(decoder.restorer.err, 1)
				continue restore
			}

			chunk = new(Chunk)
			ok = chunk.Unmarshal(data[:decoder.restorer.data])
			if !ok {
				log.Println("restore: dropping invalid packet: chunk.Unmarshal: not ok")
				atomic.AddUint64(decoder.restorer.err, 1)
				continue restore
			}
			chunk.session = session

			select {
			case <-ctx.Done():
				return
			case decoder.dispatchQueue <- chunk:
				atomic.AddUint64(decoder.restorer.ok, 1)
				continue restore
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
	decoder.assembler.maxCsn = 0
	for i := range decoder.assembler.lines {
		decoder.assembler.lines[i].reuse(0)
	}
}

func (decoder *Decoder) assembly(ctx context.Context) {
	defer close(decoder.restoreQueue)
	var lines = uint64(len(decoder.assembler.lines))
	var packet *PacketV2
	var ok bool
assembly:
	for {
		select {
		case <-ctx.Done():
			return
		case packet, ok = <-decoder.assemblyQueue:
			if !ok {
				return
			}
			if packet == nil {
				log.Println("assembly: dropping invalid packet: packet is nil")
				atomic.AddUint64(decoder.assembler.err, 1)
				continue assembly
			}
			if uint64(packet.psn) >= decoder.restorer.total {
				log.Printf("assembly: invalid psn: %d", packet.psn)
				atomic.AddUint64(decoder.assembler.err, 1)
				continue assembly
			}
			if packet.csn == 0 {
				log.Println("assembly: dropping invalid packet: csn == 0")
				atomic.AddUint64(decoder.assembler.err, 1)
				continue assembly
			}

			if packet.session != decoder.assembler.session {
				if decoder.assembler.session != 0 && packet.session == decoder.assembler.previousSession {
					// late packet of the sender run before the restart
					continue assembly
				}
				decoder.startSession(packet.session)
			}

			if packet.csn+lines <= decoder.assembler.maxCsn {
				// its line has been reused by a newer chunk already
				atomic.AddUint64(decoder.assembler.err, 1)
				continue assembly
			}
			decoder.assembler.maxCsn = max(decoder.assembler.maxCsn, packet.csn)

			var line = &decoder.assembler.lines[packet.csn%lines]
			switch {
			case line.csn == packet.csn:
			case line.csn < packet.csn:
				if line.found != 0 && !line.restored {
					log.Printf("assembly: dropping chunk: csn: %d with %d of %d packets -> %d",
						line.csn, line.found, decoder.restorer.data, packet.csn)
					atomic.AddUint64(decoder.assembler.err, 1)
				}
				line.reuse(packet.csn)
			default:
				// an older chunk than the one holding the line
				atomic.AddUint64(decoder.assembler.err, 1)
				continue assembly
			}

			if line.restored {
				// surplus parity of a chunk that is already restored
				continue assembly
			}
			if line.packets[packet.psn] != nil {
				// duplicate datagram
				continue assembly
			}
			line.packets[packet.psn] = packet
			line.found++

			if line.found == decoder.restorer.data {
				select {
				case <-ctx.Done():
					return
				case decoder.restoreQueue <- slices.Clone(line.packets):
					line.restored = true
					clear(line.packets)
				}
			}
			atomic.AddUint64(decoder.assembler.ok, 1)
		}
	}
}

func (decoder *Decoder) decode(ctx context.Context) {
	defer close(decoder.assemblyQueue)
	var data []byte
	var packet *PacketV2
	var ok bool
decode:
	for {
		select {
		case <-ctx.Done():
			return
		case data = <-decoder.argumentQueue:
			if len(data) != PacketV2Size {
				log.Printf("decode: invalid data: %d bytes", len(data))
				continue decode
			}

			packet = new(PacketV2)
			ok = packet.Unmarshal(data)
			if !ok {
				log.Println("decode: packet.Unmarshal is not ok")
				continue decode
			}

			if packet.kind != KindData {
				log.Printf("decode: packet: invalid kind: %d", packet.kind)
				continue decode
			}

			select {
			case <-ctx.Done():
				return
			case decoder.assemblyQueue <- packet:
				continue decode
			}
		}
	}
}

func (decoder *Decoder) IncomingSize() uint64 {
	return PacketV2Size
}

func (decoder *Decoder) OutgoingSize() uint64 {
	return PacketV2DataSize*decoder.restorer.data - ChunkHeaderSize
}

// Decode starts the decoder. The in channel is never closed by the decoder,
// so writers may keep sending until ctx is done; out is closed once the
// decoder has stopped after ctx is done.
func (decoder *Decoder) Decode(ctx context.Context) (in chan<- []byte, out <-chan []byte, err error) {
	decoder.resultQueue = make(chan []byte, 16*decoder.restorer.data)
	decoder.dispatchQueue = make(chan *Chunk, 16)
	decoder.restoreQueue = make(chan []*PacketV2, 16*decoder.restorer.data)
	decoder.assemblyQueue = make(chan *PacketV2, 16*decoder.restorer.data)
	decoder.argumentQueue = make(chan []byte, 16*decoder.restorer.data)

	go decoder.dispatch(ctx)
	go decoder.restore(ctx)
	go decoder.assembly(ctx)
	go decoder.decode(ctx)
	return decoder.argumentQueue, decoder.resultQueue, nil
}
