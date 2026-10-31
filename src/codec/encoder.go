package codec

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/go-faster/errors"
	"github.com/klauspost/reedsolomon"
	"log"
	"sync"
	"time"
)

type Encoder struct {
	aggregator struct {
		mu      *sync.Mutex
		timeout time.Duration
		timer   *time.Timer
		buf     []byte
	}
	encoder struct {
		rs      reedsolomon.Encoder
		total   uint64
		data    uint64
		csn     uint64
		session uint64 // random per Encode call, see PacketV2.session
	}
	dispatcher struct {
		length uint64
		chunks uint64
		buf    []*PacketV2
	}

	aggregateQueue  chan []byte
	encodeQueue     chan []byte
	dispatcherQueue chan *PacketV2
	resultQueue     chan []byte
}

func NewEncoder(aggrtTimeout time.Duration, dispatcherSize, dataParts, totalParts uint64) (*Encoder, error) {
	if dataParts == 0 {
		return nil, errors.New("dataParts cannot be zero")
	}
	if totalParts < 2 {
		return nil, errors.New("totalParts cannot be less than 2")
	}

	var encoder = new(Encoder)
	encoder.encoder.total = totalParts
	encoder.encoder.data = dataParts

	var err error
	encoder.encoder.rs, err = reedsolomon.New(int(dataParts), int(totalParts)-int(dataParts))
	if err != nil {
		return nil, err
	}

	encoder.aggregator.mu = new(sync.Mutex)
	encoder.aggregator.buf = make([]byte, 0, encoder.IncomingSize())
	encoder.aggregator.timeout = aggrtTimeout
	encoder.aggregator.timer = time.NewTimer(100000000 * time.Second)

	encoder.dispatcher.chunks = dispatcherSize
	encoder.dispatcher.buf = make([]*PacketV2, totalParts*dispatcherSize)

	return encoder, nil
}

func (encoder *Encoder) IncomingSize() uint64 {
	return PacketV2DataSize*encoder.encoder.data - ChunkHeaderSize
}

func (encoder *Encoder) OutgoingSize() uint64 {
	return PacketV2Size
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
	case encoder.encodeQueue <- agData:
		return
	}
}

func (encoder *Encoder) dispatch(ctx context.Context) {
	defer close(encoder.resultQueue)
	var packet *PacketV2
	var ok bool
	var chunks = encoder.dispatcher.chunks
	var perChunk = encoder.encoder.total
	var size = chunks * perChunk
	//dispatch:
	for {
		select {
		case <-ctx.Done():
			return
		case packet, ok = <-encoder.dispatcherQueue:
			if !ok {
				return
			}
			var at = ((chunks * encoder.dispatcher.length) + (encoder.dispatcher.length / perChunk)) % size
			encoder.dispatcher.buf[at] = packet
			encoder.dispatcher.length++

			if encoder.dispatcher.length == size {
				for i := range encoder.dispatcher.buf {
					if encoder.dispatcher.buf[i] == nil {
						continue
					}
					select {
					case <-ctx.Done():
						return
					case encoder.resultQueue <- encoder.dispatcher.buf[i].Marshal():
						encoder.dispatcher.buf[i] = nil
					}
				}
				encoder.dispatcher.length = 0
			}
		}
	}
}

func (encoder *Encoder) encode(ctx context.Context) {
	defer close(encoder.dispatcherQueue)
	encoder.encoder.csn = 1
	defer func() { encoder.encoder.csn = 0 }()
	var err error
	var data []byte
	var packet *PacketV2
	var ok bool
encode:
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok = <-encoder.encodeQueue:
			if !ok {
				return
			}
			var chunk, rem = NewChunk(encoder.encoder.data, encoder.encoder.csn, KindData, data)
			if rem != nil {
				log.Printf("encode: NewChunk: res is not nil: len %d", len(rem))
				continue
			}
			var data = chunk.Marshal(encoder.encoder.total)

			err = encoder.encoder.rs.Encode(data)
			if err != nil {
				fmt.Println(errors.Wrap(err, "encode: rs.Encode"))
				continue encode
			}

			for i := range data {
				packet, rem = NewPacketV2(KindData, encoder.encoder.csn, uint32(i), encoder.encoder.session, data[i])
				if rem != nil {
					log.Printf("encode: NewPacketV2: res is not nil: len %d", len(rem))
					continue
				}

				select {
				case <-ctx.Done():
					return
				case encoder.dispatcherQueue <- packet:
				}
			}
			encoder.encoder.csn++
		}
	}
}

func (encoder *Encoder) aggregate(ctx context.Context) {
	defer close(encoder.encodeQueue)
	var data []byte
	var sep int
	//aggregate:
	for {
		select {
		case <-ctx.Done():
			return
		case <-encoder.aggregator.timer.C:
			encoder.aggregatorFlush(ctx)
		case data = <-encoder.aggregateQueue:
			for len(data) > 0 {
				sep = min(cap(encoder.aggregator.buf)-len(encoder.aggregator.buf), len(data))
				encoder.aggregator.buf = append(encoder.aggregator.buf, data[:sep]...)
				data = data[sep:]
				if len(encoder.aggregator.buf) == cap(encoder.aggregator.buf) {
					encoder.aggregatorFlush(ctx)
				}
			}
			encoder.aggregator.timer.Reset(encoder.aggregator.timeout)
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
	encoder.encoder.session, err = newSession()
	if err != nil {
		return nil, nil, errors.Wrap(err, "newSession")
	}
	encoder.resultQueue = make(chan []byte, 8*encoder.dispatcher.chunks)
	encoder.dispatcherQueue = make(chan *PacketV2, 8*encoder.dispatcher.chunks)
	encoder.encodeQueue = make(chan []byte, 64*encoder.encoder.data)
	encoder.aggregateQueue = make(chan []byte, 64*encoder.encoder.data)

	go encoder.dispatch(ctx)
	go encoder.aggregate(ctx)
	go encoder.encode(ctx)
	return encoder.aggregateQueue, encoder.resultQueue, nil
}
