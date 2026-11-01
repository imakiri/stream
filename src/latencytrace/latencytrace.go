// Package latencytrace writes when each chunk and each FEC packet of the
// stream passes the stages of the server and the client, by the true time of
// their NTP clocks, so cmd/latency-report can lay the end-to-end delay out
// stage by stage.
//
// A trace file has one line per event, tab separated:
//
//	stage  session(hex)  csn  psn  unix_us
//
// psn is -1 for the events of a whole chunk. A data packet is named by its
// chunk's csn and its piece of the chunk; a parity packet by the csn of its
// block's first chunk and ParityPsnBase plus its index in the block. The
// stages are the Stage constants; their order is the order of the pipeline.
package latencytrace

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// Stage is a point of the pipeline a chunk or a packet passes.
type Stage string

const (
	// ServerInput is when the first byte of the chunk came from OBS.
	ServerInput Stage = "server.input"
	// ServerChunk is when the chunk was closed: full, or chunk_max_age after
	// its first byte.
	ServerChunk Stage = "server.chunk"
	// ServerShards is when the chunk was cut into its data shards; the
	// parity of its block is made when the block closes.
	ServerShards Stage = "server.shards"
	// ServerEncoderOut is when a packet left the encoder: a data packet as
	// soon as its chunk is cut, a parity packet when its block closes.
	ServerEncoderOut Stage = "server.encoder_out"
	// ServerSent is when a packet went to the relay, after pacing.
	ServerSent Stage = "server.sent"
	// ClientReceived is when a packet came from the relay and opened.
	ClientReceived Stage = "client.received"
	// ClientRestored is when FEC restored the chunk: enough of its packets
	// were in.
	ClientRestored Stage = "client.restored"
	// ClientOut is when the chunk went to the player, after the decoder put
	// the chunks back in order.
	ClientOut Stage = "client.out"
)

// Trace writes the events to a file. A nil Trace writes nothing, so callers
// need not check.
type Trace struct {
	clock func() time.Time

	mu   sync.Mutex
	file *os.File
	w    *bufio.Writer
	buf  []byte
}

// Create starts a trace file at path, stamped by clock.
func Create(path string, clock func() time.Time) (*Trace, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Trace{clock: clock, file: file, w: bufio.NewWriterSize(file, 1<<16)}, nil
}

// Chunk records that the chunk csn of session passed stage at the time at,
// or now if at is zero.
func (t *Trace) Chunk(stage Stage, session, csn uint64, at time.Time) {
	t.event(stage, session, csn, -1, at)
}

// ParityPsnBase is added to a parity packet's index in its block for its psn,
// so that it does not count as a piece of the block's first chunk.
const ParityPsnBase = 1000

// Packet records that a FEC packet passed stage now; packet is the encoded
// packet (codec.Packet), whose header and shard descriptor name its session,
// chunk and piece; see the package comment for parity packets.
func (t *Trace) Packet(stage Stage, packet []byte) {
	if t == nil || len(packet) < packetHeaderSize+shardDescriptorPrefix {
		return
	}
	// the codec.Packet layout; parsed here so the hot path makes no packet
	var session = binary.LittleEndian.Uint64(packet[8:16])
	var csn, psn uint64
	if packet[1] == packetKindParity {
		csn = binary.LittleEndian.Uint64(packet[16:24])
		psn = ParityPsnBase + uint64(binary.LittleEndian.Uint16(packet[2:4]))
	} else {
		csn = binary.LittleEndian.Uint64(packet[24:32])
		psn = uint64(packet[32])
	}
	t.event(stage, session, csn, int64(psn), time.Time{})
}

// codec.Packet: header size, parity kind, and the shard descriptor's csn and
// piece that follow the header in a data packet
const (
	packetHeaderSize      = 24
	packetKindParity      = 2
	shardDescriptorPrefix = 9
)

func (t *Trace) event(stage Stage, session, csn uint64, psn int64, at time.Time) {
	if t == nil {
		return
	}
	if at.IsZero() {
		at = t.clock()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.w == nil {
		return
	}
	var b = t.buf[:0]
	b = append(b, stage...)
	b = append(b, '\t')
	b = strconv.AppendUint(b, session, 16)
	b = append(b, '\t')
	b = strconv.AppendUint(b, csn, 10)
	b = append(b, '\t')
	b = strconv.AppendInt(b, psn, 10)
	b = append(b, '\t')
	b = strconv.AppendInt(b, at.UnixMicro(), 10)
	b = append(b, '\n')
	t.buf = b
	_, _ = t.w.Write(b)
}

// Close writes out what is buffered and closes the file.
func (t *Trace) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.w == nil {
		return nil
	}
	var err = t.w.Flush()
	if closeErr := t.file.Close(); err == nil {
		err = closeErr
	}
	t.w = nil
	if err != nil {
		return fmt.Errorf("latency trace: %w", err)
	}
	return nil
}
