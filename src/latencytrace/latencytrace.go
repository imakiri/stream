// Package latencytrace writes when each chunk and each FEC packet of the
// stream passes the stages of the server and the client, by the true time of
// their NTP clocks, so cmd/latency-report can lay the end-to-end delay out
// stage by stage.
//
// A trace file has one line per event, tab separated:
//
//	stage  session(hex)  csn  psn  unix_us
//
// psn is -1 for the events of a whole chunk. The stages are the Stage
// constants; their order is the order of the pipeline.
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
	// ServerChunk is when the chunk was closed: full, or no input for the
	// encoder's dispatcher timeout.
	ServerChunk Stage = "server.chunk"
	// ServerFEC is when the parity packets of the chunk were made.
	ServerFEC Stage = "server.fec"
	// ServerDispatch is when a packet left the interleaver, which holds the
	// packets until the chunks it mixes are all in.
	ServerDispatch Stage = "server.dispatch"
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

// Packet records that a FEC packet passed stage now; packet is the encoded
// packet (codec.PacketV2), whose header names its session, chunk and index.
func (t *Trace) Packet(stage Stage, packet []byte) {
	if t == nil || len(packet) < packetHeaderSize {
		return
	}
	// the codec.PacketV2 header; parsed here so the hot path makes no packet
	var psn = binary.LittleEndian.Uint32(packet[4:8])
	var csn = binary.LittleEndian.Uint64(packet[8:16])
	var session = binary.LittleEndian.Uint64(packet[16:24])
	t.event(stage, session, csn, int64(psn), time.Time{})
}

const packetHeaderSize = 24

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
