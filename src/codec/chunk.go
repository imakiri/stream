package codec

import (
	"encoding/binary"
	"time"
)

// ChunkHeaderSize is the header Chunk.Marshal puts before the chunk's data.
const ChunkHeaderSize = 22

// Chunk is a run of the input stream that the encoder closed at once: full
// (chunk_packets packets of data) or max age after its first byte. On the
// wire it is its header and data cut into pieces of at most
// PacketPayloadSize, one per data packet, see packet.go.
type Chunk struct {
	session uint64 // not on the wire: taken from the packets the chunk was restored from

	version uint8 // 0...1
	kind    uint8 // 1...2
	// inputUnixMilli is when the first byte of the chunk reached the
	// encoder, by the sender's clock: the low 32 bits of the unix time in
	// milliseconds, so it wraps every 49 days; compare with InputDelay.
	inputUnixMilli uint32 // 2...6
	csn            uint64 // 6...14
	size           uint64 // 14...22
	data           []byte
}

// InputDelay returns how long ago, at now by the sender's clock, a chunk with
// this input time reached the encoder.
func InputDelay(inputUnixMilli uint32, now time.Time) time.Duration {
	return time.Duration(int32(uint32(now.UnixMilli())-inputUnixMilli)) * time.Millisecond
}

// NewChunk makes chunk csn of data, which it keeps (does not copy).
func NewChunk(csn uint64, kind uint8, data []byte) *Chunk {
	return &Chunk{version: 1, kind: kind, csn: csn, size: uint64(len(data)), data: data}
}

// Data returns copy of internal data field
func (c *Chunk) Data() []byte {
	var data = make([]byte, c.size)
	copy(data, c.data)
	return data
}

// Marshal returns the chunk's header and data, as its pieces carry them.
func (c *Chunk) Marshal() []byte {
	var b = make([]byte, ChunkHeaderSize+len(c.data))
	b[0] = c.version
	b[1] = c.kind
	binary.LittleEndian.PutUint32(b[2:6], c.inputUnixMilli)
	binary.LittleEndian.PutUint64(b[6:14], c.csn)
	binary.LittleEndian.PutUint64(b[14:22], uint64(len(c.data)))
	copy(b[ChunkHeaderSize:], c.data)
	return b
}

// Unmarshal parses what Marshal made (the chunk's pieces joined); false if
// the size in the header is not what follows it.
func (c *Chunk) Unmarshal(b []byte) bool {
	if len(b) < ChunkHeaderSize {
		return false
	}
	c.version = b[0]
	c.kind = b[1]
	c.inputUnixMilli = binary.LittleEndian.Uint32(b[2:6])
	c.csn = binary.LittleEndian.Uint64(b[6:14])
	c.size = binary.LittleEndian.Uint64(b[14:22])
	if c.size != uint64(len(b)-ChunkHeaderSize) {
		return false
	}
	c.data = b[ChunkHeaderSize:]
	return true
}
