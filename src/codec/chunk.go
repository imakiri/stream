package codec

import (
	"encoding/binary"
	"github.com/klauspost/reedsolomon"
)

const ChunkHeaderSize = 22

type Chunk struct {
	parts   uint64
	session uint64 // not on the wire: taken from the packets the chunk was restored from

	version uint8  // 0...1
	kind    uint8  // 1...2
	_       uint32 // 2...6
	csn     uint64 // 6...14
	size    uint64 // 14...22
	data    []byte
}

func NewChunk(parts, csn uint64, kind uint8, data []byte) (*Chunk, []byte) {
	var size = min(uint64(len(data)), parts*PacketV2DataSize)
	var chunk = &Chunk{
		parts: parts,

		version: 1,
		kind:    kind,
		csn:     csn,
		size:    size,
		data:    data[:size],
	}
	data = data[size:]
	// The Good, the Bad and the Ugly
	if len(data) == 0 {
		return chunk, nil
	}
	return chunk, data
}

// Data returns copy of internal data field
func (c *Chunk) Data() []byte {
	var data = make([]byte, c.size)
	copy(data, c.data)
	return data
}

func (c *Chunk) Parts() uint64 {
	return c.parts
}

// Len returns length of the chunk in bytes. It'll be multiples of PacketV2DataSize
func (c *Chunk) Len() uint64 {
	return c.parts * PacketV2DataSize
}

// Marshal accepts total number of parts for allocation purposes. Values less than number of chunk's parts are ignored
func (c *Chunk) Marshal(total uint64) [][]byte {
	var dst = reedsolomon.AllocAligned(int(max(total, c.parts)), PacketV2DataSize)
	var i uint64 = 0

	dst[i][0] = c.version
	dst[i][1] = c.kind
	//binary.LittleEndian.PutUint32(dst[i][2:6], c.id)
	binary.LittleEndian.PutUint64(dst[i][6:14], c.csn)
	binary.LittleEndian.PutUint64(dst[i][14:22], uint64(len(c.data)))

	var copied = copy(dst[i][ChunkHeaderSize:], c.data)
	for i = 1; uint64(copied) < c.size; i++ {
		copied += copy(dst[i], c.data[copied:])
	}
	return dst
}

func (c *Chunk) Unmarshal(data [][]byte) bool {
	if len(data) < 1 {
		return false
	}
	c.parts = uint64(len(data))

	var i uint64 = 0

	c.version = data[i][0]
	c.kind = data[i][1]
	//c.id = binary.LittleEndian.Uint32(data[i][2:6])
	c.csn = binary.LittleEndian.Uint64(data[i][6:14])
	c.size = binary.LittleEndian.Uint64(data[i][14:22])
	// size comes from restored bytes; a chunk mixed from foreign packets would
	// make it garbage, so bound it by what the parts can hold before allocating.
	var capacity uint64
	for i := range data {
		capacity += uint64(len(data[i]))
	}
	if len(data[0]) < ChunkHeaderSize || c.size > capacity-ChunkHeaderSize {
		return false
	}
	c.data = make([]byte, c.size)

	var copied = copy(c.data, data[i][ChunkHeaderSize:])
	for i = 1; uint64(copied) < c.size; i++ {
		if i >= c.parts {
			return false
		}
		copied += copy(c.data[copied:], data[i])
	}
	return c.size == uint64(copied)
}
