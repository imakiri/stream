package codec

import (
	"crypto/sha1"
	"encoding/binary"
)

// A block is the unit of FEC: the data packets of up to block_chunks chunks
// in a row and the Reed-Solomon parity packets made over them. The data
// packets of a chunk go out as soon as the chunk is closed; the parity
// packets go when the block closes, after all its data packets. A chunk whose
// data packets all come is played at once; one with losses waits for the
// block to be restored.
//
// Every packet carries one shard of its block. A data shard is a shard
// descriptor (which chunk, which piece of it, how long the piece is) and up
// to PacketPayloadSize bytes of the chunk; a parity shard is ShardSize bytes.
// For the Reed-Solomon code every data shard counts as ShardSize long, the
// rest zeros: a data packet sends only the bytes its piece has, the receiver
// pads it back. The descriptor is inside the shard, so a restored data shard
// tells which chunk it belongs to.
//
// Packet layout, little endian:
//
//	0       version (PacketVersion)
//	1       kind: PacketKindData or PacketKindParity
//	2..4    index of the shard in the block: data 0..k-1, parity k..k+m-1
//	4..6    k, the block's data packets: set in parity packets only, 0 in data
//	        packets (they go before the block is closed)
//	6..8    m, the block's parity packets: as k
//	8..16   session: random per encoder run, so the decoder notices a restarted sender
//	16..24  block: the csn of the block's first chunk
//	24..    shard: data: descriptor (ShardDescriptorSize) and the piece; parity: ShardSize bytes
//	last 8  hash of everything before it (PacketHashSize)
//
// Shard descriptor, at the start of a data shard:
//
//	0..8    csn of the chunk
//	8       piece: which piece of the chunk this is, from 0
//	9       pieces: how many pieces the chunk has
//	10..12  length of the piece in bytes
const (
	PacketVersion       = 3
	PacketKindData      = 1
	PacketKindParity    = 2
	PacketHeaderSize    = 24
	PacketHashSize      = 8
	ShardDescriptorSize = 12

	// PacketMaxSize is the largest packet: it must fit, with what the
	// stream seal adds (wire.DataOverhead, 30 bytes), into the UDP payload
	// of an AmneziaWG tunnel of MTU 1420 (1392 bytes), so that no packet
	// is fragmented. src/codec_size_test.go checks it.
	PacketMaxSize = 1362
	// PacketPayloadSize is the most bytes of a chunk one packet carries.
	PacketPayloadSize = PacketMaxSize - PacketHeaderSize - PacketHashSize - ShardDescriptorSize
	// ShardSize is the length of every shard in the Reed-Solomon code.
	ShardSize = ShardDescriptorSize + PacketPayloadSize
	// maxBlockShards is the most shards the code takes (GF(2^8)).
	maxBlockShards = 256
)

// Packet is one shard of a block, see the layout above.
type Packet struct {
	kind    uint8
	index   uint16
	k, m    uint16
	session uint64
	block   uint64
	// shard: a data shard is as long as its descriptor and piece, a parity
	// shard is ShardSize
	shard []byte
}

// shardDescriptor is the start of a data shard.
type shardDescriptor struct {
	csn    uint64
	piece  uint8
	pieces uint8
	length uint16
}

func (d shardDescriptor) put(shard []byte) {
	binary.LittleEndian.PutUint64(shard[0:8], d.csn)
	shard[8] = d.piece
	shard[9] = d.pieces
	binary.LittleEndian.PutUint16(shard[10:12], d.length)
}

// readShardDescriptor parses the descriptor at the start of a data shard;
// false if the shard is too short for it or for the length it gives.
func readShardDescriptor(shard []byte) (shardDescriptor, bool) {
	if len(shard) < ShardDescriptorSize {
		return shardDescriptor{}, false
	}
	var d = shardDescriptor{
		csn:    binary.LittleEndian.Uint64(shard[0:8]),
		piece:  shard[8],
		pieces: shard[9],
		length: binary.LittleEndian.Uint16(shard[10:12]),
	}
	if d.pieces == 0 || d.piece >= d.pieces || int(d.length) > len(shard)-ShardDescriptorSize || d.csn == 0 {
		return shardDescriptor{}, false
	}
	return d, true
}

// piece returns the chunk bytes of a data shard whose descriptor is d.
func (d shardDescriptor) pieceOf(shard []byte) []byte {
	return shard[ShardDescriptorSize : ShardDescriptorSize+int(d.length)]
}

func hash(data []byte) [PacketHashSize]byte {
	var h = sha1.Sum(data)
	return [PacketHashSize]byte(h[:PacketHashSize])
}

// Marshal returns the packet as it goes on the wire.
func (p *Packet) Marshal() []byte {
	var b = make([]byte, PacketHeaderSize+len(p.shard)+PacketHashSize)
	b[0] = PacketVersion
	b[1] = p.kind
	binary.LittleEndian.PutUint16(b[2:4], p.index)
	binary.LittleEndian.PutUint16(b[4:6], p.k)
	binary.LittleEndian.PutUint16(b[6:8], p.m)
	binary.LittleEndian.PutUint64(b[8:16], p.session)
	binary.LittleEndian.PutUint64(b[16:24], p.block)
	copy(b[PacketHeaderSize:], p.shard)
	var h = hash(b[:len(b)-PacketHashSize])
	copy(b[len(b)-PacketHashSize:], h[:])
	return b
}

// Unmarshal parses a packet from the wire; false if it is not a packet of
// this version or its hash does not match.
func (p *Packet) Unmarshal(b []byte) bool {
	if len(b) < PacketHeaderSize+PacketHashSize || len(b) > PacketMaxSize || b[0] != PacketVersion {
		return false
	}
	if [PacketHashSize]byte(b[len(b)-PacketHashSize:]) != hash(b[:len(b)-PacketHashSize]) {
		return false
	}
	p.kind = b[1]
	p.index = binary.LittleEndian.Uint16(b[2:4])
	p.k = binary.LittleEndian.Uint16(b[4:6])
	p.m = binary.LittleEndian.Uint16(b[6:8])
	p.session = binary.LittleEndian.Uint64(b[8:16])
	p.block = binary.LittleEndian.Uint64(b[16:24])
	p.shard = b[PacketHeaderSize : len(b)-PacketHashSize]
	switch p.kind {
	case PacketKindData:
		_, ok := readShardDescriptor(p.shard)
		return ok && p.index < maxBlockShards
	case PacketKindParity:
		return len(p.shard) == ShardSize && p.k > 0 && p.m > 0 && int(p.k)+int(p.m) <= maxBlockShards &&
			p.index >= p.k && p.index < p.k+p.m
	}
	return false
}
