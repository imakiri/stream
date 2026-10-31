package codec

import (
	"crypto/sha1"
	"encoding/binary"
)

const PacketV2Size = PacketV2HeaderSize + PacketV2DataSize + PacketV2HashSize
const PacketV2HeaderSize = 24
const PacketV2DataSize = 1024
const PacketV2HashSize = 8

type PacketV2 struct {
	version uint8                  // 0...1
	kind    uint8                  // 1...2
	_       uint16                 // 2...4
	psn     uint32                 // 4...8  packet sequence number
	csn     uint64                 // 8...16 chunk sequence number
	session uint64                 // 16...24 random per encoder run, so the decoder notices a restarted sender
	data    []byte                 // 24...1048
	hash    [PacketV2HashSize]byte // 1048...1056
}

func NewPacketV2(kind uint8, csn uint64, psn uint32, session uint64, data []byte) (*PacketV2, []byte) {
	var packet = &PacketV2{
		version: 2,
		kind:    kind,
		psn:     psn,
		csn:     csn,
		session: session,
		data:    make([]byte, PacketV2DataSize),
	}
	var copied = copy(packet.data, data)
	data = data[copied:]
	// The Good, the Bad and the Ugly
	if len(data) == 0 {
		return packet, nil
	}
	return packet, data
}

func hash(data []byte) [PacketV2HashSize]byte {
	var h = sha1.Sum(data)
	return [8]byte{h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]}
}

func (p *PacketV2) Data() []byte {
	if p == nil {
		return nil
	}
	return p.data
}

func (p *PacketV2) Marshal() []byte {
	var b = make([]byte, PacketV2Size)
	b[0] = p.version
	b[1] = p.kind
	binary.LittleEndian.PutUint32(b[4:8], p.psn)
	binary.LittleEndian.PutUint64(b[8:16], p.csn)
	binary.LittleEndian.PutUint64(b[16:24], p.session)
	copy(b[PacketV2HeaderSize:PacketV2HeaderSize+PacketV2DataSize], p.data[:])
	var h = hash(b[:PacketV2HeaderSize+PacketV2DataSize])
	copy(b[PacketV2HeaderSize+PacketV2DataSize:PacketV2Size], h[:])
	return b
}

func (p *PacketV2) Unmarshal(data []byte) bool {
	if len(data) != PacketV2Size {
		return false
	}
	p.version = data[0]
	p.kind = data[1]
	p.psn = binary.LittleEndian.Uint32(data[4:8])
	p.csn = binary.LittleEndian.Uint64(data[8:16])
	p.session = binary.LittleEndian.Uint64(data[16:24])
	p.data = make([]byte, PacketV2DataSize)
	copy(p.data[:], data[PacketV2HeaderSize:PacketV2HeaderSize+PacketV2DataSize])
	copy(p.hash[:], data[PacketV2HeaderSize+PacketV2DataSize:PacketV2Size])
	return p.hash == hash(data[:PacketV2HeaderSize+PacketV2DataSize])
}
