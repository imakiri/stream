package wire

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
)

// StreamKey encrypts the stream for all clients at once. The server makes a
// new one on every start; Epoch tells clients which key a data packet needs.
type StreamKey struct {
	Epoch uint32
	Key   [32]byte // AES-256-GCM
}

func NewStreamKey() (StreamKey, error) {
	var k StreamKey
	var epoch [4]byte
	if _, err := rand.Read(epoch[:]); err != nil {
		return StreamKey{}, err
	}
	k.Epoch = binary.BigEndian.Uint32(epoch[:])
	if _, err := rand.Read(k.Key[:]); err != nil {
		return StreamKey{}, err
	}
	return k, nil
}

// DataHeaderSize is the clear header of a data packet, authenticated as
// additional data:
//
//	0      version
//	1      kind = KindData
//	2..6   stream key epoch, big endian
//	6..14  packet number, big endian: the AEAD nonce, unique per key
//
// followed by the sealed payload (one FEC packet) and the 16-byte tag.
const DataHeaderSize = 14

// DataOverhead is what sealing adds to a payload.
const DataOverhead = DataHeaderSize + 16

// StreamSealer turns stream payloads into data packets (server side).
type StreamSealer interface {
	SealData(payload []byte) []byte
}

// StreamOpener turns data packets back into payloads (client side).
type StreamOpener interface {
	// OpenData returns ErrUnknownEpoch for packets of another stream key.
	OpenData(packet []byte) ([]byte, error)
	Epoch() uint32
}

var (
	ErrUnknownEpoch      = errors.New("data packet of another stream key epoch")
	ErrInvalidDataPacket = errors.New("invalid data packet")
)

func newStreamAEAD(key StreamKey) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key.Key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// AESGCMStreamSealer numbers packets with a counter that serves as nonce.
type AESGCMStreamSealer struct {
	epoch  uint32
	aead   cipher.AEAD
	number atomic.Uint64
}

func NewStreamSealer(key StreamKey) (*AESGCMStreamSealer, error) {
	aead, err := newStreamAEAD(key)
	if err != nil {
		return nil, err
	}
	return &AESGCMStreamSealer{epoch: key.Epoch, aead: aead}, nil
}

func dataNonce(number uint64) []byte {
	var nonce = make([]byte, 12)
	binary.BigEndian.PutUint64(nonce[4:], number)
	return nonce
}

// SealData is safe for concurrent use.
func (s *AESGCMStreamSealer) SealData(payload []byte) []byte {
	var number = s.number.Add(1)
	var packet = make([]byte, DataHeaderSize, DataOverhead+len(payload))
	packet[0] = Version
	packet[1] = byte(KindData)
	binary.BigEndian.PutUint32(packet[2:6], s.epoch)
	binary.BigEndian.PutUint64(packet[6:14], number)
	return s.aead.Seal(packet, dataNonce(number), payload, packet[:DataHeaderSize])
}

type AESGCMStreamOpener struct {
	epoch uint32
	aead  cipher.AEAD
}

func NewStreamOpener(key StreamKey) (*AESGCMStreamOpener, error) {
	aead, err := newStreamAEAD(key)
	if err != nil {
		return nil, err
	}
	return &AESGCMStreamOpener{epoch: key.Epoch, aead: aead}, nil
}

func (o *AESGCMStreamOpener) Epoch() uint32 {
	return o.epoch
}

// DataPacketEpoch returns the stream key epoch of a data packet.
func DataPacketEpoch(packet []byte) (uint32, bool) {
	if len(packet) < DataOverhead || PacketKind(packet) != KindData {
		return 0, false
	}
	return binary.BigEndian.Uint32(packet[2:6]), true
}

func (o *AESGCMStreamOpener) OpenData(packet []byte) ([]byte, error) {
	epoch, ok := DataPacketEpoch(packet)
	if !ok {
		return nil, ErrInvalidDataPacket
	}
	if epoch != o.epoch {
		return nil, ErrUnknownEpoch
	}
	var number = binary.BigEndian.Uint64(packet[6:14])
	payload, err := o.aead.Open(nil, dataNonce(number), packet[DataHeaderSize:], packet[:DataHeaderSize])
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDataPacket, err)
	}
	return payload, nil
}
