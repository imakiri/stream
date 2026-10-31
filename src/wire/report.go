package wire

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"
)

// A client report tells the server how the stream reaches one client: the
// end-to-end delay and the lost chunks over the last few seconds. Each
// client sends one every few seconds, client -> relay -> server, so the
// server knows who watches and how well.
//
// It is sealed with a report key of its own per handshake, which only that
// client and the server know: the relay cannot read it, and another client,
// which has the shared stream key, cannot forge it.
//
// report key = HKDF-SHA256(X25519 shared secret of the handshake, salt =
// SHA-256(client hello || server hello 0..174), info = reportKeyInfo)
//
// The report key ID is the first 8 bytes of the server's ephemeral key in the
// server hello. A client that handshakes while its stream does not work sends
// the same hello several times and the server answers each with another
// ephemeral key; the ID tells the server which of its answers the client
// took.
//
// Client report, ClientReportSize bytes:
//
//	0       version
//	1       kind = KindClientReport
//	2..18   client ID: the relay checks it is the sender
//	18..26  report key ID
//	26..34  report number, big endian: the AEAD nonce, increasing per key
//	34..58  ClientReport, sealed with the report key
//	58..74  AEAD tag
//
// Bytes 0..34 are authenticated as additional data.
const ClientReportSize = clientReportHeaderSize + clientReportPayloadSize + 16

const (
	clientReportHeaderSize  = 34
	clientReportPayloadSize = 24
	reportKeyIDSize         = 8
	reportKeyInfo           = "imakiri/stream wire v3 client report"
)

// ClientReport is what a client tells the server about the stream.
type ClientReport struct {
	DelayP50, DelayP99, DelayMax time.Duration // end-to-end, from the server's input to the client's output
	Chunks                       uint32        // chunks the delays are over
	Lost                         uint32        // chunks lost
	Incomplete                   uint32        // of the lost, those that came short of packets
}

func (r ClientReport) marshal() []byte {
	var b = make([]byte, clientReportPayloadSize)
	binary.BigEndian.PutUint32(b[0:4], durationMicros(r.DelayP50))
	binary.BigEndian.PutUint32(b[4:8], durationMicros(r.DelayP99))
	binary.BigEndian.PutUint32(b[8:12], durationMicros(r.DelayMax))
	binary.BigEndian.PutUint32(b[12:16], r.Chunks)
	binary.BigEndian.PutUint32(b[16:20], r.Lost)
	binary.BigEndian.PutUint32(b[20:24], r.Incomplete)
	return b
}

func parseClientReport(b []byte) ClientReport {
	return ClientReport{
		DelayP50:   time.Duration(binary.BigEndian.Uint32(b[0:4])) * time.Microsecond,
		DelayP99:   time.Duration(binary.BigEndian.Uint32(b[4:8])) * time.Microsecond,
		DelayMax:   time.Duration(binary.BigEndian.Uint32(b[8:12])) * time.Microsecond,
		Chunks:     binary.BigEndian.Uint32(b[12:16]),
		Lost:       binary.BigEndian.Uint32(b[16:20]),
		Incomplete: binary.BigEndian.Uint32(b[20:24]),
	}
}

// durationMicros clamps d to what 32 bits of microseconds hold (71 min).
func durationMicros(d time.Duration) uint32 {
	var micros = d.Microseconds()
	if micros < 0 {
		return 0
	}
	if micros > 1<<32-1 {
		return 1<<32 - 1
	}
	return uint32(micros)
}

// ClientReportClientID returns the client a report says it comes from, for
// the relay to check against the sender.
func ClientReportClientID(packet []byte) (uuid.UUID, bool) {
	if len(packet) != ClientReportSize || PacketKind(packet) != KindClientReport {
		return uuid.Nil, false
	}
	var id = uuid.FromBytesOrNil(packet[2:18])
	return id, id != uuid.Nil
}

// ClientReportSealer seals the reports of one client (client side). The
// handshake makes one along with the stream opener.
type ClientReportSealer interface {
	SealClientReport(report ClientReport) []byte
}

// ClientReportOpener opens the reports of one client sealed with one report
// key (server side). The handshake makes one per answered client hello.
type ClientReportOpener interface {
	// KeyID is the report key ID the client's reports carry.
	KeyID() ReportKeyID
	// OpenClientReport rejects a report that does not open with the key or
	// whose number is not above the last one opened (a replay).
	OpenClientReport(packet []byte) (ClientReport, error)
}

var ErrInvalidClientReport = errors.New("invalid client report")

func deriveReportAEAD(shared, clientHello, serverHelloPrefix []byte) (cipher.AEAD, error) {
	var transcript = sha256.New()
	transcript.Write(clientHello)
	transcript.Write(serverHelloPrefix)
	key, err := hkdf.Key(sha256.New, shared, transcript.Sum(nil), reportKeyInfo, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// ReportKeyID names the report key of one handshake.
type ReportKeyID [reportKeyIDSize]byte

// reportKeyID is the report key ID of a server hello.
func reportKeyID(serverHello []byte) ReportKeyID {
	var id ReportKeyID
	copy(id[:], serverHello[18+CertificateSize:])
	return id
}

func reportNonce(number uint64) []byte {
	var nonce = make([]byte, 12)
	binary.BigEndian.PutUint64(nonce[4:], number)
	return nonce
}

// AESGCMClientReportSealer numbers the reports with a counter that serves as
// nonce; the key is new with every handshake.
type AESGCMClientReportSealer struct {
	clientID uuid.UUID
	keyID    ReportKeyID
	aead     cipher.AEAD
	number   atomic.Uint64
}

// SealClientReport is safe for concurrent use.
func (s *AESGCMClientReportSealer) SealClientReport(report ClientReport) []byte {
	var number = s.number.Add(1)
	var packet = make([]byte, clientReportHeaderSize, ClientReportSize)
	packet[0] = Version
	packet[1] = byte(KindClientReport)
	copy(packet[2:18], s.clientID.Bytes())
	copy(packet[18:26], s.keyID[:])
	binary.BigEndian.PutUint64(packet[26:34], number)
	return s.aead.Seal(packet, reportNonce(number), report.marshal(), packet[:clientReportHeaderSize])
}

type AESGCMClientReportOpener struct {
	clientID uuid.UUID
	keyID    ReportKeyID
	aead     cipher.AEAD

	mu         sync.Mutex
	lastNumber uint64
}

func (o *AESGCMClientReportOpener) KeyID() ReportKeyID {
	return o.keyID
}

// ClientReportKeyID returns the report key ID of a report.
func ClientReportKeyID(packet []byte) (ReportKeyID, bool) {
	var id ReportKeyID
	if len(packet) != ClientReportSize || PacketKind(packet) != KindClientReport {
		return id, false
	}
	copy(id[:], packet[18:26])
	return id, true
}

func (o *AESGCMClientReportOpener) OpenClientReport(packet []byte) (ClientReport, error) {
	clientID, ok := ClientReportClientID(packet)
	if !ok {
		return ClientReport{}, fmt.Errorf("%w: not a client report", ErrInvalidClientReport)
	}
	if clientID != o.clientID {
		return ClientReport{}, fmt.Errorf("%w: from %s, the key is of %s", ErrInvalidClientReport, clientID, o.clientID)
	}
	if keyID, _ := ClientReportKeyID(packet); keyID != o.keyID {
		return ClientReport{}, fmt.Errorf("%w: another report key", ErrInvalidClientReport)
	}
	var number = binary.BigEndian.Uint64(packet[26:34])
	payload, err := o.aead.Open(nil, reportNonce(number), packet[clientReportHeaderSize:], packet[:clientReportHeaderSize])
	if err != nil {
		return ClientReport{}, fmt.Errorf("%w: %w", ErrInvalidClientReport, err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if number <= o.lastNumber {
		return ClientReport{}, fmt.Errorf("%w: report %d again or out of order", ErrInvalidClientReport, number)
	}
	o.lastNumber = number
	return parseClientReport(payload), nil
}
