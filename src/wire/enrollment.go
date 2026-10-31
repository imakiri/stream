package wire

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/gofrs/uuid/v5"
)

// Enrollment is what the server owner sends back to a new client after
// signing its public key: everything the client needs to connect.
//
//	0..120    client certificate
//	120..152  root public key
//	152..168  server ID: the only certificate the client accepts as server
//
// It travels as standard base64, see EncodeEnrollment.
type Enrollment struct {
	Certificate   Certificate
	RootPublicKey ed25519.PublicKey
	ServerID      uuid.UUID
}

const EnrollmentSize = CertificateSize + ed25519.PublicKeySize + 16

var ErrInvalidEnrollment = errors.New("invalid enrollment")

func (e Enrollment) Marshal() []byte {
	var b = make([]byte, 0, EnrollmentSize)
	b = append(b, e.Certificate.Marshal()...)
	b = append(b, e.RootPublicKey...)
	return append(b, e.ServerID.Bytes()...)
}

func ParseEnrollment(raw []byte) (Enrollment, error) {
	if len(raw) != EnrollmentSize {
		return Enrollment{}, fmt.Errorf("%w: %d bytes, want %d", ErrInvalidEnrollment, len(raw), EnrollmentSize)
	}
	certificate, err := ParseCertificate(raw[:CertificateSize])
	if err != nil {
		return Enrollment{}, fmt.Errorf("%w: %w", ErrInvalidEnrollment, err)
	}
	var e = Enrollment{
		Certificate:   certificate,
		RootPublicKey: ed25519.PublicKey(append([]byte(nil), raw[CertificateSize:CertificateSize+ed25519.PublicKeySize]...)),
		ServerID:      uuid.FromBytesOrNil(raw[CertificateSize+ed25519.PublicKeySize:]),
	}
	if e.ServerID == uuid.Nil {
		return Enrollment{}, fmt.Errorf("%w: nil server id", ErrInvalidEnrollment)
	}
	return e, nil
}

// EncodeBase64 and DecodeBase64 are the transfer format of public keys and
// enrollments: standard base64, surrounding whitespace ignored.
func EncodeBase64(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}

func DecodeBase64(text string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimSpace(text))
}
