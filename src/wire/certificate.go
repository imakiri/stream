package wire

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
)

// CertificateSize is the size of a marshaled certificate:
//
//	0..16    ID
//	16..24   not after, unix seconds, big endian
//	24..56   ed25519 public key
//	56..120  ed25519 signature of bytes 0..56 by the root key
const CertificateSize = 16 + 8 + ed25519.PublicKeySize + ed25519.SignatureSize

const certificateSignedSize = CertificateSize - ed25519.SignatureSize

// Certificate binds a peer ID to its public key until NotAfter. It is signed
// by the root key; the PKI has two levels, the root signs every peer.
type Certificate struct {
	ID        uuid.UUID
	NotAfter  time.Time
	PublicKey ed25519.PublicKey
	Signature []byte
}

func (c Certificate) signed() []byte {
	var b = make([]byte, certificateSignedSize)
	copy(b[0:16], c.ID.Bytes())
	binary.BigEndian.PutUint64(b[16:24], uint64(c.NotAfter.Unix()))
	copy(b[24:56], c.PublicKey)
	return b
}

// IssueCertificate signs a certificate for publicKey with the root key.
func IssueCertificate(root ed25519.PrivateKey, id uuid.UUID, notAfter time.Time, publicKey ed25519.PublicKey) Certificate {
	var c = Certificate{ID: id, NotAfter: time.Unix(notAfter.Unix(), 0), PublicKey: publicKey}
	c.Signature = ed25519.Sign(root, c.signed())
	return c
}

func (c Certificate) Marshal() []byte {
	return append(c.signed(), c.Signature...)
}

var ErrInvalidCertificate = errors.New("invalid certificate")

func ParseCertificate(raw []byte) (Certificate, error) {
	if len(raw) != CertificateSize {
		return Certificate{}, fmt.Errorf("%w: %d bytes", ErrInvalidCertificate, len(raw))
	}
	var c Certificate
	c.ID = uuid.FromBytesOrNil(raw[0:16])
	c.NotAfter = time.Unix(int64(binary.BigEndian.Uint64(raw[16:24])), 0)
	c.PublicKey = ed25519.PublicKey(append([]byte(nil), raw[24:56]...))
	c.Signature = append([]byte(nil), raw[56:CertificateSize]...)
	if c.ID == uuid.Nil {
		return Certificate{}, fmt.Errorf("%w: nil id", ErrInvalidCertificate)
	}
	return c, nil
}

// CertificateVerifier decides whether a peer certificate is trusted.
type CertificateVerifier interface {
	VerifyCertificate(c Certificate, now time.Time) error
}

// RootVerifier trusts certificates signed by Root that have not expired.
type RootVerifier struct {
	Root ed25519.PublicKey
}

var (
	ErrCertificateNotTrusted = errors.New("certificate is not signed by the root")
	ErrCertificateExpired    = errors.New("certificate expired")
)

func (v RootVerifier) VerifyCertificate(c Certificate, now time.Time) error {
	if len(c.PublicKey) != ed25519.PublicKeySize || !ed25519.Verify(v.Root, c.signed(), c.Signature) {
		return fmt.Errorf("%w: %s", ErrCertificateNotTrusted, c.ID)
	}
	if now.After(c.NotAfter) {
		return fmt.Errorf("%w: %s on %s", ErrCertificateExpired, c.ID, c.NotAfter.UTC().Format(time.DateOnly))
	}
	return nil
}

// Credentials are a peer's certificate and the private key it certifies.
type Credentials struct {
	Certificate Certificate
	PrivateKey  ed25519.PrivateKey
}
