package wire

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
)

// The handshake runs between a client and the server through the proxy, like
// mutual TLS with a two-level PKI: both sides show a certificate signed by
// the root. The client sends its certificate and an ephemeral X25519 key; the
// server answers with its certificate, its ephemeral key and the stream key
// encrypted with a key both derive from the two ephemeral keys. Each side
// signs what it sends with the key of its certificate; the server's signature
// also covers the client hello, so a recorded server hello does not fit any
// later client hello.
//
// Client hello, ClientHelloSize bytes:
//
//	0         version
//	1         kind = KindClientHello
//	2..122    client certificate
//	122..154  client ephemeral X25519 public key
//	154..218  client signature of bytes 0..154
//
// Server hello, ServerHelloSize bytes:
//
//	0         version
//	1         kind = KindServerHello
//	2..18     client ID: the proxy routes the hello to this client
//	18..138   server certificate
//	138..170  server ephemeral X25519 public key
//	170..174  stream key epoch, big endian
//	174..222  stream key, AES-256-GCM sealed with the wrap key, zero nonce
//	222..286  server signature of the client hello followed by bytes 0..222
//
// wrap key = HKDF-SHA256(X25519 shared secret, salt = SHA-256(client hello ||
// server hello 0..174), info = wrapKeyInfo). It is used once, so the zero
// nonce is safe.
const (
	ClientHelloSize = 2 + CertificateSize + 32 + ed25519.SignatureSize
	ServerHelloSize = 2 + 16 + CertificateSize + 32 + 4 + 32 + 16 + ed25519.SignatureSize
)

const (
	clientHelloSignedSize   = ClientHelloSize - ed25519.SignatureSize
	serverHelloWrapAADSize  = 174
	serverHelloSignedSize   = ServerHelloSize - ed25519.SignatureSize
	serverHelloEphemeralEnd = 170
	wrapKeyInfo             = "imakiri/stream wire v3 stream key wrap"
)

var ErrInvalidHandshake = errors.New("invalid handshake")

// ServerHandshaker answers client hellos on the server.
type ServerHandshaker interface {
	// AnswerClientHello verifies a client hello and returns the server hello
	// for that client along with the client's ID.
	AnswerClientHello(clientHello []byte, now time.Time) (serverHello []byte, clientID uuid.UUID, err error)
}

// ClientHandshaker runs the handshake on a client.
type ClientHandshaker interface {
	// NewClientHello starts a new attempt and returns its hello; a server
	// hello is accepted only for the latest attempt.
	NewClientHello() ([]byte, error)
	// FinishServerHello verifies the server hello of the latest attempt and
	// returns the opener for the stream.
	FinishServerHello(serverHello []byte, now time.Time) (StreamOpener, error)
}

// ServerHelloClientID returns the client a server hello is addressed to, for
// the proxy.
func ServerHelloClientID(serverHello []byte) (uuid.UUID, bool) {
	if len(serverHello) != ServerHelloSize || PacketKind(serverHello) != KindServerHello {
		return uuid.Nil, false
	}
	var id = uuid.FromBytesOrNil(serverHello[2:18])
	return id, id != uuid.Nil
}

func deriveWrapAEAD(shared, clientHello, serverHelloPrefix []byte) (cipher.AEAD, error) {
	var transcript = sha256.New()
	transcript.Write(clientHello)
	transcript.Write(serverHelloPrefix)
	key, err := hkdf.Key(sha256.New, shared, transcript.Sum(nil), wrapKeyInfo, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func serverHelloSigned(clientHello, serverHello []byte) []byte {
	var signed = make([]byte, 0, len(clientHello)+serverHelloSignedSize)
	signed = append(signed, clientHello...)
	return append(signed, serverHello[:serverHelloSignedSize]...)
}

// CertificateServerHandshake is the server side of the certificate handshake.
type CertificateServerHandshake struct {
	Credentials Credentials
	Verifier    CertificateVerifier
	StreamKey   StreamKey
}

func (h *CertificateServerHandshake) AnswerClientHello(clientHello []byte, now time.Time) ([]byte, uuid.UUID, error) {
	if len(clientHello) != ClientHelloSize || PacketKind(clientHello) != KindClientHello {
		return nil, uuid.Nil, fmt.Errorf("%w: not a client hello", ErrInvalidHandshake)
	}
	clientCertificate, err := ParseCertificate(clientHello[2 : 2+CertificateSize])
	if err != nil {
		return nil, uuid.Nil, err
	}
	if err := h.Verifier.VerifyCertificate(clientCertificate, now); err != nil {
		return nil, uuid.Nil, err
	}
	if !ed25519.Verify(clientCertificate.PublicKey, clientHello[:clientHelloSignedSize], clientHello[clientHelloSignedSize:]) {
		return nil, uuid.Nil, fmt.Errorf("%w: bad client signature of %s", ErrInvalidHandshake, clientCertificate.ID)
	}
	clientEphemeral, err := ecdh.X25519().NewPublicKey(clientHello[2+CertificateSize : clientHelloSignedSize])
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("%w: %w", ErrInvalidHandshake, err)
	}

	serverEphemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, uuid.Nil, err
	}
	shared, err := serverEphemeral.ECDH(clientEphemeral)
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("%w: %w", ErrInvalidHandshake, err)
	}

	var serverHello = make([]byte, ServerHelloSize)
	serverHello[0] = Version
	serverHello[1] = byte(KindServerHello)
	copy(serverHello[2:18], clientCertificate.ID.Bytes())
	copy(serverHello[18:18+CertificateSize], h.Credentials.Certificate.Marshal())
	copy(serverHello[18+CertificateSize:serverHelloEphemeralEnd], serverEphemeral.PublicKey().Bytes())
	binary.BigEndian.PutUint32(serverHello[serverHelloEphemeralEnd:serverHelloWrapAADSize], h.StreamKey.Epoch)

	wrap, err := deriveWrapAEAD(shared, clientHello, serverHello[:serverHelloWrapAADSize])
	if err != nil {
		return nil, uuid.Nil, err
	}
	wrap.Seal(serverHello[serverHelloWrapAADSize:serverHelloWrapAADSize], make([]byte, wrap.NonceSize()), h.StreamKey.Key[:], serverHello[:serverHelloWrapAADSize])

	copy(serverHello[serverHelloSignedSize:], ed25519.Sign(h.Credentials.PrivateKey, serverHelloSigned(clientHello, serverHello)))
	return serverHello, clientCertificate.ID, nil
}

// CertificateClientHandshake is the client side of the certificate handshake.
// It accepts only a server whose certificate has ServerID, so another client
// certificate from the same root cannot pose as the server.
type CertificateClientHandshake struct {
	Credentials Credentials
	Verifier    CertificateVerifier
	ServerID    uuid.UUID

	mu        sync.Mutex
	ephemeral *ecdh.PrivateKey
	hello     []byte
}

func (h *CertificateClientHandshake) NewClientHello() ([]byte, error) {
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	var hello = make([]byte, ClientHelloSize)
	hello[0] = Version
	hello[1] = byte(KindClientHello)
	copy(hello[2:2+CertificateSize], h.Credentials.Certificate.Marshal())
	copy(hello[2+CertificateSize:clientHelloSignedSize], ephemeral.PublicKey().Bytes())
	copy(hello[clientHelloSignedSize:], ed25519.Sign(h.Credentials.PrivateKey, hello[:clientHelloSignedSize]))

	h.mu.Lock()
	h.ephemeral, h.hello = ephemeral, hello
	h.mu.Unlock()
	return hello, nil
}

func (h *CertificateClientHandshake) FinishServerHello(serverHello []byte, now time.Time) (StreamOpener, error) {
	h.mu.Lock()
	var ephemeral, clientHello = h.ephemeral, h.hello
	h.mu.Unlock()
	if ephemeral == nil {
		return nil, fmt.Errorf("%w: no client hello sent", ErrInvalidHandshake)
	}

	clientID, ok := ServerHelloClientID(serverHello)
	if !ok {
		return nil, fmt.Errorf("%w: not a server hello", ErrInvalidHandshake)
	}
	if clientID != h.Credentials.Certificate.ID {
		return nil, fmt.Errorf("%w: server hello for %s", ErrInvalidHandshake, clientID)
	}
	serverCertificate, err := ParseCertificate(serverHello[18 : 18+CertificateSize])
	if err != nil {
		return nil, err
	}
	if serverCertificate.ID != h.ServerID {
		return nil, fmt.Errorf("%w: certificate %s is not the server %s", ErrInvalidHandshake, serverCertificate.ID, h.ServerID)
	}
	if err := h.Verifier.VerifyCertificate(serverCertificate, now); err != nil {
		return nil, err
	}
	if !ed25519.Verify(serverCertificate.PublicKey, serverHelloSigned(clientHello, serverHello), serverHello[serverHelloSignedSize:]) {
		return nil, fmt.Errorf("%w: bad server signature, or the hello answers an older attempt", ErrInvalidHandshake)
	}

	serverEphemeral, err := ecdh.X25519().NewPublicKey(serverHello[18+CertificateSize : serverHelloEphemeralEnd])
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidHandshake, err)
	}
	shared, err := ephemeral.ECDH(serverEphemeral)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidHandshake, err)
	}
	wrap, err := deriveWrapAEAD(shared, clientHello, serverHello[:serverHelloWrapAADSize])
	if err != nil {
		return nil, err
	}
	key, err := wrap.Open(nil, make([]byte, wrap.NonceSize()), serverHello[serverHelloWrapAADSize:serverHelloSignedSize], serverHello[:serverHelloWrapAADSize])
	if err != nil {
		return nil, fmt.Errorf("%w: stream key: %w", ErrInvalidHandshake, err)
	}

	var streamKey = StreamKey{Epoch: binary.BigEndian.Uint32(serverHello[serverHelloEphemeralEnd:serverHelloWrapAADSize])}
	copy(streamKey.Key[:], key)
	return NewStreamOpener(streamKey)
}
