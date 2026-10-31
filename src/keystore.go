package src

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/atotto/clipboard"
	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/wire"
)

// Keys and certificates live as base64 text files in a key directory, by
// default the directory of the binary:
//
//	server: ca.key, server.key, server.cert  made on the first start
//	client: client.key                       made on the first start
//	        client.enrollment                pasted by the user once the
//	                                         server owner signed client.key
const (
	caKeyFile            = "ca.key"
	serverKeyFile        = "server.key"
	serverCertFile       = "server.cert"
	clientKeyFile        = "client.key"
	clientEnrollmentFile = "client.enrollment"
)

// copyToClipboard is replaced in tests, so they leave the clipboard alone.
var copyToClipboard = clipboard.WriteAll

// serverCertificateValidity is long: the server certificate is made together
// with the CA and renewing it would need every client to be enrolled again.
const serverCertificateValidity = 10 * 365 * 24 * time.Hour

// DefaultKeyDir returns the directory of the running binary.
func DefaultKeyDir() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(executable), nil
}

func readBase64File(dir, name string) ([]byte, error) {
	text, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}
	raw, err := wire.DecodeBase64(string(text))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(dir, name), err)
	}
	return raw, nil
}

// writeBase64File refuses to overwrite: these files are keys.
func writeBase64File(dir, name string, raw []byte) error {
	var file, err = os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(file, wire.EncodeBase64(raw)); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func readPrivateKey(dir, name string) (ed25519.PrivateKey, error) {
	raw, err := readBase64File(dir, name)
	if err != nil {
		return nil, err
	}
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%s: %d bytes, want an ed25519 private key", filepath.Join(dir, name), len(raw))
	}
	return ed25519.PrivateKey(raw), nil
}

// ServerIdentity is the server's credentials and the CA key it signs clients
// with.
type ServerIdentity struct {
	Credentials wire.Credentials
	CAKey       ed25519.PrivateKey
}

func (id ServerIdentity) Verifier() wire.CertificateVerifier {
	return wire.RootVerifier{Root: id.CAKey.Public().(ed25519.PublicKey)}
}

// LoadOrCreateServerIdentity loads the CA and the server credentials from dir,
// making them on the first start.
func LoadOrCreateServerIdentity(dir string) (ServerIdentity, error) {
	caKey, err := readPrivateKey(dir, caKeyFile)
	if errors.Is(err, fs.ErrNotExist) {
		return createServerIdentity(dir)
	}
	if err != nil {
		return ServerIdentity{}, err
	}
	serverKey, err := readPrivateKey(dir, serverKeyFile)
	if err != nil {
		return ServerIdentity{}, err
	}
	rawCertificate, err := readBase64File(dir, serverCertFile)
	if err != nil {
		return ServerIdentity{}, err
	}
	certificate, err := wire.ParseCertificate(rawCertificate)
	if err != nil {
		return ServerIdentity{}, fmt.Errorf("%s: %w", serverCertFile, err)
	}
	var id = ServerIdentity{Credentials: wire.Credentials{Certificate: certificate, PrivateKey: serverKey}, CAKey: caKey}
	if !serverKey.Public().(ed25519.PublicKey).Equal(certificate.PublicKey) {
		return ServerIdentity{}, fmt.Errorf("%s does not certify %s", serverCertFile, serverKeyFile)
	}
	if err := id.Verifier().VerifyCertificate(certificate, time.Now()); err != nil {
		return ServerIdentity{}, fmt.Errorf("%s: %w", serverCertFile, err)
	}
	return id, nil
}

func createServerIdentity(dir string) (ServerIdentity, error) {
	_, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return ServerIdentity{}, err
	}
	serverPublic, serverKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return ServerIdentity{}, err
	}
	var certificate = wire.IssueCertificate(caKey, uuid.Must(uuid.NewV4()), time.Now().Add(serverCertificateValidity), serverPublic)
	for _, file := range []struct {
		name string
		raw  []byte
	}{{caKeyFile, caKey}, {serverKeyFile, serverKey}, {serverCertFile, certificate.Marshal()}} {
		if err := writeBase64File(dir, file.name, file.raw); err != nil {
			return ServerIdentity{}, fmt.Errorf("failed to save %s: %w", file.name, err)
		}
	}
	log.Printf("created the CA and the server certificate %s in %s; keep %s secret: it admits clients", certificate.ID, dir, caKeyFile)
	return ServerIdentity{Credentials: wire.Credentials{Certificate: certificate, PrivateKey: serverKey}, CAKey: caKey}, nil
}

// EnrollClient signs a client's base64 public key and returns the base64
// enrollment to send back to the client.
func (id ServerIdentity) EnrollClient(publicKeyBase64 string, validFor time.Duration) (string, wire.Certificate, error) {
	raw, err := wire.DecodeBase64(publicKeyBase64)
	if err != nil {
		return "", wire.Certificate{}, fmt.Errorf("client public key: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return "", wire.Certificate{}, fmt.Errorf("client public key is %d bytes, want %d", len(raw), ed25519.PublicKeySize)
	}
	var certificate = wire.IssueCertificate(id.CAKey, uuid.Must(uuid.NewV4()), time.Now().Add(validFor), ed25519.PublicKey(raw))
	var enrollment = wire.Enrollment{
		Certificate:   certificate,
		RootPublicKey: id.CAKey.Public().(ed25519.PublicKey),
		ServerID:      id.Credentials.Certificate.ID,
	}
	return wire.EncodeBase64(enrollment.Marshal()), certificate, nil
}

// ClientIdentity is a client's credentials and what its enrollment says about
// the server.
type ClientIdentity struct {
	Credentials wire.Credentials
	Enrollment  wire.Enrollment
}

func (id ClientIdentity) Verifier() wire.CertificateVerifier {
	return wire.RootVerifier{Root: id.Enrollment.RootPublicKey}
}

// ErrClientNotEnrolled is returned by LoadClientIdentity while the client
// key has no enrollment yet.
var ErrClientNotEnrolled = errors.New("the client is not enrolled yet")

// LoadOrCreateClientKey returns the client's public key in base64, the text
// to send to the server owner, making the key on the first call.
func LoadOrCreateClientKey(dir string) (publicKeyBase64 string, created bool, err error) {
	key, err := readPrivateKey(dir, clientKeyFile)
	if errors.Is(err, fs.ErrNotExist) {
		if _, key, err = ed25519.GenerateKey(rand.Reader); err != nil {
			return "", false, err
		}
		if err := writeBase64File(dir, clientKeyFile, key); err != nil {
			return "", false, fmt.Errorf("failed to save %s: %w", clientKeyFile, err)
		}
		created = true
	} else if err != nil {
		return "", false, err
	}
	return wire.EncodeBase64(key.Public().(ed25519.PublicKey)), created, nil
}

// LoadClientIdentity loads the client key and its enrollment from dir;
// ErrClientNotEnrolled if there is no enrollment yet.
func LoadClientIdentity(dir string) (ClientIdentity, error) {
	key, err := readPrivateKey(dir, clientKeyFile)
	if err != nil {
		return ClientIdentity{}, err
	}
	raw, err := readBase64File(dir, clientEnrollmentFile)
	if errors.Is(err, fs.ErrNotExist) {
		return ClientIdentity{}, ErrClientNotEnrolled
	}
	if err != nil {
		return ClientIdentity{}, err
	}
	enrollment, err := parseOwnEnrollment(raw, key)
	if err != nil {
		return ClientIdentity{}, fmt.Errorf("%s: %w", clientEnrollmentFile, err)
	}
	return ClientIdentity{Credentials: wire.Credentials{Certificate: enrollment.Certificate, PrivateKey: key}, Enrollment: enrollment}, nil
}

// SaveClientEnrollment checks the enrollment the server owner sent back,
// base64 as server -sign prints it, against the client key in dir and
// saves it.
func SaveClientEnrollment(dir, enrollmentBase64 string) (ClientIdentity, error) {
	key, err := readPrivateKey(dir, clientKeyFile)
	if err != nil {
		return ClientIdentity{}, err
	}
	raw, err := wire.DecodeBase64(strings.TrimSpace(enrollmentBase64))
	if err != nil {
		return ClientIdentity{}, fmt.Errorf("not base64: %w", err)
	}
	enrollment, err := parseOwnEnrollment(raw, key)
	if err != nil {
		return ClientIdentity{}, fmt.Errorf("not an enrollment of this client: %w", err)
	}
	if err := writeBase64File(dir, clientEnrollmentFile, raw); err != nil {
		return ClientIdentity{}, fmt.Errorf("failed to save %s: %w", clientEnrollmentFile, err)
	}
	return ClientIdentity{Credentials: wire.Credentials{Certificate: enrollment.Certificate, PrivateKey: key}, Enrollment: enrollment}, nil
}

// LoadOrEnrollClientIdentity loads the client credentials from dir. On the
// first start it makes the client key; until the client is enrolled it prints
// the public key, copies it to the clipboard and asks for the enrollment the
// server owner sends back.
func LoadOrEnrollClientIdentity(dir string, in io.Reader, out io.Writer) (ClientIdentity, error) {
	publicKey, created, err := LoadOrCreateClientKey(dir)
	if err != nil {
		return ClientIdentity{}, err
	}
	if created {
		_, _ = fmt.Fprintf(out, "created the client key %s\n", filepath.Join(dir, clientKeyFile))
	}
	identity, err := LoadClientIdentity(dir)
	if !errors.Is(err, ErrClientNotEnrolled) {
		return identity, err
	}

	_, _ = fmt.Fprintf(out, "\nsend this public key to the server owner to sign:\n\n%s\n\n", publicKey)
	if err := copyToClipboard(publicKey); err != nil {
		_, _ = fmt.Fprintf(out, "(could not copy it to the clipboard: %s)\n", err)
	} else {
		_, _ = fmt.Fprintln(out, "(copied to the clipboard)")
	}

	var lines = bufio.NewScanner(in)
	for {
		_, _ = fmt.Fprint(out, "paste the enrollment you got back: ")
		if !lines.Scan() {
			if err := lines.Err(); err != nil {
				return ClientIdentity{}, err
			}
			return ClientIdentity{}, errors.New("no enrollment pasted")
		}
		var text = strings.TrimSpace(lines.Text())
		if text == "" {
			continue
		}
		identity, err := SaveClientEnrollment(dir, text)
		if err != nil {
			_, _ = fmt.Fprintln(out, err)
			continue
		}
		_, _ = fmt.Fprintf(out, "enrolled as %s, valid until %s\n", identity.Credentials.Certificate.ID, identity.Credentials.Certificate.NotAfter.Format(time.DateOnly))
		return identity, nil
	}
}

// parseOwnEnrollment checks that the enrollment certifies key and is signed
// by the root it names.
func parseOwnEnrollment(raw []byte, key ed25519.PrivateKey) (wire.Enrollment, error) {
	enrollment, err := wire.ParseEnrollment(raw)
	if err != nil {
		return wire.Enrollment{}, err
	}
	if !key.Public().(ed25519.PublicKey).Equal(enrollment.Certificate.PublicKey) {
		return wire.Enrollment{}, errors.New("the certificate is for another key")
	}
	if err := (wire.RootVerifier{Root: enrollment.RootPublicKey}).VerifyCertificate(enrollment.Certificate, time.Now()); err != nil {
		return wire.Enrollment{}, err
	}
	return enrollment, nil
}
