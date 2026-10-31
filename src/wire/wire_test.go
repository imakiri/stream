package wire

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

var (
	_ CertificateVerifier = RootVerifier{}
	_ ServerHandshaker    = (*CertificateServerHandshake)(nil)
	_ ClientHandshaker    = (*CertificateClientHandshake)(nil)
	_ StreamSealer        = (*AESGCMStreamSealer)(nil)
	_ StreamOpener        = (*AESGCMStreamOpener)(nil)
)

type testPKI struct {
	root    ed25519.PrivateKey
	rootPub ed25519.PublicKey
}

func newTestPKI(t testing.TB) testPKI {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return testPKI{root: priv, rootPub: pub}
}

func (p testPKI) issue(t testing.TB, notAfter time.Time) Credentials {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return Credentials{Certificate: IssueCertificate(p.root, uuid.Must(uuid.NewV4()), notAfter, pub), PrivateKey: priv}
}

var now = time.Now()
var nextYear = now.AddDate(1, 0, 0)

func TestCertificateRoundTripAndVerify(t *testing.T) {
	var pki = newTestPKI(t)
	var creds = pki.issue(t, nextYear)
	parsed, err := ParseCertificate(creds.Certificate.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ID != creds.Certificate.ID || !parsed.NotAfter.Equal(creds.Certificate.NotAfter) {
		t.Fatalf("round trip: %+v vs %+v", parsed, creds.Certificate)
	}
	if err := (RootVerifier{Root: pki.rootPub}).VerifyCertificate(parsed, now); err != nil {
		t.Fatal(err)
	}

	if err := (RootVerifier{Root: newTestPKI(t).rootPub}).VerifyCertificate(parsed, now); !errors.Is(err, ErrCertificateNotTrusted) {
		t.Fatalf("other root: %v", err)
	}
	if err := (RootVerifier{Root: pki.rootPub}).VerifyCertificate(parsed, nextYear.Add(time.Hour)); !errors.Is(err, ErrCertificateExpired) {
		t.Fatalf("expired: %v", err)
	}
	var tampered = parsed
	tampered.NotAfter = tampered.NotAfter.AddDate(10, 0, 0)
	if err := (RootVerifier{Root: pki.rootPub}).VerifyCertificate(tampered, now); !errors.Is(err, ErrCertificateNotTrusted) {
		t.Fatalf("tampered: %v", err)
	}
}

type handshakePair struct {
	server *CertificateServerHandshake
	client *CertificateClientHandshake
	key    StreamKey
}

func newHandshakePair(t testing.TB, pki testPKI, server, client Credentials) handshakePair {
	t.Helper()
	key, err := NewStreamKey()
	if err != nil {
		t.Fatal(err)
	}
	var verifier = RootVerifier{Root: pki.rootPub}
	return handshakePair{
		server: &CertificateServerHandshake{Credentials: server, Verifier: verifier, StreamKey: key},
		client: &CertificateClientHandshake{Credentials: client, Verifier: verifier, ServerID: server.Certificate.ID},
		key:    key,
	}
}

func TestHandshakeDeliversStreamKey(t *testing.T) {
	var pki = newTestPKI(t)
	var serverCreds, clientCreds = pki.issue(t, nextYear), pki.issue(t, nextYear)
	var pair = newHandshakePair(t, pki, serverCreds, clientCreds)

	clientHello, err := pair.client.NewClientHello()
	if err != nil {
		t.Fatal(err)
	}
	serverHello, clientID, _, err := pair.server.AnswerClientHello(clientHello, now)
	if err != nil {
		t.Fatal(err)
	}
	if clientID != clientCreds.Certificate.ID {
		t.Fatalf("client id %s", clientID)
	}
	if routed, _ := ServerHelloClientID(serverHello); routed != clientCreds.Certificate.ID {
		t.Fatalf("server hello routed to %s", routed)
	}
	session, serverClock, err := pair.client.FinishServerHello(serverHello, now)
	if err != nil {
		t.Fatal(err)
	}
	var opener = session.Stream
	if !serverClock.Equal(time.UnixMilli(now.UnixMilli())) {
		t.Fatalf("server clock %s, want %s", serverClock, now)
	}

	sealer, err := NewStreamSealer(pair.key)
	if err != nil {
		t.Fatal(err)
	}
	var payload = bytes.Repeat([]byte{9}, 1056)
	var packet = sealer.SealData(payload)
	if len(packet) != len(payload)+DataOverhead {
		t.Fatalf("data packet %d bytes", len(packet))
	}
	opened, err := opener.OpenData(packet)
	if err != nil || !bytes.Equal(opened, payload) {
		t.Fatalf("open: %v", err)
	}
	packet[len(packet)-1] ^= 1
	if _, err := opener.OpenData(packet); !errors.Is(err, ErrInvalidDataPacket) {
		t.Fatalf("tampered data: %v", err)
	}
}

func TestHandshakeRejects(t *testing.T) {
	var pki = newTestPKI(t)
	var serverCreds, clientCreds = pki.issue(t, nextYear), pki.issue(t, nextYear)

	t.Run("client with an expired certificate", func(t *testing.T) {
		var pair = newHandshakePair(t, pki, serverCreds, pki.issue(t, now.Add(-time.Hour)))
		hello, _ := pair.client.NewClientHello()
		if _, _, _, err := pair.server.AnswerClientHello(hello, now); !errors.Is(err, ErrCertificateExpired) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("client from another root", func(t *testing.T) {
		var pair = newHandshakePair(t, pki, serverCreds, newTestPKI(t).issue(t, nextYear))
		hello, _ := pair.client.NewClientHello()
		if _, _, _, err := pair.server.AnswerClientHello(hello, now); !errors.Is(err, ErrCertificateNotTrusted) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("client hello with a foreign ephemeral key", func(t *testing.T) {
		var pair = newHandshakePair(t, pki, serverCreds, clientCreds)
		hello, _ := pair.client.NewClientHello()
		hello[2+CertificateSize] ^= 1
		if _, _, _, err := pair.server.AnswerClientHello(hello, now); !errors.Is(err, ErrInvalidHandshake) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("another client certificate posing as the server", func(t *testing.T) {
		var impostor = pki.issue(t, nextYear)
		var pair = newHandshakePair(t, pki, serverCreds, clientCreds)
		var fake = newHandshakePair(t, pki, impostor, clientCreds)
		hello, _ := pair.client.NewClientHello()
		serverHello, _, _, err := fake.server.AnswerClientHello(hello, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := pair.client.FinishServerHello(serverHello, now); !errors.Is(err, ErrInvalidHandshake) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("server hello of an older attempt", func(t *testing.T) {
		var pair = newHandshakePair(t, pki, serverCreds, clientCreds)
		oldHello, _ := pair.client.NewClientHello()
		oldServerHello, _, _, err := pair.server.AnswerClientHello(oldHello, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pair.client.NewClientHello(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := pair.client.FinishServerHello(oldServerHello, now); !errors.Is(err, ErrInvalidHandshake) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("data of another stream key", func(t *testing.T) {
		var pair = newHandshakePair(t, pki, serverCreds, clientCreds)
		hello, _ := pair.client.NewClientHello()
		serverHello, _, _, _ := pair.server.AnswerClientHello(hello, now)
		session, _, err := pair.client.FinishServerHello(serverHello, now)
		if err != nil {
			t.Fatal(err)
		}
		var opener = session.Stream
		otherKey, _ := NewStreamKey()
		sealer, _ := NewStreamSealer(otherKey)
		if _, err := opener.OpenData(sealer.SealData([]byte("x"))); !errors.Is(err, ErrUnknownEpoch) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestRelayHelloRoundTrip(t *testing.T) {
	var id = uuid.Must(uuid.NewV4())
	role, parsed, err := ParseRelayHello(MarshalRelayHello(RoleServer, id))
	if err != nil || role != RoleServer || parsed != id {
		t.Fatalf("%v %v %v", role, parsed, err)
	}
	if _, _, err := ParseRelayHello(MarshalRelayHello(Role(9), id)); err == nil {
		t.Fatal("want error for unknown role")
	}
}
