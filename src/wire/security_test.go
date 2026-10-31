package wire

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/gofrs/uuid/v5"
)

// Security properties of the data stream and the handshake: what an attacker
// on the path (or at the proxy) can and cannot do with the packets it sees.

// newStreamPair returns a sealer and an opener of one fresh stream key.
func newStreamPair(t testing.TB) (*AESGCMStreamSealer, *AESGCMStreamOpener) {
	t.Helper()
	key, err := NewStreamKey()
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := NewStreamSealer(key)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewStreamOpener(key)
	if err != nil {
		t.Fatal(err)
	}
	return sealer, opener
}

// TestDataPacketRejectsAnyChangedByte flips every bit of a data packet in
// turn: the header is authenticated as additional data and the rest is
// ciphertext and tag, so no change may open.
func TestDataPacketRejectsAnyChangedByte(t *testing.T) {
	var sealer, opener = newStreamPair(t)
	var packet = sealer.SealData(bytes.Repeat([]byte{7}, 1056))
	for i := range packet {
		for bit := range 8 {
			var changed = bytes.Clone(packet)
			changed[i] ^= 1 << bit
			if payload, err := opener.OpenData(changed); err == nil {
				t.Fatalf("byte %d bit %d changed, still opened %d bytes", i, bit, len(payload))
			}
		}
	}
	if _, err := opener.OpenData(packet); err != nil {
		t.Fatalf("the original no longer opens: %v", err)
	}
}

// TestDataPacketRejectsWrongLength cuts and extends a data packet.
func TestDataPacketRejectsWrongLength(t *testing.T) {
	var sealer, opener = newStreamPair(t)
	var packet = sealer.SealData(bytes.Repeat([]byte{7}, 1056))
	for n := range len(packet) {
		if _, err := opener.OpenData(packet[:n]); err == nil {
			t.Fatalf("a packet cut to %d of %d bytes opened", n, len(packet))
		}
	}
	if _, err := opener.OpenData(append(bytes.Clone(packet), 0)); err == nil {
		t.Fatal("a packet with a byte appended opened")
	}
}

// TestDataPacketOfAnotherKeyWithTheSameEpoch forges a packet under a key the
// attacker made up, with the epoch it read from a real packet's clear header.
func TestDataPacketOfAnotherKeyWithTheSameEpoch(t *testing.T) {
	var _, opener = newStreamPair(t)
	var forged = StreamKey{Epoch: opener.Epoch()}
	if _, err := rand.Read(forged.Key[:]); err != nil {
		t.Fatal(err)
	}
	forgedSealer, err := NewStreamSealer(forged)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opener.OpenData(forgedSealer.SealData([]byte("forged"))); !errors.Is(err, ErrInvalidDataPacket) {
		t.Fatalf("got %v, want ErrInvalidDataPacket", err)
	}
}

// TestDataPacketNumbersAreUnique seals from many goroutines, as the server
// does for stream data and nothing else, and checks no packet number, the
// AES-GCM nonce, repeats: a repeated nonce under one key would leak the XOR of
// two payloads and allow forgeries.
func TestDataPacketNumbersAreUnique(t *testing.T) {
	var sealer, _ = newStreamPair(t)
	const goroutines, perGoroutine = 8, 2000
	var numbers = make(chan uint64, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range perGoroutine {
				numbers <- binary.BigEndian.Uint64(sealer.SealData([]byte{1})[6:DataHeaderSize])
			}
		})
	}
	wg.Wait()
	close(numbers)
	var seen = make(map[uint64]bool, goroutines*perGoroutine)
	for number := range numbers {
		if seen[number] {
			t.Fatalf("packet number %d repeats", number)
		}
		seen[number] = true
	}
}

// TestDataPacketHidesPayload checks that the payload does not show through:
// a plain text marker is not in the packet, and the same payload sealed twice
// gives different ciphertexts.
func TestDataPacketHidesPayload(t *testing.T) {
	var sealer, _ = newStreamPair(t)
	var payload = bytes.Repeat([]byte("plain mpegts payload "), 50)
	var first, second = sealer.SealData(payload), sealer.SealData(payload)
	if bytes.Contains(first, []byte("plain mpegts")) {
		t.Fatal("the payload shows in the data packet")
	}
	if bytes.Equal(first[DataHeaderSize:], second[DataHeaderSize:]) {
		t.Fatal("the same payload sealed twice gives the same ciphertext")
	}
}

// TestClientHelloRejectsAnyChangedByte flips every byte of a client hello:
// the certificate is signed by the root and the rest by the client.
func TestClientHelloRejectsAnyChangedByte(t *testing.T) {
	var pki = newTestPKI(t)
	var pair = newHandshakePair(t, pki, pki.issue(t, nextYear), pki.issue(t, nextYear))
	hello, err := pair.client.NewClientHello()
	if err != nil {
		t.Fatal(err)
	}
	for i := range hello {
		var changed = bytes.Clone(hello)
		changed[i] ^= 0x01
		if _, _, err := pair.server.AnswerClientHello(changed, now); err == nil {
			t.Fatalf("client hello with byte %d changed was answered", i)
		}
	}
}

// TestServerHelloRejectsAnyChangedByte flips every byte of a server hello:
// the server signs all of it together with the client hello.
func TestServerHelloRejectsAnyChangedByte(t *testing.T) {
	var pki = newTestPKI(t)
	var pair = newHandshakePair(t, pki, pki.issue(t, nextYear), pki.issue(t, nextYear))
	hello, err := pair.client.NewClientHello()
	if err != nil {
		t.Fatal(err)
	}
	serverHello, _, err := pair.server.AnswerClientHello(hello, now)
	if err != nil {
		t.Fatal(err)
	}
	for i := range serverHello {
		var changed = bytes.Clone(serverHello)
		changed[i] ^= 0x01
		if _, _, err := pair.client.FinishServerHello(changed, now); err == nil {
			t.Fatalf("server hello with byte %d changed was accepted", i)
		}
	}
	if _, _, err := pair.client.FinishServerHello(serverHello, now); err != nil {
		t.Fatalf("the original server hello is rejected: %v", err)
	}
}

// TestHandshakeRejectsStolenCertificate uses a real certificate, which is
// public (it travels in clear through the proxy), with a key of the attacker:
// without the certified private key neither a client nor the server can be
// impersonated.
func TestHandshakeRejectsStolenCertificate(t *testing.T) {
	var pki = newTestPKI(t)
	var serverCreds, clientCreds = pki.issue(t, nextYear), pki.issue(t, nextYear)
	_, attackerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("as a client", func(t *testing.T) {
		var pair = newHandshakePair(t, pki, serverCreds, Credentials{Certificate: clientCreds.Certificate, PrivateKey: attackerKey})
		hello, err := pair.client.NewClientHello()
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := pair.server.AnswerClientHello(hello, now); !errors.Is(err, ErrInvalidHandshake) {
			t.Fatalf("got %v, want ErrInvalidHandshake", err)
		}
	})

	t.Run("as the server", func(t *testing.T) {
		var pair = newHandshakePair(t, pki, serverCreds, clientCreds)
		var impostor = newHandshakePair(t, pki, Credentials{Certificate: serverCreds.Certificate, PrivateKey: attackerKey}, clientCreds)
		hello, err := pair.client.NewClientHello()
		if err != nil {
			t.Fatal(err)
		}
		serverHello, _, err := impostor.server.AnswerClientHello(hello, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := pair.client.FinishServerHello(serverHello, now); !errors.Is(err, ErrInvalidHandshake) {
			t.Fatalf("got %v, want ErrInvalidHandshake", err)
		}
	})
}

// TestReplayedClientHelloGivesNothing replays a client's hello to the server,
// as anyone on the path can: the server answers, but the stream key in the
// answer is wrapped for the ephemeral key of the real client, and another
// client cannot finish it.
func TestReplayedClientHelloGivesNothing(t *testing.T) {
	var pki = newTestPKI(t)
	var serverCreds = pki.issue(t, nextYear)
	var victim = newHandshakePair(t, pki, serverCreds, pki.issue(t, nextYear))
	var attacker = newHandshakePair(t, pki, serverCreds, pki.issue(t, nextYear))
	attacker.server = victim.server

	victimHello, err := victim.client.NewClientHello()
	if err != nil {
		t.Fatal(err)
	}
	serverHello, _, err := victim.server.AnswerClientHello(victimHello, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := attacker.client.NewClientHello(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := attacker.client.FinishServerHello(serverHello, now); !errors.Is(err, ErrInvalidHandshake) {
		t.Fatalf("another client finished the victim's handshake: %v", err)
	}
	// relabelled for the attacker, the signature no longer fits
	var relabelled = bytes.Clone(serverHello)
	copy(relabelled[2:18], attacker.client.Credentials.Certificate.ID.Bytes())
	if _, _, err := attacker.client.FinishServerHello(relabelled, now); !errors.Is(err, ErrInvalidHandshake) {
		t.Fatalf("a relabelled server hello was accepted: %v", err)
	}
}

// TestEnrollmentRejectsChangedBytes checks the parts of an enrollment the
// client can check by itself: a changed certificate breaks the root's
// signature.
func TestEnrollmentRejectsChangedBytes(t *testing.T) {
	var pki = newTestPKI(t)
	var creds = pki.issue(t, nextYear)
	var raw = Enrollment{Certificate: creds.Certificate, RootPublicKey: pki.rootPub, ServerID: uuid.Must(uuid.NewV4())}.Marshal()
	for i := range CertificateSize {
		var changed = bytes.Clone(raw)
		changed[i] ^= 0x01
		enrollment, err := ParseEnrollment(changed)
		if err != nil {
			continue
		}
		if err := (RootVerifier{Root: enrollment.RootPublicKey}).VerifyCertificate(enrollment.Certificate, now); err == nil {
			t.Fatalf("enrollment with certificate byte %d changed verifies", i)
		}
	}
}

// The fuzz targets feed untrusted bytes to every parser that runs before
// anything is authenticated; they must fail cleanly, never panic. The seeds
// run with go test; go test -fuzz=FuzzX ./src/wire explores further.

func FuzzParseProxyHello(f *testing.F) {
	f.Add(MarshalProxyHello(RoleClient, uuid.Must(uuid.NewV4())))
	f.Fuzz(func(t *testing.T, packet []byte) {
		_, _, _ = ParseProxyHello(packet)
		_ = PacketKind(packet)
	})
}

func FuzzParseCertificateAndEnrollment(f *testing.F) {
	var pki = newTestPKI(f)
	var creds = pki.issue(f, nextYear)
	f.Add(Enrollment{Certificate: creds.Certificate, RootPublicKey: pki.rootPub, ServerID: uuid.Must(uuid.NewV4())}.Marshal())
	f.Add(creds.Certificate.Marshal())
	f.Fuzz(func(t *testing.T, raw []byte) {
		if certificate, err := ParseCertificate(raw); err == nil {
			_ = (RootVerifier{Root: pki.rootPub}).VerifyCertificate(certificate, now)
		}
		if enrollment, err := ParseEnrollment(raw); err == nil {
			_ = (RootVerifier{Root: enrollment.RootPublicKey}).VerifyCertificate(enrollment.Certificate, now)
		}
		_, _ = DecodeBase64(string(raw))
	})
}

func FuzzOpenData(f *testing.F) {
	var sealer, opener = newStreamPair(f)
	var sealed = sealer.SealData([]byte("payload"))
	f.Add(sealed)
	f.Add([]byte{Version, byte(KindData)})
	f.Fuzz(func(t *testing.T, packet []byte) {
		if bytes.Equal(packet, sealed) {
			return
		}
		if payload, err := opener.OpenData(packet); err == nil {
			t.Fatalf("a fuzzed packet opened: %d bytes", len(payload))
		}
		_, _ = DataPacketEpoch(packet)
	})
}

func FuzzAnswerClientHello(f *testing.F) {
	var pki = newTestPKI(f)
	var pair = newHandshakePair(f, pki, pki.issue(f, nextYear), pki.issue(f, nextYear))
	hello, _ := pair.client.NewClientHello()
	f.Add(hello)
	f.Fuzz(func(t *testing.T, clientHello []byte) {
		_, _, _ = pair.server.AnswerClientHello(clientHello, now)
	})
}

func FuzzFinishServerHello(f *testing.F) {
	var pki = newTestPKI(f)
	var pair = newHandshakePair(f, pki, pki.issue(f, nextYear), pki.issue(f, nextYear))
	hello, _ := pair.client.NewClientHello()
	serverHello, _, _ := pair.server.AnswerClientHello(hello, now)
	f.Add(serverHello)
	f.Fuzz(func(t *testing.T, packet []byte) {
		_, _ = ServerHelloClientID(packet)
		if bytes.Equal(packet, serverHello) {
			return
		}
		if _, _, err := pair.client.FinishServerHello(packet, now); err == nil {
			t.Fatal("a fuzzed server hello was accepted")
		}
	})
}
