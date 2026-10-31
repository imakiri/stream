package src

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/imakiri/stream/src/wire"
)

// TestEnrollmentFromFirstStartToConnect walks the first start of a server and
// a client: the client prints its public key, the server signs it, the
// client takes the pasted enrollment, and the next starts load everything.
func TestEnrollmentFromFirstStartToConnect(t *testing.T) {
	copyToClipboard = func(string) error { return nil }
	var serverDir, clientDir = t.TempDir(), t.TempDir()

	server, err := LoadOrCreateServerIdentity(serverDir)
	if err != nil {
		t.Fatal(err)
	}
	again, err := LoadOrCreateServerIdentity(serverDir)
	if err != nil {
		t.Fatal(err)
	}
	if again.Credentials.Certificate.ID != server.Credentials.Certificate.ID || !again.CAKey.Equal(server.CAKey) {
		t.Fatal("second server start made a new identity")
	}

	// first client start: no enrollment pasted yet
	var out bytes.Buffer
	if _, err := LoadOrEnrollClientIdentity(clientDir, strings.NewReader(""), &out); err == nil {
		t.Fatal("want an error without an enrollment")
	}
	var publicKey = publicKeyFromPrompt(t, out.String())

	enrollment, certificate, err := server.EnrollClient(publicKey, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// an enrollment for another key is refused, then the right one is taken
	otherDir := t.TempDir()
	var otherOut bytes.Buffer
	_, _ = LoadOrEnrollClientIdentity(otherDir, strings.NewReader(""), &otherOut)
	otherEnrollment, _, err := server.EnrollClient(publicKeyFromPrompt(t, otherOut.String()), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	client, err := LoadOrEnrollClientIdentity(clientDir, strings.NewReader("garbage\n"+otherEnrollment+"\n"+enrollment+"\n"), &out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "not base64") || !strings.Contains(out.String(), "another key") {
		t.Fatalf("bad pastes were not reported:\n%s", out.String())
	}
	if client.Credentials.Certificate.ID != certificate.ID || client.Enrollment.ServerID != server.Credentials.Certificate.ID {
		t.Fatal("enrollment does not name this client and server")
	}
	if !client.Enrollment.RootPublicKey.Equal(server.CAKey.Public().(ed25519.PublicKey)) {
		t.Fatal("enrollment names another root")
	}

	// next client start loads without asking
	client2, err := LoadOrEnrollClientIdentity(clientDir, strings.NewReader(""), &out)
	if err != nil || client2.Credentials.Certificate.ID != certificate.ID {
		t.Fatalf("second client start: %v", err)
	}

	// and the two can handshake
	streamKey, _ := wire.NewStreamKey()
	var serverHandshake = &wire.CertificateServerHandshake{Credentials: server.Credentials, Verifier: server.Verifier(), StreamKey: streamKey}
	var clientHandshake = &wire.CertificateClientHandshake{Credentials: client.Credentials, Verifier: client.Verifier(), ServerID: client.Enrollment.ServerID}
	hello, _ := clientHandshake.NewClientHello()
	serverHello, _, err := serverHandshake.AnswerClientHello(hello, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := clientHandshake.FinishServerHello(serverHello, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// publicKeyFromPrompt finds the base64 public key in the client's first-start
// output: the line after the "send this public key" line and a blank one.
func publicKeyFromPrompt(t *testing.T, output string) string {
	t.Helper()
	var lines = strings.Split(output, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "send this public key") && i+2 < len(lines) {
			return lines[i+2]
		}
	}
	t.Fatalf("no public key in:\n%s", output)
	return ""
}
