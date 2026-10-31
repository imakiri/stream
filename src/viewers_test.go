package src

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/wire"
)

// viewerHandshake is a finished handshake of one client: the client's
// report sealer and what the server got from it, the client ID and the
// report key.
type viewerHandshake struct {
	clientID  uuid.UUID
	sealer    wire.ClientReportSealer
	reportKey wire.ClientReportOpener
}

// newViewerHandshakes runs the handshakes of count clients with one server.
func newViewerHandshakes(t *testing.T, count int) []viewerHandshake {
	var root = newTestRoot(t)
	var serverCreds = root.issue(t)
	var verifier = wire.RootVerifier{Root: root.pub}
	var server = &wire.CertificateServerHandshake{Credentials: serverCreds, Verifier: verifier}
	var handshakes []viewerHandshake
	for range count {
		var clientCreds = root.issue(t)
		var client = &wire.CertificateClientHandshake{Credentials: clientCreds, Verifier: verifier, ServerID: serverCreds.Certificate.ID}
		hello, err := client.NewClientHello()
		if err != nil {
			t.Fatal(err)
		}
		serverHello, clientID, reportKey, err := server.AnswerClientHello(hello, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		session, _, err := client.FinishServerHello(serverHello, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		handshakes = append(handshakes, viewerHandshake{clientID: clientID, sealer: session.Reports, reportKey: reportKey})
	}
	return handshakes
}

func captureLog(t *testing.T) *bytes.Buffer {
	var buf bytes.Buffer
	var previous = log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &buf
}

func TestViewersFollowReports(t *testing.T) {
	var logged = captureLog(t)
	var handshakes = newViewerHandshakes(t, 2)
	var watcher, silent = handshakes[0], handshakes[1]
	var v = newViewers(nil)
	var start = time.Now()
	v.handshake(watcher.clientID, watcher.reportKey, start)
	v.handshake(silent.clientID, silent.reportKey, start)

	var report = wire.ClientReport{DelayP50: 120 * time.Millisecond, DelayP99: 230 * time.Millisecond, DelayMax: 280 * time.Millisecond, Chunks: 750, Lost: 2}
	if err := v.report(watcher.sealer.SealClientReport(report), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logged.String(), "client "+watcher.clientID.String()+" watches: e2e delay p50 120ms") {
		t.Errorf("log: %s", logged)
	}
	var summary, _ = v.summary(start.Add(2 * time.Second))
	if !strings.HasPrefix(summary, "1 clients watch: "+shortID(watcher.clientID)+" e2e delay p50 120ms") ||
		!strings.Contains(summary, "no reports (an older client, or gone): "+shortID(silent.clientID)) {
		t.Errorf("summary: %s", summary)
	}

	v.check(start.Add(viewerTimeout + 2*time.Second))
	if !strings.Contains(logged.String(), "client "+watcher.clientID.String()+" stopped reporting") {
		t.Errorf("log: %s", logged)
	}
	if summary, _ = v.summary(start.Add(viewerTimeout + 2*time.Second)); !strings.HasPrefix(summary, "0 clients watch") {
		t.Errorf("summary after the reports stopped: %s", summary)
	}

	v.check(start.Add(viewerForgetTime + time.Minute))
	if _, hasViewers := v.summary(start.Add(viewerForgetTime + time.Minute)); hasViewers {
		t.Error("the clients are kept after viewerForgetTime")
	}
}

func TestViewersRejectReports(t *testing.T) {
	captureLog(t)
	var handshakes = newViewerHandshakes(t, 2)
	var v = newViewers(nil)
	var now = time.Now()
	var report = wire.ClientReport{Chunks: 1}

	// from a client the server never answered
	if err := v.report(handshakes[0].sealer.SealClientReport(report), now); !errors.Is(err, wire.ErrInvalidClientReport) {
		t.Fatalf("report before the handshake: %v", err)
	}
	v.handshake(handshakes[0].clientID, handshakes[0].reportKey, now)
	// with the key of another handshake: the second client's sealer under
	// the first client's ID cannot open
	var foreign = handshakes[1].sealer.SealClientReport(report)
	copy(foreign[2:18], handshakes[0].clientID.Bytes())
	if err := v.report(foreign, now); !errors.Is(err, wire.ErrInvalidClientReport) {
		t.Fatalf("report with another key: %v", err)
	}
	var packet = handshakes[0].sealer.SealClientReport(report)
	if err := v.report(packet, now); err != nil {
		t.Fatal(err)
	}
	if err := v.report(packet, now); !errors.Is(err, wire.ErrInvalidClientReport) {
		t.Fatalf("replayed report: %v", err)
	}
}

// TestViewersKeepTheKeyTheClientTook answers a client hello twice, as while
// a client without a stream repeats it; the client takes the first answer,
// and its reports must still open after the second answer.
func TestViewersKeepTheKeyTheClientTook(t *testing.T) {
	captureLog(t)
	var root = newTestRoot(t)
	var serverCreds, clientCreds = root.issue(t), root.issue(t)
	var verifier = wire.RootVerifier{Root: root.pub}
	var server = &wire.CertificateServerHandshake{Credentials: serverCreds, Verifier: verifier}
	var client = &wire.CertificateClientHandshake{Credentials: clientCreds, Verifier: verifier, ServerID: serverCreds.Certificate.ID}
	hello, _ := client.NewClientHello()
	var v = newViewers(nil)
	var now = time.Now()
	firstAnswer, clientID, firstKey, err := server.AnswerClientHello(hello, now)
	if err != nil {
		t.Fatal(err)
	}
	v.handshake(clientID, firstKey, now)
	_, _, secondKey, err := server.AnswerClientHello(hello, now)
	if err != nil {
		t.Fatal(err)
	}
	v.handshake(clientID, secondKey, now)

	session, _, err := client.FinishServerHello(firstAnswer, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.report(session.Reports.SealClientReport(wire.ClientReport{Chunks: 1}), now); err != nil {
		t.Fatal(err)
	}
}
