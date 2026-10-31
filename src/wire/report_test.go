package wire

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// reportHandshake runs a handshake and returns both ends of the report key.
func reportHandshake(t testing.TB, pki testPKI, pair handshakePair) (ClientReportSealer, ClientReportOpener) {
	hello, err := pair.client.NewClientHello()
	if err != nil {
		t.Fatal(err)
	}
	serverHello, _, opener, err := pair.server.AnswerClientHello(hello, now)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err := pair.client.FinishServerHello(serverHello, now)
	if err != nil {
		t.Fatal(err)
	}
	return session.Reports, opener
}

var testReport = ClientReport{
	DelayP50:   121 * time.Millisecond,
	DelayP99:   238 * time.Millisecond,
	DelayMax:   282500 * time.Microsecond,
	Chunks:     768,
	Lost:       3,
	Incomplete: 1,
}

func TestClientReportRoundTrip(t *testing.T) {
	var pki = newTestPKI(t)
	var clientCreds = pki.issue(t, nextYear)
	var pair = newHandshakePair(t, pki, pki.issue(t, nextYear), clientCreds)
	sealer, opener := reportHandshake(t, pki, pair)

	var packet = sealer.SealClientReport(testReport)
	if len(packet) != ClientReportSize || PacketKind(packet) != KindClientReport {
		t.Fatalf("report of %d bytes, kind %d", len(packet), PacketKind(packet))
	}
	if id, ok := ClientReportClientID(packet); !ok || id != clientCreds.Certificate.ID {
		t.Fatalf("report says it is from %s", id)
	}
	if keyID, ok := ClientReportKeyID(packet); !ok || keyID != opener.KeyID() {
		t.Fatal("the report key ID differs from the opener's")
	}
	report, err := opener.OpenClientReport(packet)
	if err != nil {
		t.Fatal(err)
	}
	if report != testReport {
		t.Fatalf("got %+v, want %+v", report, testReport)
	}
	if bytes.Contains(packet, testReport.marshal()) {
		t.Fatal("the report is in the clear")
	}

	// a client whose estimate of the server clock is off reports negative
	// delays; they must come through as such, not as zero
	var skewed = ClientReport{DelayP50: -40 * time.Millisecond, DelayP99: -2 * time.Millisecond, DelayMax: 5 * time.Millisecond, Chunks: 10}
	if report, err = opener.OpenClientReport(sealer.SealClientReport(skewed)); err != nil || report != skewed {
		t.Fatalf("got %+v, %v, want %+v", report, err, skewed)
	}
}

// TestClientReportKeyOfTheAnswerTaken answers the same client hello twice,
// as happens while a client without a stream repeats its hello; the client
// takes the first answer, and its reports must carry the ID of that answer's
// key, not of the server's latest one.
func TestClientReportKeyOfTheAnswerTaken(t *testing.T) {
	var pki = newTestPKI(t)
	var pair = newHandshakePair(t, pki, pki.issue(t, nextYear), pki.issue(t, nextYear))
	hello, _ := pair.client.NewClientHello()
	firstAnswer, _, firstOpener, err := pair.server.AnswerClientHello(hello, now)
	if err != nil {
		t.Fatal(err)
	}
	_, _, secondOpener, err := pair.server.AnswerClientHello(hello, now)
	if err != nil {
		t.Fatal(err)
	}
	if firstOpener.KeyID() == secondOpener.KeyID() {
		t.Fatal("two answers have the same report key ID")
	}
	session, _, err := pair.client.FinishServerHello(firstAnswer, now)
	if err != nil {
		t.Fatal(err)
	}
	var packet = session.Reports.SealClientReport(testReport)
	if keyID, _ := ClientReportKeyID(packet); keyID != firstOpener.KeyID() {
		t.Fatal("the report names another key than the answer the client took")
	}
	if _, err := secondOpener.OpenClientReport(packet); !errors.Is(err, ErrInvalidClientReport) {
		t.Fatalf("the other answer's key: %v", err)
	}
	if _, err := firstOpener.OpenClientReport(packet); err != nil {
		t.Fatal(err)
	}
}

func TestClientReportRejected(t *testing.T) {
	var pki = newTestPKI(t)
	var serverCreds = pki.issue(t, nextYear)
	var pair = newHandshakePair(t, pki, serverCreds, pki.issue(t, nextYear))
	sealer, opener := reportHandshake(t, pki, pair)
	var other = newHandshakePair(t, pki, serverCreds, pki.issue(t, nextYear))
	otherSealer, otherOpener := reportHandshake(t, pki, other)

	var first = sealer.SealClientReport(testReport)
	var second = sealer.SealClientReport(testReport)
	if _, err := opener.OpenClientReport(second); err != nil {
		t.Fatal(err)
	}
	t.Run("replayed", func(t *testing.T) {
		if _, err := opener.OpenClientReport(second); !errors.Is(err, ErrInvalidClientReport) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("older than the last opened", func(t *testing.T) {
		if _, err := opener.OpenClientReport(first); !errors.Is(err, ErrInvalidClientReport) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("sealed by another client", func(t *testing.T) {
		if _, err := opener.OpenClientReport(otherSealer.SealClientReport(testReport)); !errors.Is(err, ErrInvalidClientReport) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("another client's ID put on it", func(t *testing.T) {
		var packet = sealer.SealClientReport(testReport)
		var otherPacket = otherSealer.SealClientReport(testReport)
		copy(packet[2:18], otherPacket[2:18])
		copy(packet[18:26], otherPacket[18:26])
		if _, err := otherOpener.OpenClientReport(packet); !errors.Is(err, ErrInvalidClientReport) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("changed", func(t *testing.T) {
		var packet = sealer.SealClientReport(testReport)
		for i := range packet {
			var changed = bytes.Clone(packet)
			changed[i] ^= 1
			if _, err := opener.OpenClientReport(changed); err == nil {
				t.Fatalf("a report with byte %d changed opened", i)
			}
		}
	})
}

func FuzzOpenClientReport(f *testing.F) {
	var pki = newTestPKI(f)
	var pair = newHandshakePair(f, pki, pki.issue(f, nextYear), pki.issue(f, nextYear))
	sealer, opener := reportHandshake(f, pki, pair)
	var valid = sealer.SealClientReport(testReport)
	f.Add(valid)
	f.Fuzz(func(t *testing.T, packet []byte) {
		_, _ = ClientReportClientID(packet)
		_, _ = ClientReportKeyID(packet)
		if bytes.Equal(packet, valid) {
			return
		}
		if _, err := opener.OpenClientReport(packet); err == nil {
			t.Fatal("a fuzzed report opened")
		}
	})
}
