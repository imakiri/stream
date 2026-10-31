package liveoutput

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// TestStatusStaysUnderTheLog checks that a log line does not take the status
// lines off the screen: they are drawn again after it, newest values, in the
// order they were first set.
func TestStatusStaysUnderTheLog(t *testing.T) {
	var out bytes.Buffer
	var output = New(&out)
	var logger = log.New(output.Log(), "", 0)

	output.SetStatus("in", "in 1")
	output.SetStatus("out", "out 1")
	output.SetStatus("in", "in 2")
	out.Reset()
	logger.Print("handshake")

	var printed = out.String()
	var logAt, inAt, outAt = strings.Index(printed, "handshake\n"), strings.Index(printed, "in 2\n"), strings.Index(printed, "out 1\n")
	if logAt < 0 || inAt < 0 || outAt < 0 {
		t.Fatalf("missing the log line or a status line after a log line: %q", printed)
	}
	if !(logAt < inAt && inAt < outAt) {
		t.Fatalf("want the log line, then the status lines in their order: %q", printed)
	}
	if strings.Contains(printed, "in 1") {
		t.Fatalf("an old status value was drawn: %q", printed)
	}
}
