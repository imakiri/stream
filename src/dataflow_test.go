package src

import (
	"strings"
	"testing"
	"time"
)

// TestDataFlowLogsChangesOnce walks a client whose server has no input at
// first, then streams, then stops: each change is told once, a steady state
// never.
func TestDataFlowLogsChangesOnce(t *testing.T) {
	var start = time.Unix(1_000_000, 0)
	var at = func(seconds int) time.Time { return start.Add(time.Duration(seconds) * time.Second) }
	var flow dataFlow

	var steps = []struct {
		lastData time.Time // zero: no data yet
		now      time.Time
		want     string // a part of the line, "" for no line
	}{
		{time.Time{}, at(5), "no data from the server yet"},
		{time.Time{}, at(10), ""},
		{at(14), at(15), "data from the server is coming"},
		{at(20), at(20), ""},
		{at(20), at(25), "no data from the server since"},
		{at(20), at(30), ""},
		{at(34), at(35), "again after 15s"},
	}
	for i, step := range steps {
		var lastDataUnixNano int64
		if !step.lastData.IsZero() {
			lastDataUnixNano = step.lastData.UnixNano()
		}
		var got = flow.update(lastDataUnixNano, step.now)
		if step.want == "" && got != "" || !strings.Contains(got, step.want) {
			t.Fatalf("step %d: got %q, want %q", i, got, step.want)
		}
	}

	// data from the very start is not announced: the delay lines show it
	var fromStart dataFlow
	if got := fromStart.update(at(4).UnixNano(), at(5)); got != "" {
		t.Fatalf("got %q for data from the start", got)
	}
}
