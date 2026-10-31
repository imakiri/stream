package src

import (
	"testing"
	"time"
)

// fakeClock is a clock that moves only when the pacer sleeps or the test
// says so.
type fakeClock struct{ now time.Time }

func (c *fakeClock) sleep(d time.Duration) { c.now = c.now.Add(d) }

func newTestPacer(bitsPerSecond uint64, clock *fakeClock) *pacer {
	var p = newPacer(bitsPerSecond)
	p.sleep, p.now = clock.sleep, func() time.Time { return clock.now }
	return p
}

// TestPacerSpreadsABurst hands the pacer an interleave group at once: it
// must take the time the rate gives, with at most a millisecond of packets
// going back to back.
func TestPacerSpreadsABurst(t *testing.T) {
	const packets, size, rate = 40, 1086, 30_000_000
	var clock = &fakeClock{now: time.Unix(1_000_000, 0)}
	var p = newTestPacer(rate, clock)
	var start = clock.now
	var gap = time.Duration(size * 8 * int64(time.Second) / rate)

	var sentAt = make(map[time.Time]int)
	for range packets {
		p.wait(size)
		sentAt[clock.now]++
	}
	var took = clock.now.Sub(start)
	var want = time.Duration(packets-1) * gap
	if took < want-pacerBurst || took > want {
		t.Fatalf("the burst took %s, want about %s", took, want)
	}
	var maxTogether = int(pacerBurst/gap) + 2
	for at, n := range sentAt {
		if n > maxTogether {
			t.Fatalf("%d packets at %s, want at most %d together", n, at.Sub(start), maxTogether)
		}
	}
}

// TestPacerDoesNotSaveUpIdleTime checks that after a pause the next burst is
// paced again rather than sent at once on time saved during the pause.
func TestPacerDoesNotSaveUpIdleTime(t *testing.T) {
	var clock = &fakeClock{now: time.Unix(1_000_000, 0)}
	var p = newTestPacer(30_000_000, clock)
	p.wait(1086)
	clock.now = clock.now.Add(time.Second)

	var start = clock.now
	for range 40 {
		p.wait(1086)
	}
	if took := clock.now.Sub(start); took < 10*time.Millisecond {
		t.Fatalf("the burst after a pause took %s: the pause was saved up", took)
	}
}

func TestPacerOff(t *testing.T) {
	var p = newPacer(0)
	p.wait(1086) // a nil pacer does not wait
	if p != nil {
		t.Fatal("rate 0 must turn pacing off")
	}
}
