package ntpclock

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/beevik/ntp"
)

// answer is a valid NTP response with the given clock offset and round trip.
func answer(offset, roundTrip time.Duration) *ntp.Response {
	var now = time.Now()
	return &ntp.Response{
		Time:          now,
		ReferenceTime: now,
		Stratum:       2,
		ClockOffset:   offset,
		RTT:           roundTrip,
		RootDistance:  roundTrip/2 + time.Millisecond, // as ntp computes it, with a server 1ms off the reference
	}
}

// fakeClock returns a clock whose queries take the answers in turn; a nil
// answer is a failed query.
func fakeClock(answers ...*ntp.Response) *Clock {
	var c = New("ntp.test")
	c.query = func(string) (*ntp.Response, error) {
		if len(answers) == 0 {
			return nil, errors.New("no more answers")
		}
		var next = answers[0]
		answers = answers[1:]
		if next == nil {
			return nil, errors.New("timeout")
		}
		return next, nil
	}
	return c
}

func captureLog(t *testing.T) *bytes.Buffer {
	var buf bytes.Buffer
	var previous = log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &buf
}

// near reports whether got is within a few milliseconds of want: the test
// runs between the moments the clock and the test read time.Now.
func near(got, want time.Time) bool {
	var d = got.Sub(want)
	return d > -5*time.Millisecond && d < 5*time.Millisecond
}

func TestUnsyncedIsTheLocalClock(t *testing.T) {
	captureLog(t)
	for name, c := range map[string]*Clock{"nil": nil, "off": New(""), "before the first sync": New("ntp.test")} {
		now, synced := c.Now()
		if synced || !near(now, time.Now()) {
			t.Errorf("%s: %s, synced %v", name, now, synced)
		}
		if _, ok := c.Error(); ok {
			t.Errorf("%s: has an error bound", name)
		}
	}
}

func TestSyncTakesTheShortestRoundTrip(t *testing.T) {
	var logged = captureLog(t)
	var c = fakeClock(
		answer(300*time.Millisecond, 80*time.Millisecond),
		answer(250*time.Millisecond, 20*time.Millisecond),
		answer(200*time.Millisecond, 60*time.Millisecond),
	)
	if !c.sync() {
		t.Fatal("sync failed")
	}
	now, synced := c.Now()
	if !synced || !near(now, time.Now().Add(250*time.Millisecond)) {
		t.Fatalf("now %s, synced %v; want the local clock + 250ms", now, synced)
	}
	if bound, _ := c.Error(); bound != 11*time.Millisecond {
		t.Errorf("error bound %s, want the root distance, 11ms", bound)
	}
	if !strings.Contains(logged.String(), "this clock minus true time -250ms ± 11ms") {
		t.Errorf("log: %s", logged)
	}
}

// TestTimeGoesByTheMonotonicClock checks that the time between syncs is
// counted from the anchor's monotonic reading, which a step of the wall
// clock does not move.
func TestTimeGoesByTheMonotonicClock(t *testing.T) {
	captureLog(t)
	var c = fakeClock(answer(time.Hour, time.Millisecond))
	c.sync()
	var local = time.Now()
	trueTime, _ := c.At(local)
	later, _ := c.At(local.Add(time.Minute))
	if later.Sub(trueTime) != time.Minute {
		t.Fatalf("a minute later by the local clock is %s later by the clock", later.Sub(trueTime))
	}
}

func TestFailedSyncKeepsTheTime(t *testing.T) {
	var logged = captureLog(t)
	var c = fakeClock(nil, nil, nil, nil, nil, nil)
	if c.sync() || c.sync() {
		t.Fatal("sync succeeded without answers")
	}
	if strings.Count(logged.String(), "does not answer") != 1 {
		t.Errorf("the failure is not logged once: %s", logged)
	}
	if _, synced := c.Now(); synced {
		t.Fatal("synced without answers")
	}

	c = fakeClock(answer(time.Second, time.Millisecond), nil, nil, nil, nil, nil)
	if !c.sync() {
		t.Fatal("one answer of three must do")
	}
	logged.Reset()
	if c.sync() {
		t.Fatal("sync succeeded without answers")
	}
	now, synced := c.Now()
	if !synced || !near(now, time.Now().Add(time.Second)) {
		t.Fatalf("now %s, synced %v after a failed sync; want the last sync's time", now, synced)
	}
	if !strings.Contains(logged.String(), "the time goes on from the last sync") {
		t.Errorf("log: %s", logged)
	}
}

func TestSyncLogsSteps(t *testing.T) {
	var logged = captureLog(t)
	var c = fakeClock(
		answer(100*time.Millisecond, time.Millisecond), nil, nil,
		answer(105*time.Millisecond, time.Millisecond), nil, nil,
		answer(160*time.Millisecond, time.Millisecond), nil, nil,
	)
	c.sync()
	c.sync()
	if strings.Contains(logged.String(), "moved the time") {
		t.Errorf("a 5ms move is logged: %s", logged)
	}
	c.sync()
	if !strings.Contains(logged.String(), "moved the time by 55ms") {
		t.Errorf("a 55ms move is not logged: %s", logged)
	}
}

func TestInvalidAnswerIsAFailure(t *testing.T) {
	captureLog(t)
	var kissOfDeath = answer(time.Second, time.Millisecond)
	kissOfDeath.Stratum = 0
	var c = fakeClock(kissOfDeath, kissOfDeath, kissOfDeath)
	if c.sync() {
		t.Fatal("an invalid answer synced the clock")
	}
}
