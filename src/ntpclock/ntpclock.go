// Package ntpclock keeps the true time (UTC) from an NTP server, so the stream
// server and its clients compare the same clock: the server stamps the
// chunks with it and a client measures the end-to-end delay against it,
// without estimating one clock from the other over the relay.
//
// Between queries the time goes by the monotonic clock, which a step of the
// wall clock (a time sync) does not move; the queries, every QueryInterval,
// take out the drift of the monotonic clock, which on Windows is not steered
// by time sync and drifts with the crystal, tens of milliseconds an hour.
package ntpclock

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/beevik/ntp"
)

const (
	// QueryInterval is how often the clock asks the NTP server.
	QueryInterval = 2 * time.Minute
	// retryInterval is how soon the clock asks again after a failed query.
	retryInterval = 15 * time.Second
	// queriesPerSync is how many queries make one sync: the one with the
	// shortest round trip, the least queued, gives the time.
	queriesPerSync = 3
	queryTimeout   = 3 * time.Second
	// reportedStep is how far a sync must move the time from what the clock
	// expected to be logged.
	reportedStep = 20 * time.Millisecond
)

// anchor ties the true time to a moment of the monotonic clock.
type anchor struct {
	utc   time.Time     // the true time, no monotonic reading
	local time.Time     // the same moment by time.Now, with the monotonic reading
	error time.Duration // how far utc may be off: the root distance, half the round trip plus the server's own error
}

// at returns the true time at local, a reading of time.Now.
func (a anchor) at(local time.Time) time.Time {
	return a.utc.Add(local.Sub(a.local))
}

// Clock is the true time by an NTP server. A nil Clock, or one with no
// server, is never synced and gives the local clock.
type Clock struct {
	server string
	query  func(server string) (*ntp.Response, error)

	mu       sync.Mutex
	anchor   *anchor // nil until the first sync
	failing  bool    // the last sync failed and that was logged
	lastSync time.Time
}

// New makes a clock that asks server ("host" or "host:port"); "" turns NTP
// off.
func New(server string) *Clock {
	return &Clock{server: server, query: func(server string) (*ntp.Response, error) {
		return ntp.QueryWithOptions(server, ntp.QueryOptions{Timeout: queryTimeout})
	}}
}

// Run syncs the clock now and then every QueryInterval until ctx is done.
func (c *Clock) Run(ctx context.Context) {
	if c == nil || c.server == "" {
		log.Printf("clock: ntp is off")
		return
	}
	for {
		var wait = QueryInterval
		if !c.sync() {
			wait = retryInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// sync queries the server and moves the clock to the answer with the
// shortest round trip; it returns whether any query succeeded.
func (c *Clock) sync() bool {
	var best *ntp.Response
	var bestAt time.Time
	var lastErr error
	for range queriesPerSync {
		response, err := c.query(c.server)
		var receivedAt = time.Now()
		if err == nil {
			err = response.Validate()
		}
		if err != nil {
			lastErr = err
			continue
		}
		if best == nil || response.RTT < best.RTT {
			best, bestAt = response, receivedAt
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if best == nil {
		if !c.failing {
			c.failing = true
			if c.anchor != nil {
				log.Printf("clock: ntp %s does not answer: %s; the time goes on from the last sync", c.server, lastErr)
			} else {
				log.Printf("clock: ntp %s does not answer: %s; using the local clock", c.server, lastErr)
			}
		}
		return false
	}
	// ClockOffset is the true time minus the wall clock at the query; the
	// wall clock did not step since, a few milliseconds ago
	var next = &anchor{
		utc:   bestAt.Round(0).Add(best.ClockOffset),
		local: bestAt,
		error: best.RootDistance,
	}
	switch {
	case c.anchor == nil || c.failing:
		log.Printf("clock: ntp %s, this clock minus true time %s ± %s", c.server,
			(-best.ClockOffset).Round(time.Millisecond), next.error.Round(time.Millisecond))
	default:
		var step = next.utc.Sub(c.anchor.at(bestAt))
		if step > reportedStep || step < -reportedStep {
			log.Printf("clock: ntp %s moved the time by %s in %s (drift of the local clock or a step of the ntp time)",
				c.server, step.Round(time.Millisecond), bestAt.Sub(c.lastSync).Round(time.Second))
		}
	}
	c.anchor, c.failing, c.lastSync = next, false, bestAt
	return true
}

// Now returns the true time and whether it comes from NTP; before the first
// sync, or with NTP off, it is the local clock.
func (c *Clock) Now() (time.Time, bool) {
	return c.At(time.Now())
}

// At returns the true time at local, a reading of time.Now with its
// monotonic part, and whether it comes from NTP; before the first sync, or
// with NTP off, it is local itself.
func (c *Clock) At(local time.Time) (time.Time, bool) {
	var a = c.current()
	if a == nil {
		return local, false
	}
	return a.at(local), true
}

// Error returns how far the time from Now may be off, if it comes from NTP.
func (c *Clock) Error() (time.Duration, bool) {
	var a = c.current()
	if a == nil {
		return 0, false
	}
	return a.error, true
}

func (c *Clock) current() *anchor {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.anchor
}

// String describes the clock for the log.
func (c *Clock) String() string {
	if c == nil || c.server == "" {
		return "local clock"
	}
	return fmt.Sprintf("ntp %s", c.server)
}
