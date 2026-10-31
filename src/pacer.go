package src

import "time"

// DefaultPacingRate is the server's send rate for the packets of a burst,
// about three times the wire rate of an 8 Mbit/s video with FEC 8/10.
const DefaultPacingRate = 30_000_000 // bits per second

// pacerBurst is how far a pacer lets the packets run ahead of their schedule
// before it sleeps: sleeping for one packet gap (0.3 ms at 30 Mbit/s) would
// overshoot by the timer slack, so the pacer sleeps in steps of this size and
// sends the few packets of a step back to back.
const pacerBurst = time.Millisecond

// pacer spreads packets over time at a fixed rate. The encoder hands over
// the packets of a whole interleave group at once (40 packets with the
// default settings); sent back to back they leave at the speed of the
// network card and a shaper on the way cuts off the tail of the burst,
// more packets of a chunk than FEC can restore. The relay forwards each
// packet as it comes, so pacing at the server smooths both legs.
type pacer struct {
	bitsPerSecond uint64
	next          time.Time // when the next packet is due
	sleep         func(time.Duration)
	now           func() time.Time
}

// newPacer returns a pacer for bitsPerSecond, or nil for 0: no pacing.
func newPacer(bitsPerSecond uint64) *pacer {
	if bitsPerSecond == 0 {
		return nil
	}
	return &pacer{bitsPerSecond: bitsPerSecond, sleep: time.Sleep, now: time.Now}
}

// wait blocks until a packet of size bytes may go. A pacer that fell behind
// (the stream was idle) does not save the time up for a later burst.
func (p *pacer) wait(size int) {
	if p == nil {
		return
	}
	var now = p.now()
	if p.next.Before(now) {
		p.next = now
	}
	if ahead := p.next.Sub(now); ahead > pacerBurst {
		p.sleep(ahead)
	}
	p.next = p.next.Add(time.Duration(uint64(size) * 8 * uint64(time.Second) / p.bitsPerSecond))
}
