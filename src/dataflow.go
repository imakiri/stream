package src

import (
	"fmt"
	"time"
)

// dataFlow tells when the data from the server stops and when it comes
// again, so the log says it once rather than at every handshake.
type dataFlow struct {
	reported  bool // a state was logged
	flowing   bool // the logged state
	stoppedAt time.Time
}

// update takes the time of the latest data packet (unix ns, zero before the
// first) and returns a line for the log when the flow changed, else "".
func (f *dataFlow) update(lastDataUnixNano int64, now time.Time) string {
	var lastData = time.Unix(0, lastDataUnixNano)
	var flowing = lastDataUnixNano != 0 && now.Sub(lastData) < streamStaleTimeout
	if f.reported && flowing == f.flowing {
		return ""
	}
	var wasReported = f.reported
	f.reported, f.flowing = true, flowing
	switch {
	case flowing && !wasReported:
		return "" // came from the start; the e2e delay lines show it
	case flowing && f.stoppedAt.IsZero():
		return "data from the server is coming"
	case flowing:
		return fmt.Sprintf("data from the server again after %s", now.Sub(f.stoppedAt).Round(time.Second))
	case lastDataUnixNano == 0:
		return "no data from the server yet: it may have no input (is OBS sending?); handshaking quietly until it comes"
	default:
		f.stoppedAt = lastData
		return fmt.Sprintf("no data from the server since %s: it may have lost its input; handshaking quietly until it comes", lastData.Format(time.TimeOnly))
	}
}
