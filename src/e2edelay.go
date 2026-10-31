package src

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/imakiri/stream/src/wire"
)

// e2eDelayReportInterval is how often a client logs the end-to-end delay of
// the stream, from the moment a chunk's first byte reached the server to the
// moment the client has the chunk restored and sends it to the player, and
// the chunks lost meanwhile.
const e2eDelayReportInterval = 5 * time.Second

// e2eDelays collects the end-to-end delays of the chunks played since the
// last report.
type e2eDelays struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (d *e2eDelays) add(delay time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.samples = append(d.samples, delay)
}

// take returns the samples since the last take, sorted.
func (d *e2eDelays) take() []time.Duration {
	d.mu.Lock()
	var samples = d.samples
	d.samples = nil
	d.mu.Unlock()
	slices.Sort(samples)
	return samples
}

// newClientReport summarizes the sorted delay samples of the chunks played
// in one report interval and the chunks lost in the same time (see
// codec.Decoder.Losses).
func newClientReport(samples []time.Duration, lost, incomplete uint64) wire.ClientReport {
	var report = wire.ClientReport{Chunks: uint32(len(samples)), Lost: uint32(lost), Incomplete: uint32(incomplete)}
	if len(samples) > 0 {
		var percentile = func(p int) time.Duration {
			return samples[min(len(samples)-1, len(samples)*p/100)]
		}
		report.DelayP50, report.DelayP99, report.DelayMax = percentile(50), percentile(99), samples[len(samples)-1]
	}
	return report
}

// formatStreamReport describes a client report; the client logs it, and the
// server logs the reports of its clients.
func formatStreamReport(r wire.ClientReport) string {
	if r.Chunks == 0 && r.Lost == 0 {
		return "no stream data"
	}
	var report = fmt.Sprintf("e2e delay p50 %s, p99 %s, max %s over %d chunks, lost %d",
		r.DelayP50.Round(time.Millisecond), r.DelayP99.Round(time.Millisecond), r.DelayMax.Round(time.Millisecond), r.Chunks, r.Lost)
	if r.Incomplete > 0 {
		report += fmt.Sprintf(" (%d came short of packets)", r.Incomplete)
	}
	return report
}
