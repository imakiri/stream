package src

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// e2eDelayReportInterval is how often a client logs the end-to-end delay of
// the stream: from the moment a chunk's first byte reached the server to the
// moment the client has the chunk restored and sends it to the player.
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

// formatE2EDelays describes sorted samples, which must not be empty.
func formatE2EDelays(samples []time.Duration) string {
	var percentile = func(p int) time.Duration {
		return samples[min(len(samples)-1, len(samples)*p/100)]
	}
	return fmt.Sprintf("e2e delay p50 %s, p99 %s, max %s over %d chunks",
		percentile(50).Round(time.Millisecond), percentile(99).Round(time.Millisecond),
		samples[len(samples)-1].Round(time.Millisecond), len(samples))
}
