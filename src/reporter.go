package src

import (
	"fmt"

	"github.com/imakiri/stream/src/liveoutput"
)

// Reporter shows the speed an observer measured as a status line.
type Reporter struct {
	name   string
	output *liveoutput.Output
}

// NewReporter shows the speed on the status line called name, e.g. "in".
func NewReporter(output *liveoutput.Output, name string) *Reporter {
	return &Reporter{name: name, output: output}
}

func (r *Reporter) Report(total, delta uint64) {
	r.output.SetStatus(r.name, fmt.Sprintf("[%-3s] %6d kbit/s, total %6d mbit", r.name, delta*8/(1<<10), total*8/(1<<20)))
}
