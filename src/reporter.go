package src

import (
	"fmt"
	"io"
)

// Reporter prints the speed an observer measured, one line per report.
type Reporter struct {
	header string
	output io.Writer
}

func NewReporter(output io.Writer, header string) *Reporter {
	return &Reporter{header: header, output: output}
}

func (r *Reporter) Report(total, delta uint64) {
	_, _ = fmt.Fprintf(r.output, "[%s] delta: %6d kbit/sec, total: %6d mbit\n", r.header, delta*8/(1<<10), total*8/(1<<20))
}
