// Package liveoutput keeps status lines, such as the stream speed, at the
// bottom of a terminal under the scrolling log.
package liveoutput

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/gosuri/uilive"
)

// Output is a terminal with status lines under the log. A log line goes above
// the status lines and they are drawn again under it, so they stay on screen
// until replaced.
type Output struct {
	mu          sync.Mutex
	terminal    *uilive.Writer
	statusShown bool // false when out is a file or a pipe, e.g. the journal of a service
	statusOrder []string
	statusLines map[string]string
}

// New makes an Output that writes to out, usually os.Stdout. When out is a
// file or a pipe rather than a terminal, only the log goes there: status lines
// redrawn every second would flood a log file or the journal.
func New(out io.Writer) *Output {
	var terminal = uilive.New()
	terminal.Out = out
	return &Output{terminal: terminal, statusShown: !isFileOrPipe(out), statusLines: make(map[string]string)}
}

func isFileOrPipe(out io.Writer) bool {
	file, ok := out.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice == 0
}

// SetStatus replaces the status line called name, or adds it under the
// others. The line has no trailing newline.
func (o *Output) SetStatus(name, line string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.statusLines[name]; !ok {
		o.statusOrder = append(o.statusOrder, name)
	}
	o.statusLines[name] = line
	o.drawStatus()
}

// Log returns the writer for log lines, for log.SetOutput.
func (o *Output) Log() io.Writer {
	return logWriter{output: o}
}

// drawStatus replaces the status lines on the terminal; o.mu is held.
func (o *Output) drawStatus() {
	if !o.statusShown {
		return
	}
	for _, name := range o.statusOrder {
		_, _ = fmt.Fprintln(o.terminal, o.statusLines[name])
	}
	_ = o.terminal.Flush()
}

type logWriter struct {
	output *Output
}

// Write clears the status lines, writes the log line in their place and
// draws them again under it.
func (w logWriter) Write(p []byte) (int, error) {
	w.output.mu.Lock()
	defer w.output.mu.Unlock()
	n, err := w.output.terminal.Bypass().Write(p)
	w.output.drawStatus()
	return n, err
}
