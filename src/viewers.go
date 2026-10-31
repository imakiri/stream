package src

import (
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/liveoutput"
	"github.com/imakiri/stream/src/wire"
)

// ClientReportInterval is how often a client reports to the server how the
// stream reaches it; it is the window of the client's own delay log line,
// so the two agree.
const ClientReportInterval = e2eDelayReportInterval

// viewerTimeout is how long after its last report a client still counts as
// watching: three reports may be lost before the server calls it gone.
const viewerTimeout = 3*ClientReportInterval + time.Second

// viewerSummaryInterval is how often the server logs the list of viewers,
// for a log file or the journal, where the status line is not shown.
const viewerSummaryInterval = time.Minute

// reportKeysPerViewer is how many report keys of a client's latest
// handshakes the server keeps: a client that repeats its hello takes one of
// the answers, see wire.ReportKeyID.
const reportKeysPerViewer = 4

// viewerForgetTime is how long the server keeps a client that does not
// watch: an older client, which does not report, is listed that long after
// its handshake.
const viewerForgetTime = time.Hour

// viewer is what the server knows of one client.
type viewer struct {
	id           uuid.UUID
	reportKeys   []wire.ClientReportOpener // newest last
	handshakeAt  time.Time
	lastReportAt time.Time // zero before the first report
	lastReport   wire.ClientReport
	watching     bool // reported within viewerTimeout, as of the last check
}

// lastSeen is the time of the client's last handshake or report.
func (client *viewer) lastSeen() time.Time {
	if client.lastReportAt.After(client.handshakeAt) {
		return client.lastReportAt
	}
	return client.handshakeAt
}

// viewers keeps track of the clients from their handshakes and reports, logs
// when one starts and stops watching and shows the list on a status line.
type viewers struct {
	output *liveoutput.Output // nil: log only

	mu      sync.Mutex
	viewers map[uuid.UUID]*viewer
}

func newViewers(output *liveoutput.Output) *viewers {
	return &viewers{output: output, viewers: make(map[uuid.UUID]*viewer)}
}

// handshake records an answered client hello and the report key it gave.
func (v *viewers) handshake(clientID uuid.UUID, reportKey wire.ClientReportOpener, now time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	var client = v.viewers[clientID]
	if client == nil {
		client = &viewer{id: clientID}
		v.viewers[clientID] = client
	}
	client.handshakeAt = now
	client.reportKeys = append(client.reportKeys, reportKey)
	if len(client.reportKeys) > reportKeysPerViewer {
		client.reportKeys = slices.Delete(client.reportKeys, 0, len(client.reportKeys)-reportKeysPerViewer)
	}
}

// report opens a client report and records it.
func (v *viewers) report(packet []byte, now time.Time) error {
	clientID, ok := wire.ClientReportClientID(packet)
	if !ok {
		return wire.ErrInvalidClientReport
	}
	keyID, _ := wire.ClientReportKeyID(packet)
	v.mu.Lock()
	defer v.mu.Unlock()
	var client = v.viewers[clientID]
	if client == nil {
		return fmt.Errorf("%w: from %s, which has not handshaked", wire.ErrInvalidClientReport, clientID)
	}
	var index = slices.IndexFunc(client.reportKeys, func(key wire.ClientReportOpener) bool { return key.KeyID() == keyID })
	if index < 0 {
		return fmt.Errorf("%w: from %s with an unknown report key", wire.ErrInvalidClientReport, clientID)
	}
	report, err := client.reportKeys[index].OpenClientReport(packet)
	if err != nil {
		return err
	}
	// the client took this answer: older keys are of no use any more
	client.reportKeys = slices.Delete(client.reportKeys, 0, index)
	client.lastReport, client.lastReportAt = report, now
	if !client.watching {
		client.watching = true
		log.Printf("client %s watches: %s", client.id, formatStreamReport(report))
	}
	v.showStatus()
	return nil
}

// check notes the clients that stopped reporting and forgets the clients
// gone for long; it runs every few seconds.
func (v *viewers) check(now time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for id, client := range v.viewers {
		if client.watching && now.Sub(client.lastReportAt) > viewerTimeout {
			client.watching = false
			log.Printf("client %s stopped reporting %s ago: gone or cut off", client.id, now.Sub(client.lastReportAt).Round(time.Second))
		}
		if !client.watching && now.Sub(client.lastSeen()) > viewerForgetTime {
			delete(v.viewers, id)
		}
	}
	v.showStatus()
}

// summary is a line with every client that watches and every client that
// handshaked but sends no reports; hasViewers is false when there are none.
func (v *viewers) summary(now time.Time) (line string, hasViewers bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.summaryLocked(now), len(v.viewers) > 0
}

func (v *viewers) summaryLocked(now time.Time) string {
	var watching, silent []string
	for _, client := range v.viewers {
		switch {
		case client.watching:
			watching = append(watching, fmt.Sprintf("%s %s", shortID(client.id), formatStreamReport(client.lastReport)))
		case client.lastReportAt.IsZero():
			// an older client, which handshakes only at start and does not
			// report, or one that left right after its handshake
			silent = append(silent, fmt.Sprintf("%s %s ago", shortID(client.id), now.Sub(client.handshakeAt).Round(time.Minute)))
		}
	}
	slices.Sort(watching)
	slices.Sort(silent)
	var line = fmt.Sprintf("%d clients watch", len(watching))
	if len(watching) > 0 {
		line += ": " + strings.Join(watching, "; ")
	}
	if len(silent) > 0 {
		line += fmt.Sprintf("; handshaked, no reports (an older client, or gone): %s", strings.Join(silent, ", "))
	}
	return line
}

// showStatus puts the summary on the status line; v.mu is held.
func (v *viewers) showStatus() {
	if v.output != nil {
		v.output.SetStatus("clients", v.summaryLocked(time.Now()))
	}
}

func shortID(id uuid.UUID) string {
	return id.String()[:8]
}
