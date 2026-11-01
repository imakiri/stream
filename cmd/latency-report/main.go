// Command latency-report lays the delay from a frame on the PC's screen to
// the same frame on the phone's screen out stage by stage, from three files
// taken during one run:
//
//   - the server's latency trace (server -trace FILE),
//   - the client's latency trace (the Android app's "Measure latency":
//     trace-client.tsv in the profile directory),
//   - the app's screen-to-screen readings of player/latency-marker.html
//     (marker.tsv next to it).
//
// Both traces are stamped by NTP time (set [clock] ntp_server on both ends,
// the same server), so the stages of the two machines line up. The part
// before the server (screen capture, OBS encoding) has no stamps of its own:
// it is what remains of the screen-to-screen delay after the measured stages.
//
//	adb shell run-as one.imakiri.stream cat files/profile/trace-client.tsv > trace-client.tsv
//	adb shell run-as one.imakiri.stream cat files/profile/marker.tsv > marker.tsv
//	latency-report -server trace-server.tsv -client trace-client.tsv -marker marker.tsv
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/imakiri/stream/src/latencytrace"
)

// chunkKey names a chunk across both traces.
type chunkKey struct {
	session string
	csn     uint64
}

// chunkEvents is what the traces say about one chunk: the time of each
// chunk-level stage and of each packet at each packet-level stage, unix µs.
type chunkEvents struct {
	chunk   map[latencytrace.Stage]int64
	packets map[latencytrace.Stage]map[int64]int64
}

func (e *chunkEvents) first(stage latencytrace.Stage) (int64, bool) {
	return extreme(e.packets[stage], func(a, b int64) bool { return a < b })
}

func (e *chunkEvents) last(stage latencytrace.Stage) (int64, bool) {
	return extreme(e.packets[stage], func(a, b int64) bool { return a > b })
}

func extreme(times map[int64]int64, better func(a, b int64) bool) (int64, bool) {
	var best int64
	var found bool
	for _, t := range times {
		if !found || better(t, best) {
			best, found = t, true
		}
	}
	return best, found
}

func readTrace(path string, chunks map[chunkKey]*chunkEvents) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	var lines = bufio.NewScanner(file)
	for lines.Scan() {
		var f = strings.Split(lines.Text(), "\t")
		if len(f) != 5 {
			continue
		}
		csn, err1 := strconv.ParseUint(f[2], 10, 64)
		psn, err2 := strconv.ParseInt(f[3], 10, 64)
		at, err3 := strconv.ParseInt(f[4], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		var key = chunkKey{session: f[1], csn: csn}
		var events = chunks[key]
		if events == nil {
			events = &chunkEvents{chunk: map[latencytrace.Stage]int64{}, packets: map[latencytrace.Stage]map[int64]int64{}}
			chunks[key] = events
		}
		var stage = latencytrace.Stage(f[0])
		if psn < 0 {
			events.chunk[stage] = at
			continue
		}
		if psn >= latencytrace.ParityPsnBase {
			// a parity packet of the block that starts with this chunk:
			// not one of the chunk's own packets
			continue
		}
		if events.packets[stage] == nil {
			events.packets[stage] = map[int64]int64{}
		}
		if _, seen := events.packets[stage][psn]; !seen {
			events.packets[stage][psn] = at
		}
	}
	return lines.Err()
}

// markerReading is one screen-to-screen reading of the app.
type markerReading struct {
	seenUs, markerMs int64
	lagS             float64
	hasLag           bool
}

func readMarkers(path string) ([]markerReading, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var readings []markerReading
	var lines = bufio.NewScanner(file)
	for lines.Scan() {
		var f = strings.Split(lines.Text(), "\t")
		if len(f) < 2 {
			continue
		}
		seen, err1 := strconv.ParseInt(f[0], 10, 64)
		marker, err2 := strconv.ParseInt(f[1], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		var r = markerReading{seenUs: seen, markerMs: marker}
		if len(f) > 2 {
			if lag, err := strconv.ParseFloat(f[2], 64); err == nil {
				r.lagS, r.hasLag = lag, true
			}
		}
		readings = append(readings, r)
	}
	return readings, lines.Err()
}

// sample collects durations in ms.
type sample []float64

func (s sample) quantile(q float64) float64 {
	if len(s) == 0 {
		return 0
	}
	var sorted = slices.Clone(s)
	slices.Sort(sorted)
	return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))]
}

// point is a stage of the timeline, as an offset from the chunk's input.
type point struct {
	name    string
	explain string
	at      func(e *chunkEvents) (int64, bool)
}

func chunkStage(stage latencytrace.Stage) func(e *chunkEvents) (int64, bool) {
	return func(e *chunkEvents) (int64, bool) {
		at, ok := e.chunk[stage]
		return at, ok
	}
}

func main() {
	var serverPath = flag.String("server", "trace-server.tsv", "server latency trace")
	var clientPath = flag.String("client", "trace-client.tsv", "client latency trace")
	var markerPath = flag.String("marker", "marker.tsv", "screen-to-screen readings; empty to skip")
	flag.Parse()

	var chunks = map[chunkKey]*chunkEvents{}
	for _, path := range []string{*serverPath, *clientPath} {
		if err := readTrace(path, chunks); err != nil {
			log.Fatalln(err)
		}
	}

	var points = []point{
		{"server: first byte from OBS", "the chunk's time 0", chunkStage(latencytrace.ServerInput)},
		{"server: chunk closed", "filling the chunk (chunk_packets packets) or chunk_max_age without input", chunkStage(latencytrace.ServerChunk)},
		{"server: chunk cut into packets", "data shards; the block's parity follows later", chunkStage(latencytrace.ServerShards)},
		{"server: first packet out of the encoder", "the chunk's data packets go out as it closes", func(e *chunkEvents) (int64, bool) { return e.first(latencytrace.ServerEncoderOut) }},
		{"server: first packet sent", "pacing", func(e *chunkEvents) (int64, bool) { return e.first(latencytrace.ServerSent) }},
		{"server: last packet sent", "the chunk's data packets back to back at the pacing rate", func(e *chunkEvents) (int64, bool) { return e.last(latencytrace.ServerSent) }},
		{"phone: first packet received", "network: PC → relay → phone", func(e *chunkEvents) (int64, bool) { return e.first(latencytrace.ClientReceived) }},
		{"phone: chunk restored", "all its data packets in, or its block restored from parity", chunkStage(latencytrace.ClientRestored)},
		{"phone: chunk to the player", "the decoder puts chunks back in order", chunkStage(latencytrace.ClientOut)},
	}

	// offsets of each point from the chunk's input, over the chunks both
	// traces have from input to the player
	var offsets = make([]sample, len(points))
	var steps = make([]sample, len(points))
	var network sample
	var complete int
	var example *chunkEvents
	var exampleKey chunkKey
	for key, e := range chunks {
		var times = make([]int64, len(points))
		var ok = true
		for i, p := range points {
			if times[i], ok = p.at(e); !ok {
				break
			}
		}
		if !ok {
			continue
		}
		complete++
		for i := range points {
			offsets[i] = append(offsets[i], float64(times[i]-times[0])/1000)
			if i > 0 {
				steps[i] = append(steps[i], float64(times[i]-times[i-1])/1000)
			}
		}
		for psn, sent := range e.packets[latencytrace.ServerSent] {
			if received, ok := e.packets[latencytrace.ClientReceived][psn]; ok {
				network = append(network, float64(received-sent)/1000)
			}
		}
		if example == nil || key.csn%997 == 0 {
			example, exampleKey = e, key
		}
	}
	if complete == 0 {
		log.Fatalln("no chunk is in both traces from the server's input to the player: are they of the same run?")
	}

	var markers []markerReading
	if *markerPath != "" {
		var err error
		if markers, err = readMarkers(*markerPath); err != nil {
			log.Fatalln(err)
		}
	}
	var screenToScreen, lag sample
	for _, r := range markers {
		screenToScreen = append(screenToScreen, float64(r.seenUs/1000-r.markerMs))
		if r.hasLag {
			lag = append(lag, r.lagS*1000)
		}
	}

	fmt.Printf("# Latency, PC screen → phone screen\n\n")
	fmt.Printf("%d chunks traced end to end, %d screen-to-screen readings. Times in ms, p50 (p90).\n\n", complete, len(screenToScreen))

	var serverToPlayer = offsets[len(offsets)-1].quantile(0.5)
	var playerBuffer = lag.quantile(0.5)
	var total = screenToScreen.quantile(0.5)
	var beforeServer = total - serverToPlayer - playerBuffer

	fmt.Println("| stage | at | step | what takes the time |")
	fmt.Println("|---|---:|---:|---|")
	if len(screenToScreen) > 0 {
		fmt.Printf("| PC screen: frame drawn (t0) | 0 | | latency-marker.html |\n")
		fmt.Printf("| server: first byte from OBS | %.0f | %.0f | screen capture, OBS encoding and muxing (what remains of the total) |\n", beforeServer, beforeServer)
	}
	var base = 0.0
	if len(screenToScreen) > 0 {
		base = beforeServer
	}
	for i, p := range points[1:] {
		var at, step = offsets[i+1], steps[i+1]
		fmt.Printf("| %s | %.0f (%.0f) | %.1f (%.1f) | %s |\n", p.name, base+at.quantile(0.5), base+at.quantile(0.9), step.quantile(0.5), step.quantile(0.9), p.explain)
	}
	if len(lag) > 0 {
		fmt.Printf("| phone screen: frame shown | %.0f | %.0f (%.0f) | the player's buffer: mpv demuxer, decoder and audio output |\n", base+serverToPlayer+playerBuffer, playerBuffer, lag.quantile(0.9))
	}
	if len(screenToScreen) > 0 {
		fmt.Printf("\nScreen to screen, measured: p50 %.0f, p90 %.0f, min %.0f, max %.0f.\n", total, screenToScreen.quantile(0.9), screenToScreen.quantile(0), screenToScreen.quantile(1))
	}
	fmt.Printf("One-way network delay per packet (sent → received): p50 %.1f, p90 %.1f, min %.1f.\n", network.quantile(0.5), network.quantile(0.9), network.quantile(0))

	fmt.Printf("\n## One chunk (session %s, csn %d), UTC\n\n", exampleKey.session, exampleKey.csn)
	fmt.Println("| stage | time | +ms |")
	fmt.Println("|---|---|---:|")
	var start, _ = points[0].at(example)
	for _, p := range points {
		at, _ := p.at(example)
		fmt.Printf("| %s | %s | %.1f |\n", p.name, time.UnixMicro(at).UTC().Format("15:04:05.000000"), float64(at-start)/1000)
	}
}
