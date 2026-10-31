package src

import (
	"bytes"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/imakiri/stream/src/codec"
)

// TestPlayLogsAMissingPlayerOnce sends the stream to a port nobody listens
// on, then starts a player there: the refusals give one line, the player one
// more.
func TestPlayLogsAMissingPlayerOnce(t *testing.T) {
	var logged bytes.Buffer
	var logOutput = log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(logOutput)

	// a free port with no player on it
	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	var playerAddr = probe.LocalAddr().(*net.UDPAddr)
	probe.Close()
	conn, err := net.DialUDP("udp4", nil, playerAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var chunks = make(chan codec.DecodedChunk)
	var done = make(chan struct{})
	go func() {
		new(Client).play(conn, chunks)
		close(done)
	}()
	var send = func(d time.Duration) {
		for end := time.Now().Add(d); time.Now().Before(end); {
			chunks <- codec.DecodedChunk{Data: make([]byte, 4*playerDatagramSize)}
			time.Sleep(5 * time.Millisecond)
		}
	}

	send(500 * time.Millisecond)
	player, err := net.ListenUDP("udp4", playerAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer player.Close()
	go func() { // drain
		var buf = make([]byte, 2048)
		for {
			if _, err := player.Read(buf); err != nil {
				return
			}
		}
	}()
	send(playerRecoveryTime + 500*time.Millisecond)
	close(chunks)
	<-done

	var text = logged.String()
	if n := strings.Count(text, "failed to send to the player"); n != 1 {
		t.Fatalf("want one failure line, got %d:\n%s", n, text)
	}
	if n := strings.Count(text, "the player takes the stream again"); n != 1 {
		t.Fatalf("want one recovery line, got %d:\n%s", n, text)
	}
}
