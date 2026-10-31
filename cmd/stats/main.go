// Command stats measures the delay and jitter of packets through the relay.
//
// The writer registers at the relay as the server and sends data packets of
// the real stream size at a fixed rate; each carries its number and the send
// time. The reader registers as a client and writes one line per packet it
// gets to a file:
//
//	<packet number> <send unix ns> <receive unix ns>
//
// which histo.go plots. Run writer and reader on machines with synced clocks,
// or on one machine, for absolute delays; jitter needs no sync. The relay
// keeps one server, so the writer replaces a real stream server while it
// runs.
//
//	stats -mode writer -addr relay.example.com -rate 1000
//	stats -mode reader -addr relay.example.com -out received
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src"
	"github.com/imakiri/stream/src/codec"
	"github.com/imakiri/stream/src/wire"
)

// statsPacketSize is the size of a stream data packet: an FEC packet sealed
// in a wire data packet.
const statsPacketSize = codec.PacketV2Size + wire.DataOverhead

// A stats packet has the wire data header, so the relay routes it as stream
// data, then the send time; the rest is random, like ciphertext.
const statsSendTimeOffset = wire.DataHeaderSize

func main() {
	var mode = flag.String("mode", "writer", "writer or reader")
	var rate = flag.Int("rate", 1000, "writer: packets per second, each "+fmt.Sprint(statsPacketSize)+" bytes")
	var rawPeerID = flag.String("id", "", "peer id at the relay; random if empty")
	var addr = flag.String("addr", "", "relay address")
	var port = flag.Uint("port", 25565, "relay port")
	var network = flag.String("network", "udp", "udp or tcp")
	var socks5 = flag.String("socks5", "", "tcp only: reach the relay through this socks5 proxy, e.g. socks5://127.0.0.1:2080")
	var amneziaWG = flag.String("amneziawg", "", "reach the relay through an amneziawg tunnel with this config file")
	var out = flag.String("out", "received", "reader: file to write the packet times to")
	flag.Parse()

	var peerID = uuid.Must(uuid.NewV4())
	if *rawPeerID != "" {
		var err error
		if peerID, err = uuid.FromString(*rawPeerID); err != nil {
			log.Fatalln(err)
		}
	}
	var role wire.Role
	switch *mode {
	case "writer":
		role = wire.RoleServer
	case "reader":
		role = wire.RoleClient
	default:
		log.Fatalln("invalid mode", *mode)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	var relayConfig = src.RelayConfig{Network: src.RelayNetwork(*network), Addr: *addr, Port: uint16(*port)}
	if *socks5 != "" {
		relayConfig.Proxy.Socks5 = &src.Socks5ProxyConfig{URL: *socks5}
	}
	if *amneziaWG != "" {
		relayConfig.Proxy.AmneziaWG = &src.AmneziaWGProxyConfig{ConfigPath: *amneziaWG}
	}
	link, err := src.DialRelay(ctx, relayConfig, role, peerID)
	if err != nil {
		log.Fatalln(err)
	}

	switch role {
	case wire.RoleServer:
		// Nothing comes to the writer, but the link takes the relay's hello
		// echoes in Read and dials again without them.
		go func() {
			var buf = make([]byte, 1<<16-1)
			for {
				if _, err := link.Read(buf); errors.Is(err, net.ErrClosed) {
					return
				}
			}
		}()
		err = write(ctx, link, *rate)
	case wire.RoleClient:
		err = read(link, *out)
	}
	if err != nil && ctx.Err() == nil {
		log.Fatalln(err)
	}
}

// write sends packets on a fixed schedule, so the sender's sleep jitter does
// not add up into the measurement.
func write(ctx context.Context, link interface{ Write([]byte) (int, error) }, rate int) error {
	if rate <= 0 {
		return fmt.Errorf("rate %d, want a positive number of packets per second", rate)
	}
	var interval = time.Second / time.Duration(rate)
	var packet = make([]byte, statsPacketSize)
	packet[0] = wire.Version
	packet[1] = byte(wire.KindData)
	var start = time.Now()
	for number := uint64(0); ctx.Err() == nil; number++ {
		if wait := time.Until(start.Add(time.Duration(number) * interval)); wait > 0 {
			time.Sleep(wait)
		}
		binary.BigEndian.PutUint64(packet[6:wire.DataHeaderSize], number)
		_, _ = rand.Read(packet[statsSendTimeOffset+8:])
		binary.BigEndian.PutUint64(packet[statsSendTimeOffset:], uint64(time.Now().UnixNano()))
		if _, err := link.Write(packet); err != nil {
			return err
		}
	}
	return nil
}

func read(link interface{ Read([]byte) (int, error) }, path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var output = bufio.NewWriter(file)
	defer func() {
		if err := output.Flush(); err != nil {
			log.Println(err)
		}
		if err := file.Close(); err != nil {
			log.Println(err)
		}
	}()

	var buf = make([]byte, 1<<16-1)
	var count uint64
	var report = time.NewTicker(5 * time.Second)
	defer report.Stop()
	for {
		n, err := link.Read(buf)
		if err != nil {
			return err // the link is closed: ctx is done
		}
		var receivedAt = time.Now().UnixNano()
		var packet = buf[:n]
		// only data is measured
		if wire.PacketKind(packet) != wire.KindData || len(packet) < statsSendTimeOffset+8 {
			continue
		}
		var number = binary.BigEndian.Uint64(packet[6:wire.DataHeaderSize])
		var sentAt = binary.BigEndian.Uint64(packet[statsSendTimeOffset:])
		if _, err := fmt.Fprintf(output, "%d %d %d\n", number, sentAt, receivedAt); err != nil {
			return err
		}
		count++
		select {
		case <-report.C:
			log.Printf("%d packets", count)
		default:
		}
	}
}
