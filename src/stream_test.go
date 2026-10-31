package src

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/liveoutput"
	"github.com/imakiri/stream/src/relay"
	"github.com/imakiri/stream/src/wire"
)

func freeUDPPort(t *testing.T) uint16 {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return uint16(conn.LocalAddr().(*net.UDPAddr).Port)
}

// freeRelayAddr returns a localhost port free for both udp and tcp.
func freeRelayAddr(t *testing.T) (string, uint16) {
	t.Helper()
	for range 20 {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		var port = uint16(l.Addr().(*net.TCPAddr).Port)
		_ = l.Close()
		if u, err := net.ListenPacket("udp4", l.Addr().String()); err == nil {
			_ = u.Close()
			return "127.0.0.1", port
		}
	}
	t.Fatal("no free port")
	return "", 0
}

type testRoot struct {
	key ed25519.PrivateKey
	pub ed25519.PublicKey
}

func newTestRoot(t *testing.T) testRoot {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return testRoot{key: key, pub: pub}
}

func (r testRoot) issue(t *testing.T) wire.Credentials {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Credentials{
		Certificate: wire.IssueCertificate(r.key, uuid.Must(uuid.NewV4()), time.Now().Add(time.Hour), pub),
		PrivateKey:  key,
	}
}

var testFec = FecConfig{DataParts: 4, TotalParts: 6}

func startTestServer(t *testing.T, ctx context.Context, relayCfg RelayConfig, root testRoot, creds wire.Credentials) uint16 {
	t.Helper()
	var inputPort = freeUDPPort(t)
	server, err := NewServer(ServerConfig{
		Relay:           relayCfg,
		StreamInputPort: inputPort,
		PacingRate:      DefaultPacingRate,
		Fec:             testFec,
		Encoder:         EncoderConfig{DispatcherTimeout: 5 * time.Millisecond, DispatcherSize: 2},
		Credentials:     creds,
		Verifier:        wire.RootVerifier{Root: root.pub},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := server.Run(ctx); err != nil {
			t.Error(err)
		}
	}()
	return inputPort
}

// startTestPlayer starts a client and returns the UDP socket of its player.
func startTestPlayer(t *testing.T, ctx context.Context, relayCfg RelayConfig, root ed25519.PublicKey, creds wire.Credentials, serverID uuid.UUID) *net.UDPConn {
	t.Helper()
	player, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { player.Close() })
	client, err := NewClient(ClientConfig{
		Relay:       relayCfg,
		PlayerPort:  uint16(player.LocalAddr().(*net.UDPAddr).Port),
		Fec:         testFec,
		Decoder:     DecoderConfig{AssemblerSize: 16, DispatcherSize: 8},
		Credentials: creds,
		Verifier:    wire.RootVerifier{Root: root},
		ServerID:    serverID,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := client.Run(ctx); err != nil {
			t.Error(err)
		}
	}()
	return player
}

// streamUntilPlayed sends mpegts-sized datagrams to the server input until
// every player has received want bytes of them, and returns what it sent.
// A player may first play the tail of an earlier stream, the earlier sent
// bytes; after that it must play a contiguous piece of this stream.
func streamUntilPlayed(t *testing.T, inputPort uint16, earlier []byte, want int, players ...*net.UDPConn) []byte {
	t.Helper()
	obs, err := net.Dial("udp4", "127.0.0.1:"+strconv.Itoa(int(inputPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close()

	type playerOutput struct {
		mu   sync.Mutex
		data bytes.Buffer
	}
	var outputs = make([]*playerOutput, len(players))
	var done sync.WaitGroup
	for i, player := range players {
		outputs[i] = new(playerOutput)
		_ = player.SetReadDeadline(time.Time{})
		done.Go(func() {
			var buf = make([]byte, packetBufferSize)
			for {
				n, err := player.Read(buf)
				if err != nil {
					return
				}
				outputs[i].mu.Lock()
				outputs[i].data.Write(buf[:n])
				outputs[i].mu.Unlock()
			}
		})
	}
	var sent bytes.Buffer
	// thisStream returns what player i played of the bytes sent now: a player
	// may first play the tail of an earlier stream, the earlier sent bytes.
	// The data is random, so a 64-byte probe does not match by chance.
	var thisStream = func(i int) (before, now []byte) {
		outputs[i].mu.Lock()
		var got = bytes.Clone(outputs[i].data.Bytes())
		outputs[i].mu.Unlock()
		const probe = 64
		var start = 0
		for start+probe <= len(got) && !bytes.Contains(sent.Bytes(), got[start:start+probe]) {
			start++
		}
		return got[:start], got[start:]
	}
	var allPlayed = func() bool {
		for i := range players {
			if _, now := thisStream(i); len(now) < want {
				return false
			}
		}
		return true
	}

	var deadline = time.Now().Add(15 * time.Second)
	var datagram = make([]byte, 1316)
	// allPlayed searches everything sent, so it is asked every 20 ms only
	var nextCheck time.Time
	for time.Now().Before(deadline) {
		if time.Now().After(nextCheck) {
			if allPlayed() {
				break
			}
			nextCheck = time.Now().Add(20 * time.Millisecond)
		}
		_, _ = rand.Read(datagram)
		sent.Write(datagram)
		// The server listens a moment after startTestServer returns; until
		// then the kernel answers with port unreachable, which a connected
		// UDP socket reports on a later write. Keep sending, as OBS does.
		_, _ = obs.Write(datagram)
		time.Sleep(200 * time.Microsecond)
	}
	for _, player := range players {
		_ = player.SetReadDeadline(time.Now())
	}
	done.Wait()

	for i := range players {
		var before, now = thisStream(i)
		if len(now) < want {
			t.Fatalf("player %d got %d bytes of this stream, want at least %d", i, len(now), want)
		}
		if !bytes.Contains(earlier, before) {
			t.Fatalf("player %d got %d bytes before this stream that the earlier stream did not send", i, len(before))
		}
		// The client joins after the server started, so it plays a
		// contiguous piece of the sent stream from some point on.
		if !bytes.Contains(sent.Bytes(), now) {
			t.Fatalf("player %d got bytes that were not sent in this order", i)
		}
	}
	return sent.Bytes()
}

// TestStreamReachesClientsOverUDPAndTCP runs a relay, a server on UDP and
// two clients, one on UDP and one on TCP, and checks the stream from OBS
// arrives at both players.
func TestStreamReachesClientsOverUDPAndTCP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, port := freeRelayAddr(t)
	var p = relay.New(net.JoinHostPort(addr, strconv.Itoa(int(port))), liveoutput.New(io.Discard))
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var root = newTestRoot(t)
	var serverCreds = root.issue(t)
	var udp = RelayConfig{Network: RelayNetworkUDP, Addr: addr, Port: port}
	var tcp = RelayConfig{Network: RelayNetworkTCP, Addr: addr, Port: port}
	var inputPort = startTestServer(t, ctx, udp, root, serverCreds)
	var udpPlayer = startTestPlayer(t, ctx, udp, root.pub, root.issue(t), serverCreds.Certificate.ID)
	var tcpPlayer = startTestPlayer(t, ctx, tcp, root.pub, root.issue(t), serverCreds.Certificate.ID)

	streamUntilPlayed(t, inputPort, nil, 64<<10, udpPlayer, tcpPlayer)
}

// TestStreamRejectsClientOfAnotherRoot checks that a client whose
// certificate the server does not trust gets nothing to play.
func TestStreamRejectsClientOfAnotherRoot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, port := freeRelayAddr(t)
	var p = relay.New(net.JoinHostPort(addr, strconv.Itoa(int(port))), liveoutput.New(io.Discard))
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var root = newTestRoot(t)
	var serverCreds = root.issue(t)
	var udp = RelayConfig{Network: RelayNetworkUDP, Addr: addr, Port: port}
	var inputPort = startTestServer(t, ctx, udp, root, serverCreds)
	var trusted = startTestPlayer(t, ctx, udp, root.pub, root.issue(t), serverCreds.Certificate.ID)
	var stranger = newTestRoot(t)
	// trusts our root, so it would accept the server, but its own
	// certificate is from another root
	var untrusted = startTestPlayer(t, ctx, udp, root.pub, stranger.issue(t), serverCreds.Certificate.ID)

	streamUntilPlayed(t, inputPort, nil, 32<<10, trusted)
	var buf = make([]byte, packetBufferSize)
	_ = untrusted.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := untrusted.Read(buf); err == nil {
		t.Fatalf("untrusted client played %d bytes", n)
	}
}

// TestStreamClientFollowsRestartedServer restarts the server, which makes a
// new stream key, and checks the client handshakes again and plays on.
func TestStreamClientFollowsRestartedServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, port := freeRelayAddr(t)
	var p = relay.New(net.JoinHostPort(addr, strconv.Itoa(int(port))), liveoutput.New(io.Discard))
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var root = newTestRoot(t)
	var serverCreds = root.issue(t)
	var udp = RelayConfig{Network: RelayNetworkUDP, Addr: addr, Port: port}

	firstCtx, stopFirst := context.WithCancel(ctx)
	var inputPort = startTestServer(t, firstCtx, udp, root, serverCreds)
	var player = startTestPlayer(t, ctx, udp, root.pub, root.issue(t), serverCreds.Certificate.ID)
	var firstStream = streamUntilPlayed(t, inputPort, nil, 16<<10, player)
	stopFirst()

	// the player may still get the end of the first stream from the client
	inputPort = startTestServer(t, ctx, udp, root, serverCreds)
	streamUntilPlayed(t, inputPort, firstStream, 16<<10, player)
}

// TestStreamSurvivesRelayRestart restarts the relay, which forgets every
// peer and, over TCP, breaks their connections, and checks that the server
// and the client register again and the stream plays on.
func TestStreamSurvivesRelayRestart(t *testing.T) {
	for _, network := range []RelayNetwork{RelayNetworkUDP, RelayNetworkTCP} {
		t.Run(string(network), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			addr, port := freeRelayAddr(t)
			var relayAddr = net.JoinHostPort(addr, strconv.Itoa(int(port)))
			var p = relay.New(relayAddr, liveoutput.New(io.Discard))
			if err := p.Serve(); err != nil {
				t.Fatal(err)
			}

			var root = newTestRoot(t)
			var serverCreds = root.issue(t)
			var link = RelayConfig{Network: network, Addr: addr, Port: port}
			var inputPort = startTestServer(t, ctx, link, root, serverCreds)
			var player = startTestPlayer(t, ctx, link, root.pub, root.issue(t), serverCreds.Certificate.ID)
			var firstStream = streamUntilPlayed(t, inputPort, nil, 16<<10, player)

			p.Close()
			time.Sleep(300 * time.Millisecond)
			p = relay.New(relayAddr, liveoutput.New(io.Discard))
			if err := p.Serve(); err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			streamUntilPlayed(t, inputPort, firstStream, 16<<10, player)
		})
	}
}

// TestStreamClientPlaysNothingForged lets an attacker who knows only the
// relay address take the server's place there, as the relay authenticates
// nobody, and send data packets with the real stream's epoch, which it read
// as a client. The client must play none of it, and the stream must come back
// once the real server repeats its registration.
func TestStreamClientPlaysNothingForged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, port := freeRelayAddr(t)
	var relayAddr = net.JoinHostPort(addr, strconv.Itoa(int(port)))
	var p = relay.New(relayAddr, liveoutput.New(io.Discard))
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var root = newTestRoot(t)
	var serverCreds = root.issue(t)
	var udp = RelayConfig{Network: RelayNetworkUDP, Addr: addr, Port: port}
	var inputPort = startTestServer(t, ctx, udp, root, serverCreds)
	var player = startTestPlayer(t, ctx, udp, root.pub, root.issue(t), serverCreds.Certificate.ID)

	// the attacker listens as a client for the epoch while the stream runs
	var spy = dialRawRelay(t, relayAddr, wire.RoleClient)
	var epoch = make(chan []byte, 1)
	go func() {
		var buf = make([]byte, packetBufferSize)
		for {
			n, err := spy.Read(buf)
			if err != nil {
				return
			}
			if wire.PacketKind(buf[:n]) == wire.KindData && n >= wire.DataHeaderSize {
				epoch <- bytes.Clone(buf[2:6])
				return
			}
		}
	}()
	var firstStream = streamUntilPlayed(t, inputPort, nil, 16<<10, player)
	var streamEpoch = <-epoch

	// OBS is quiet now; the player may still get the end of the real stream,
	// but nothing else
	time.Sleep(500 * time.Millisecond)
	var played = make(chan []byte, 1)
	go func() {
		var got bytes.Buffer
		var buf = make([]byte, packetBufferSize)
		_ = player.SetReadDeadline(time.Now().Add(2500 * time.Millisecond))
		for {
			n, err := player.Read(buf)
			if err != nil {
				played <- got.Bytes()
				return
			}
			got.Write(buf[:n])
		}
	}()
	var hijacker = dialRawRelay(t, relayAddr, wire.RoleServer)
	var forged = make([]byte, 1086)
	for i := range 2000 {
		_, _ = rand.Read(forged)
		forged[0], forged[1] = wire.Version, byte(wire.KindData)
		copy(forged[2:6], streamEpoch)
		_, _ = hijacker.Write(forged)
		if i%100 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	if got := <-played; !bytes.Contains(firstStream, got) {
		t.Fatalf("the player got %d bytes that the server did not send", len(got))
	} else if len(got) > 0 {
		t.Logf("the player got %d late bytes of the real stream during the attack", len(got))
	}
	_ = player.SetReadDeadline(time.Time{})

	// the server's next keepalive takes its place back
	streamUntilPlayed(t, inputPort, firstStream, 16<<10, player)
}

// dialRawRelay registers a bare UDP socket at the relay, the way anyone who
// knows its address can.
func dialRawRelay(t *testing.T, relayAddr string, role wire.Role) net.Conn {
	t.Helper()
	conn, err := net.Dial("udp4", relayAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := exchangeRelayHello(conn, wire.MarshalRelayHello(role, uuid.Must(uuid.NewV4())), relayHelloUDPAttempts); err != nil {
		t.Fatal(err)
	}
	return conn
}
