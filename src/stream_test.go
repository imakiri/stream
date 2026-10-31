package src

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/gosuri/uilive"
	"github.com/imakiri/stream/src/proxy"
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

// freeProxyAddr returns a localhost port free for both udp and tcp.
func freeProxyAddr(t *testing.T) (string, uint16) {
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

func startTestServer(t *testing.T, ctx context.Context, proxyCfg ProxyConfig, root testRoot, creds wire.Credentials) uint16 {
	t.Helper()
	var inputPort = freeUDPPort(t)
	server, err := NewServer(ServerConfig{
		Proxy:           proxyCfg,
		StreamInputPort: inputPort,
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
func startTestPlayer(t *testing.T, ctx context.Context, proxyCfg ProxyConfig, root ed25519.PublicKey, creds wire.Credentials, serverID uuid.UUID) *net.UDPConn {
	t.Helper()
	player, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { player.Close() })
	client, err := NewClient(ClientConfig{
		Proxy:       proxyCfg,
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

// TestStreamReachesClientsOverUDPAndTCP runs a proxy, a server on UDP and
// two clients, one on UDP and one on TCP, and checks the stream from OBS
// arrives at both players.
func TestStreamReachesClientsOverUDPAndTCP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, port := freeProxyAddr(t)
	var p = proxy.New(net.JoinHostPort(addr, strconv.Itoa(int(port))), uilive.New())
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var root = newTestRoot(t)
	var serverCreds = root.issue(t)
	var udp = ProxyConfig{Network: ProxyNetworkUDP, Addr: addr, Port: port}
	var tcp = ProxyConfig{Network: ProxyNetworkTCP, Addr: addr, Port: port}
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
	addr, port := freeProxyAddr(t)
	var p = proxy.New(net.JoinHostPort(addr, strconv.Itoa(int(port))), uilive.New())
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var root = newTestRoot(t)
	var serverCreds = root.issue(t)
	var udp = ProxyConfig{Network: ProxyNetworkUDP, Addr: addr, Port: port}
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
	addr, port := freeProxyAddr(t)
	var p = proxy.New(net.JoinHostPort(addr, strconv.Itoa(int(port))), uilive.New())
	if err := p.Serve(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var root = newTestRoot(t)
	var serverCreds = root.issue(t)
	var udp = ProxyConfig{Network: ProxyNetworkUDP, Addr: addr, Port: port}

	firstCtx, stopFirst := context.WithCancel(ctx)
	var inputPort = startTestServer(t, firstCtx, udp, root, serverCreds)
	var player = startTestPlayer(t, ctx, udp, root.pub, root.issue(t), serverCreds.Certificate.ID)
	var firstStream = streamUntilPlayed(t, inputPort, nil, 16<<10, player)
	stopFirst()

	// the player may still get the end of the first stream from the client
	inputPort = startTestServer(t, ctx, udp, root, serverCreds)
	streamUntilPlayed(t, inputPort, firstStream, 16<<10, player)
}
