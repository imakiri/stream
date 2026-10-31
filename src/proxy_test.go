package src

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/amneziawg"
	"github.com/imakiri/stream/src/frame"
	"github.com/imakiri/stream/src/wire"
	"golang.org/x/crypto/curve25519"
)

func newWireGuardKeys(t *testing.T) (private, public string) {
	var key = make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	key[0] &= 248
	key[31] = key[31]&127 | 64
	publicKey, err := curve25519.X25519(key, curve25519.Basepoint)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(key), base64.StdEncoding.EncodeToString(publicKey)
}

// startAmneziaWGPeer brings up the far end of an AmneziaWG tunnel at
// 10.77.0.1 on loopback and writes the config of the near end, as the
// Amnezia app would export it, to a file; it returns the far end and the
// path of the file.
func startAmneziaWGPeer(t *testing.T) (*amneziawg.Tunnel, string) {
	const obfuscation = "Jc = 3\nJmin = 10\nJmax = 50\nS1 = 15\nS2 = 18\nH1 = 1234567\nH2 = 2345678\nH3 = 3456789\nH4 = 4567890\n"
	var farPrivate, farPublic = newWireGuardKeys(t)
	var nearPrivate, nearPublic = newWireGuardKeys(t)
	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	var port = probe.LocalAddr().(*net.UDPAddr).Port
	_ = probe.Close()

	farConfig, err := amneziawg.ParseConfig(strings.NewReader(fmt.Sprintf(
		"[Interface]\nPrivateKey = %s\nAddress = 10.77.0.1/32\nListenPort = %d\n%s[Peer]\nPublicKey = %s\nAllowedIPs = 10.77.0.2/32\n",
		farPrivate, port, obfuscation, nearPublic)))
	if err != nil {
		t.Fatal(err)
	}
	far, err := amneziawg.Open(context.Background(), farConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = far.Close() })

	var path = filepath.Join(t.TempDir(), "amneziawg.conf")
	var nearConfig = fmt.Sprintf(
		"[Interface]\nPrivateKey = %s\nAddress = 10.77.0.2/32\n%s[Peer]\nPublicKey = %s\nAllowedIPs = 10.77.0.0/24\nEndpoint = 127.0.0.1:%d\nPersistentKeepalive = 25\n",
		nearPrivate, obfuscation, farPublic, port)
	if err := os.WriteFile(path, []byte(nearConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	return far, path
}

// TestRelayLinkThroughAmneziaWG registers a RelayLink, over UDP and over
// TCP, at a fake relay that sits behind an AmneziaWG tunnel: it echoes the
// relay hello and then sends a data packet, which the link must read.
func TestRelayLinkThroughAmneziaWG(t *testing.T) {
	var far, configPath = startAmneziaWGPeer(t)
	var relayAddr = netip.MustParseAddrPort("10.77.0.1:25565")
	var data = []byte{wire.Version, byte(wire.KindData), 1, 2, 3}

	udpRelay, err := far.ListenUDP(relayAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer udpRelay.Close()
	go func() {
		var buf = make([]byte, packetBufferSize)
		for {
			n, from, err := udpRelay.ReadFrom(buf)
			if err != nil {
				return
			}
			if wire.PacketKind(buf[:n]) == wire.KindRelayHello {
				_, _ = udpRelay.WriteTo(buf[:n], from)
				_, _ = udpRelay.WriteTo(data, from)
			}
		}
	}()
	tcpRelay, err := far.ListenTCP(relayAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer tcpRelay.Close()
	go func() {
		for {
			conn, err := tcpRelay.Accept()
			if err != nil {
				return
			}
			go func() {
				var framed = frame.NewConn(conn)
				defer framed.Close()
				var buf = make([]byte, packetBufferSize)
				for {
					n, err := framed.Read(buf)
					if err != nil {
						return
					}
					if wire.PacketKind(buf[:n]) == wire.KindRelayHello {
						_, _ = framed.Write(buf[:n])
						_, _ = framed.Write(data)
					}
				}
			}()
		}
	}()

	for _, network := range []RelayNetwork{RelayNetworkUDP, RelayNetworkTCP} {
		t.Run(string(network), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			link, err := DialRelay(ctx, RelayConfig{
				Network: network,
				Addr:    relayAddr.Addr().String(),
				Port:    relayAddr.Port(),
				Proxy:   ProxyConfig{AmneziaWG: &AmneziaWGProxyConfig{ConfigPath: configPath}},
			}, wire.RoleClient, uuid.Must(uuid.NewV4()))
			if err != nil {
				t.Fatal(err)
			}
			defer link.Close()
			var got = make(chan []byte, 1)
			go func() {
				var buf = make([]byte, packetBufferSize)
				if n, err := link.Read(buf); err == nil {
					got <- bytes.Clone(buf[:n])
				}
			}()
			select {
			case packet := <-got:
				if !bytes.Equal(packet, data) {
					t.Fatalf("read %x, want %x", packet, data)
				}
			case <-ctx.Done():
				t.Fatal("no data from the relay through the tunnel")
			}
		})
	}
}

func TestProxyConfigFromFile(t *testing.T) {
	var dir = t.TempDir()
	var write = func(config string) string {
		var path = filepath.Join(dir, "client.cfg")
		if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	cfg, err := LoadClientConfig(write("[relay]\n  addr = \"relay.example\"\n[proxy.amneziawg]\n  config = \"awg/amneziawg.conf\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Relay.Addr != "relay.example" || cfg.Relay.Proxy.Socks5 != nil || cfg.Relay.Proxy.AmneziaWG == nil ||
		cfg.Relay.Proxy.AmneziaWG.ConfigPath != filepath.Join(dir, "awg", "amneziawg.conf") {
		t.Errorf("relay config %+v, amneziawg %+v", cfg.Relay, cfg.Relay.Proxy.AmneziaWG)
	}

	cfg, err = LoadClientConfig(write("[relay]\n  network = \"tcp\"\n[proxy.socks5]\n  url = \"socks5://127.0.0.1:2080\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Relay.Proxy.Socks5 == nil || cfg.Relay.Proxy.Socks5.URL != "socks5://127.0.0.1:2080" || cfg.Relay.Proxy.AmneziaWG != nil {
		t.Errorf("proxy config %+v", cfg.Relay.Proxy)
	}

	// the layout before the relay was called relay
	_, err = LoadClientConfig(write("[proxy]\n  addr = \"relay.example\"\n  port = 25565\n"))
	if err == nil || !strings.Contains(err.Error(), "now called [relay]") {
		t.Errorf("old [proxy] section: error %v, want a hint about [relay]", err)
	}
	if _, err = LoadClientConfig(write("[relay]\n  adr = \"relay.example\"\n")); err == nil {
		t.Error("no error for an unknown key")
	}
}

func TestProxyConfigIsChecked(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  RelayConfig
	}{
		{"both proxies", RelayConfig{Network: RelayNetworkTCP, Proxy: ProxyConfig{
			Socks5:    &Socks5ProxyConfig{URL: "socks5://127.0.0.1:2080"},
			AmneziaWG: &AmneziaWGProxyConfig{ConfigPath: "amneziawg.conf"},
		}}},
		{"socks5 over udp", RelayConfig{Network: RelayNetworkUDP, Proxy: ProxyConfig{
			Socks5: &Socks5ProxyConfig{URL: "socks5://127.0.0.1:2080"},
		}}},
		{"missing amneziawg config", RelayConfig{Proxy: ProxyConfig{
			AmneziaWG: &AmneziaWGProxyConfig{ConfigPath: filepath.Join(t.TempDir(), "none.conf")},
		}}},
	} {
		link, err := DialRelay(context.Background(), c.cfg, wire.RoleClient, uuid.Must(uuid.NewV4()))
		if err == nil {
			_ = link.Close()
			t.Errorf("%s: no error", c.name)
		}
	}
}
