package src

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestServerPacingRate reads the pacing rate from the example config, from
// a config without the section (older server.cfg: the default) and with
// pacing turned off.
func TestServerPacingRate(t *testing.T) {
	example, err := os.ReadFile(filepath.Join("..", "cmd", "server", "server.example.cfg"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name   string
		config string
		want   uint64
	}{
		{"example", string(example), 30_000_000},
		{"no section", "[stream_input]\n  port = 2557\n", DefaultPacingRate},
		{"off", "[pacing]\n  rate_mbit = 0\n", 0},
	} {
		var path = filepath.Join(t.TempDir(), "server.cfg")
		if err := os.WriteFile(path, []byte(c.config), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadServerConfig(path)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if cfg.PacingRate != c.want {
			t.Errorf("%s: pacing rate %d, want %d", c.name, cfg.PacingRate, c.want)
		}
	}
}

// TestClockConfig reads [clock] ntp_server: off when the section or the key
// is not there, as in the example configs, where it is commented out.
func TestClockConfig(t *testing.T) {
	for _, c := range []struct {
		name   string
		config string
		want   string
	}{
		{"no section", "[relay]\n  port = 25565\n", ""},
		{"set", "[clock]\n  ntp_server = \"time.google.com\"\n", "time.google.com"},
	} {
		var path = filepath.Join(t.TempDir(), "stream.cfg")
		if err := os.WriteFile(path, []byte(c.config), 0o600); err != nil {
			t.Fatal(err)
		}
		server, err := LoadServerConfig(path)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		client, err := LoadClientConfig(path)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if server.NTPServer != c.want || client.NTPServer != c.want {
			t.Errorf("%s: ntp server %q in the server, %q in the client, want %q", c.name, server.NTPServer, client.NTPServer, c.want)
		}
	}
}

// TestClientConfigFileRoundTrip writes client configs back as the Android
// app does and reads them again: the example, the defaults and one with a
// proxy.
func TestClientConfigFileRoundTrip(t *testing.T) {
	example, err := os.ReadFile(filepath.Join("..", "cmd", "client", "client.example.cfg"))
	if err != nil {
		t.Fatal(err)
	}
	exampleFile, err := ParseClientConfigFile(example)
	if err != nil {
		t.Fatal(err)
	}
	var withProxy = DefaultClientConfigFile()
	withProxy.Relay.Addr = "relay.example"
	withProxy.Proxy.AmneziaWG = &AmneziaWGConfigFile{Config: "amneziawg.conf"}
	withProxy.Clock.NTPServer = "time.google.com"
	for name, f := range map[string]ClientConfigFile{"example": exampleFile, "default": DefaultClientConfigFile(), "with a proxy": withProxy} {
		text, err := f.Marshal()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		again, err := ParseClientConfigFile(text)
		if err != nil {
			t.Fatalf("%s: %v\n%s", name, err, text)
		}
		if !reflect.DeepEqual(again, f) {
			t.Errorf("%s: read back %+v, want %+v\n%s", name, again, f, text)
		}
	}
	var defaults = DefaultClientConfigFile()
	defaults.Relay.Addr = exampleFile.Relay.Addr
	if !reflect.DeepEqual(exampleFile, defaults) {
		t.Errorf("the defaults %+v differ from the example config %+v", defaults, exampleFile)
	}
	text, _ := withProxy.Marshal()
	if strings.Contains(string(text), "socks5") || strings.Contains(string(text), "[proxy.http]") {
		t.Errorf("unset proxies are written:\n%s", text)
	}
}

// TestClientReorderDeadline reads [decoder] reorder_deadline: the default
// when an older client.cfg does not have it, and 0 (off) only when set so.
func TestClientReorderDeadline(t *testing.T) {
	for _, c := range []struct {
		name   string
		config string
		want   time.Duration
	}{
		{"not set", "[decoder]\n  dispatcher_size = 12\n", DefaultReorderDeadline},
		{"set", "[decoder]\n  reorder_deadline = 20\n", 20 * time.Millisecond},
		{"off", "[decoder]\n  reorder_deadline = 0\n", 0},
	} {
		var path = filepath.Join(t.TempDir(), "client.cfg")
		if err := os.WriteFile(path, []byte(c.config), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadClientConfig(path)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if cfg.Decoder.ReorderDeadline != c.want {
			t.Errorf("%s: reorder deadline %v, want %v", c.name, cfg.Decoder.ReorderDeadline, c.want)
		}
	}
}
