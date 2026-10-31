package src

import (
	"os"
	"path/filepath"
	"testing"
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
