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
