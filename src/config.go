package src

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pelletier/go-toml"
)

// The config files are TOML; see cmd/server/server.example.cfg and
// cmd/client/client.example.cfg. They hold the network, FEC and clock settings only:
// keys and certificates are files in the key directory, see keystore.go.

type relayConfigFile struct {
	Addr    string `toml:"addr"`
	Port    uint16 `toml:"port"`
	Network string `toml:"network"` // "udp" (default) or "tcp"
}

// proxyConfigFile is the [proxy] section: [proxy.socks5], [proxy.http] or
// [proxy.amneziawg], at most one. TOML has no union types, so each kind is
// a sub-table and an absent one decodes to nil.
type proxyConfigFile struct {
	Socks5 *struct {
		URL string `toml:"url"`
	} `toml:"socks5"`
	HTTP *struct {
		URL string `toml:"url"`
	} `toml:"http"`
	AmneziaWG *struct {
		Config string `toml:"config"` // path, relative to the directory of the config file
	} `toml:"amneziawg"`
}

// relayConfig makes the RelayConfig of the [relay] and [proxy] sections of
// the config file at configPath.
func relayConfig(relay relayConfigFile, proxy proxyConfigFile, configPath string) RelayConfig {
	var cfg = RelayConfig{Network: RelayNetwork(relay.Network), Addr: relay.Addr, Port: relay.Port}
	if proxy.Socks5 != nil {
		cfg.Proxy.Socks5 = &Socks5ProxyConfig{URL: proxy.Socks5.URL}
	}
	if proxy.HTTP != nil {
		cfg.Proxy.HTTP = &HTTPProxyConfig{URL: proxy.HTTP.URL}
	}
	if proxy.AmneziaWG != nil {
		var path = proxy.AmneziaWG.Config
		if path != "" && !filepath.IsAbs(path) {
			path = filepath.Join(filepath.Dir(configPath), path)
		}
		cfg.Proxy.AmneziaWG = &AmneziaWGProxyConfig{ConfigPath: path}
	}
	return cfg
}

type fecConfigFile struct {
	DataParts  uint64 `toml:"data_parts"`
	TotalParts uint64 `toml:"total_parts"`
}

func (f fecConfigFile) config() FecConfig {
	return FecConfig{DataParts: f.DataParts, TotalParts: f.TotalParts}
}

// clockConfigFile is the [clock] section: the NTP server both ends take the
// time from for the end-to-end delay.
type clockConfigFile struct {
	NTPServer string `toml:"ntp_server"` // empty or not in the file: no NTP
}

// loadConfigFile decodes the config at path into dst. Unknown keys are an
// error, so a typo or a section of an older layout is not silently ignored.
func loadConfigFile(path string, dst any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to read config %s: %w", path, err)
	}
	defer file.Close()
	if err := toml.NewDecoder(file).Strict(true).Decode(dst); err != nil {
		if tree, treeErr := toml.LoadFile(path); treeErr == nil && tree.Has("proxy.addr") {
			return fmt.Errorf("failed to parse config %s: the [proxy] section with the relay address is now called [relay]; [proxy] is for a socks5 or amneziawg proxy, see the example config", path)
		}
		return fmt.Errorf("failed to parse config %s: %w", path, err)
	}
	return nil
}

func LoadServerConfig(path string) (ServerConfig, error) {
	var f struct {
		Relay       relayConfigFile `toml:"relay"`
		Proxy       proxyConfigFile `toml:"proxy"`
		StreamInput struct {
			Port uint16 `toml:"port"`
		} `toml:"stream_input"`
		Fec     fecConfigFile `toml:"fec"`
		Encoder struct {
			DispatcherSize    uint64 `toml:"dispatcher_size"`
			DispatcherTimeout uint64 `toml:"dispatcher_timeout"` // milliseconds
		} `toml:"encoder"`
		Pacing struct {
			RateMbit *uint64 `toml:"rate_mbit"` // nil: not in the file, DefaultPacingRate
		} `toml:"pacing"`
		Clock clockConfigFile `toml:"clock"`
	}
	if err := loadConfigFile(path, &f); err != nil {
		return ServerConfig{}, err
	}
	var pacingRate uint64 = DefaultPacingRate
	if f.Pacing.RateMbit != nil {
		pacingRate = *f.Pacing.RateMbit * 1_000_000
	}
	return ServerConfig{
		Relay:           relayConfig(f.Relay, f.Proxy, path),
		StreamInputPort: f.StreamInput.Port,
		PacingRate:      pacingRate,
		Fec:             f.Fec.config(),
		Encoder: EncoderConfig{
			DispatcherTimeout: time.Duration(f.Encoder.DispatcherTimeout) * time.Millisecond,
			DispatcherSize:    f.Encoder.DispatcherSize,
		},
		NTPServer: f.Clock.NTPServer,
	}, nil
}

func LoadClientConfig(path string) (ClientConfig, error) {
	var f struct {
		Relay  relayConfigFile `toml:"relay"`
		Proxy  proxyConfigFile `toml:"proxy"`
		Player struct {
			Port uint16 `toml:"port"`
		} `toml:"player"`
		Fec     fecConfigFile `toml:"fec"`
		Decoder struct {
			AssemblerSize  uint64 `toml:"assembler_size"`
			DispatcherSize uint64 `toml:"dispatcher_size"`
		} `toml:"decoder"`
		Clock clockConfigFile `toml:"clock"`
	}
	if err := loadConfigFile(path, &f); err != nil {
		return ClientConfig{}, err
	}
	return ClientConfig{
		Relay:      relayConfig(f.Relay, f.Proxy, path),
		PlayerPort: f.Player.Port,
		Fec:        f.Fec.config(),
		Decoder: DecoderConfig{
			AssemblerSize:  f.Decoder.AssemblerSize,
			DispatcherSize: f.Decoder.DispatcherSize,
		},
		NTPServer: f.Clock.NTPServer,
	}, nil
}
