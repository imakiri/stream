package src

import (
	"fmt"
	"time"

	"github.com/pelletier/go-toml"
)

// The config files are TOML; see cmd/server/server.example.cfg and
// cmd/client/client.example.cfg. They hold the network and FEC settings only:
// keys and certificates are files in the key directory, see keystore.go.

type proxyConfigFile struct {
	Addr       string `toml:"addr"`
	Port       uint16 `toml:"port"`
	Network    string `toml:"network"`     // "udp" (default) or "tcp"
	SocksProxy string `toml:"socks_proxy"` // tcp only, e.g. socks5://127.0.0.1:2080
}

func (f proxyConfigFile) config() ProxyConfig {
	return ProxyConfig{Network: ProxyNetwork(f.Network), SocksProxy: f.SocksProxy, Addr: f.Addr, Port: f.Port}
}

type fecConfigFile struct {
	DataParts  uint64 `toml:"data_parts"`
	TotalParts uint64 `toml:"total_parts"`
}

func (f fecConfigFile) config() FecConfig {
	return FecConfig{DataParts: f.DataParts, TotalParts: f.TotalParts}
}

func loadConfigFile(path string, dst any) error {
	tree, err := toml.LoadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config %s: %w", path, err)
	}
	if err := tree.Unmarshal(dst); err != nil {
		return fmt.Errorf("failed to parse config %s: %w", path, err)
	}
	return nil
}

func LoadServerConfig(path string) (ServerConfig, error) {
	var f struct {
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
	}
	if err := loadConfigFile(path, &f); err != nil {
		return ServerConfig{}, err
	}
	var pacingRate uint64 = DefaultPacingRate
	if f.Pacing.RateMbit != nil {
		pacingRate = *f.Pacing.RateMbit * 1_000_000
	}
	return ServerConfig{
		Proxy:           f.Proxy.config(),
		StreamInputPort: f.StreamInput.Port,
		PacingRate:      pacingRate,
		Fec:             f.Fec.config(),
		Encoder: EncoderConfig{
			DispatcherTimeout: time.Duration(f.Encoder.DispatcherTimeout) * time.Millisecond,
			DispatcherSize:    f.Encoder.DispatcherSize,
		},
	}, nil
}

func LoadClientConfig(path string) (ClientConfig, error) {
	var f struct {
		Proxy  proxyConfigFile `toml:"proxy"`
		Player struct {
			Port uint16 `toml:"port"`
		} `toml:"player"`
		Fec     fecConfigFile `toml:"fec"`
		Decoder struct {
			AssemblerSize  uint64 `toml:"assembler_size"`
			DispatcherSize uint64 `toml:"dispatcher_size"`
		} `toml:"decoder"`
	}
	if err := loadConfigFile(path, &f); err != nil {
		return ClientConfig{}, err
	}
	return ClientConfig{
		Proxy:      f.Proxy.config(),
		PlayerPort: f.Player.Port,
		Fec:        f.Fec.config(),
		Decoder: DecoderConfig{
			AssemblerSize:  f.Decoder.AssemblerSize,
			DispatcherSize: f.Decoder.DispatcherSize,
		},
	}, nil
}
