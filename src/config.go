package src

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pelletier/go-toml"
)

// The config files are TOML; see cmd/server/server.example.cfg and
// cmd/client/client.example.cfg. They hold the network, FEC and clock settings only:
// keys and certificates are files in the key directory, see keystore.go.

// RelayConfigFile is the [relay] section.
type RelayConfigFile struct {
	Addr    string `toml:"addr"`
	Port    uint16 `toml:"port"`
	Network string `toml:"network"` // "udp" (default) or "tcp"
}

// ProxyConfigFile is the [proxy] section: [proxy.socks5], [proxy.http] or
// [proxy.amneziawg], at most one. TOML has no union types, so each kind is
// a sub-table and an absent one decodes to nil.
type ProxyConfigFile struct {
	Socks5    *ProxyURLConfigFile  `toml:"socks5,omitempty"`
	HTTP      *ProxyURLConfigFile  `toml:"http,omitempty"`
	AmneziaWG *AmneziaWGConfigFile `toml:"amneziawg,omitempty"`
}

// ProxyURLConfigFile is [proxy.socks5] or [proxy.http].
type ProxyURLConfigFile struct {
	URL string `toml:"url"`
}

// AmneziaWGConfigFile is [proxy.amneziawg].
type AmneziaWGConfigFile struct {
	Config string `toml:"config"` // path, relative to the directory of the config file
}

// relayConfig makes the RelayConfig of the [relay] and [proxy] sections of
// the config file at configPath.
func relayConfig(relay RelayConfigFile, proxy ProxyConfigFile, configPath string) RelayConfig {
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

// FecConfigFile is the server's [fec] section; a key not in the file takes
// its default from DefaultFecConfig. Clients take the layout from the packets.
type FecConfigFile struct {
	ChunkPackets  uint64 `toml:"chunk_packets"`  // data packets of a full chunk
	BlockChunks   uint64 `toml:"block_chunks"`   // chunks under one Reed-Solomon code
	ParityPercent uint64 `toml:"parity_percent"` // parity packets per 100 data packets of a block
}

// DefaultFecConfig is the FEC of a server config without these keys: the
// layout that lost fewest chunks within the client's reorder deadline on
// the recorded link of 2026-11-01 (capture/build/runs/live-1650-direct).
var DefaultFecConfig = FecConfig{ChunkPackets: 6, BlockChunks: 4, ParityPercent: 33}

func (f FecConfigFile) config() FecConfig {
	var cfg = DefaultFecConfig
	if f.ChunkPackets != 0 {
		cfg.ChunkPackets = f.ChunkPackets
	}
	if f.BlockChunks != 0 {
		cfg.BlockChunks = f.BlockChunks
	}
	if f.ParityPercent != 0 {
		cfg.ParityPercent = f.ParityPercent
	}
	return cfg
}

// ClockConfigFile is the [clock] section: the NTP server both ends take the
// time from for the end-to-end delay.
type ClockConfigFile struct {
	NTPServer string `toml:"ntp_server,omitempty"` // empty or not in the file: no NTP
}

// loadConfigFile decodes the config at path into dst, see decodeConfig.
func loadConfigFile(path string, dst any) error {
	text, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config %s: %w", path, err)
	}
	if err := decodeConfig(text, dst); err != nil {
		return fmt.Errorf("failed to parse config %s: %w", path, err)
	}
	return nil
}

// decodeConfig decodes a config into dst. Unknown keys are an error, so a
// typo or a section of an older layout is not silently ignored.
func decodeConfig(text []byte, dst any) error {
	if err := toml.NewDecoder(bytes.NewReader(text)).Strict(true).Decode(dst); err != nil {
		if tree, treeErr := toml.LoadBytes(text); treeErr == nil {
			switch {
			case tree.Has("proxy.addr"):
				return errors.New("the [proxy] section with the relay address is now called [relay]; [proxy] is for a socks5, http or amneziawg proxy, see the example config")
			case tree.Has("fec.data_parts"), tree.Has("fec.total_parts"), tree.Has("encoder.dispatcher_size"), tree.Has("encoder.packet_spread"), tree.Has("decoder.assembler_size"):
				return errors.New("the FEC settings changed to Reed-Solomon over blocks of chunks: the server's [fec] has chunk_packets, block_chunks and parity_percent, [encoder] only chunk_max_age; a client has no [fec] and no assembler_size; see the example config")
			}
		}
		return err
	}
	return nil
}

func LoadServerConfig(path string) (ServerConfig, error) {
	var f struct {
		Relay       RelayConfigFile `toml:"relay"`
		Proxy       ProxyConfigFile `toml:"proxy"`
		StreamInput struct {
			Port uint16 `toml:"port"`
		} `toml:"stream_input"`
		Fec     FecConfigFile `toml:"fec"`
		Encoder struct {
			ChunkMaxAge uint64 `toml:"chunk_max_age"` // milliseconds
		} `toml:"encoder"`
		Pacing struct {
			RateMbit *uint64 `toml:"rate_mbit"` // nil: not in the file, DefaultPacingRate
		} `toml:"pacing"`
		Clock ClockConfigFile `toml:"clock"`
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
			ChunkMaxAge: time.Duration(f.Encoder.ChunkMaxAge) * time.Millisecond,
		},
		NTPServer: f.Clock.NTPServer,
	}, nil
}

// ClientConfigFile is the layout of client.cfg. The Android app edits it
// field by field and writes it back with Marshal.
type ClientConfigFile struct {
	Relay   RelayConfigFile   `toml:"relay"`
	Proxy   ProxyConfigFile   `toml:"proxy"`
	Player  PlayerConfigFile  `toml:"player"`
	Decoder DecoderConfigFile `toml:"decoder"`
	Clock   ClockConfigFile   `toml:"clock"`
}

// PlayerConfigFile is the client's [player] section.
type PlayerConfigFile struct {
	Port uint16 `toml:"port"` // local UDP port the player reads the mpegts stream from
}

// DecoderConfigFile is the client's [decoder] section.
type DecoderConfigFile struct {
	DispatcherSize  uint64  `toml:"dispatcher_size"`
	ReorderDeadline *uint64 `toml:"reorder_deadline"` // milliseconds; 0: wait for dispatcher_size newer chunks; nil: not in the file, DefaultReorderDeadline
}

// DefaultReorderDeadline is the decoder's reorder_deadline when the client
// config does not set it.
const DefaultReorderDeadline = 30 * time.Millisecond

// defaultReorderDeadlineMilli is DefaultReorderDeadline as the file has it,
// for DefaultClientConfigFile; copied with the struct, never written to.
var defaultReorderDeadlineMilli = uint64(DefaultReorderDeadline.Milliseconds())

// reorderDeadline is the deadline the section sets, DefaultReorderDeadline
// when it sets none.
func (f DecoderConfigFile) reorderDeadline() time.Duration {
	if f.ReorderDeadline == nil {
		return DefaultReorderDeadline
	}
	return time.Duration(*f.ReorderDeadline) * time.Millisecond
}

// ReorderDeadlineMilli is reorderDeadline in milliseconds, for the Android
// app's form.
func (f DecoderConfigFile) ReorderDeadlineMilli() uint64 {
	return uint64(f.reorderDeadline().Milliseconds())
}

// DefaultClientConfigFile is the client config of cmd/client/client.example.cfg
// without its relay address, which only the server owner knows.
func DefaultClientConfigFile() ClientConfigFile {
	return ClientConfigFile{
		Relay:   RelayConfigFile{Port: 25565, Network: string(RelayNetworkUDP)},
		Player:  PlayerConfigFile{Port: 25566},
		Decoder: DecoderConfigFile{DispatcherSize: 12, ReorderDeadline: &defaultReorderDeadlineMilli},
	}
}

// ParseClientConfigFile decodes the text of a client.cfg.
func ParseClientConfigFile(text []byte) (ClientConfigFile, error) {
	var f ClientConfigFile
	if err := decodeConfig(text, &f); err != nil {
		return ClientConfigFile{}, err
	}
	return f, nil
}

// Marshal returns the text of the client.cfg, the sections in the order of
// the example config.
func (f ClientConfigFile) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Order(toml.OrderPreserve).Encode(f); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Validate checks the settings a client would fail on at start, so the
// Android app can refuse them in its form.
func (f ClientConfigFile) Validate() error {
	switch {
	case f.Relay.Addr == "":
		return errors.New("the relay address is not set")
	case f.Relay.Port == 0:
		return errors.New("the relay port is not set")
	case f.Player.Port == 0:
		return errors.New("the player port is not set")
	case f.Decoder.DispatcherSize == 0:
		return errors.New("the decoder's dispatcher size must be above zero")
	case f.Proxy.Socks5 != nil && f.Proxy.Socks5.URL == "", f.Proxy.HTTP != nil && f.Proxy.HTTP.URL == "":
		return errors.New("the proxy url is not set")
	case f.Proxy.AmneziaWG != nil && f.Proxy.AmneziaWG.Config == "":
		return errors.New("the amneziawg config is not set")
	}
	_, err := relayConfig(f.Relay, f.Proxy, "").withDefaults()
	return err
}

// Config makes the ClientConfig of the file at configPath; the AmneziaWG
// config path is relative to its directory.
func (f ClientConfigFile) Config(configPath string) ClientConfig {
	return ClientConfig{
		Relay:      relayConfig(f.Relay, f.Proxy, configPath),
		PlayerPort: f.Player.Port,
		Decoder: DecoderConfig{
			DispatcherSize:  f.Decoder.DispatcherSize,
			ReorderDeadline: f.Decoder.reorderDeadline(),
		},
		NTPServer: f.Clock.NTPServer,
	}
}

func LoadClientConfig(path string) (ClientConfig, error) {
	var f ClientConfigFile
	if err := loadConfigFile(path, &f); err != nil {
		return ClientConfig{}, err
	}
	return f.Config(path), nil
}
