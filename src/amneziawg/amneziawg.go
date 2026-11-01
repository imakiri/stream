// Package amneziawg runs an AmneziaWG tunnel inside the process: the
// WireGuard protocol with the AmneziaWG obfuscation (junk packets, padded
// handshakes, changed message headers), over a user-space network stack.
// Connections dialed through a Tunnel go into the tunnel; the rest of the
// machine's traffic does not, and no system VPN or root rights are needed.
package amneziawg

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"

	"github.com/amnezia-vpn/amneziawg-go/conn"
	"github.com/amnezia-vpn/amneziawg-go/device"
	"github.com/amnezia-vpn/amneziawg-go/tun/netstack"
)

// DefaultMTU is the MTU of wg-quick and of the Amnezia app when the config
// sets none.
const DefaultMTU = 1420

// Config is a parsed AmneziaWG config file.
type Config struct {
	Addresses  []netip.Addr // the tunnel addresses of this side, [Interface] Address
	DNSServers []netip.Addr // [Interface] DNS, used to resolve names inside the tunnel
	MTU        int
	// uapi is the rest of the config in the form amneziawg-go takes it
	// (device.IpcSet): keys in hex, one peer section per [Peer]. Peer
	// endpoints that are host names are resolved by Open, so they are kept
	// apart until then.
	uapiDevice string
	peers      []peerConfig
}

type peerConfig struct {
	uapi     string // the peer lines without the endpoint
	endpoint string // host:port, may be empty
}

// wgQuickOnlyKeys are [Interface] keys that wg-quick and the Amnezia app
// act on themselves (routes, firewall, scripts); a tunnel in the process
// has no use for them.
var wgQuickOnlyKeys = map[string]bool{
	"table": true, "preup": true, "postup": true, "predown": true, "postdown": true,
	"saveconfig": true, "fwmark": true,
}

// ParseConfig parses an AmneziaWG config in the wg-quick format, as the
// Amnezia app exports it:
//
//	[Interface]
//	PrivateKey = <base64>
//	Address = 10.8.1.2/32
//	DNS = 1.1.1.1
//	Jc = 4
//	Jmin = 40
//	Jmax = 70
//	S1 = 0
//	S2 = 0
//	H1 = 1
//	...
//	[Peer]
//	PublicKey = <base64>
//	PresharedKey = <base64>
//	AllowedIPs = 0.0.0.0/0
//	Endpoint = vpn.example.com:51820
//	PersistentKeepalive = 25
//
// The obfuscation keys (Jc, Jmin, Jmax, S1-S4, H1-H4, I1-I5, ...) go to
// amneziawg-go as they are, so keys of newer AmneziaWG versions work if the
// library knows them. A plain WireGuard config works too.
func ParseConfig(r io.Reader) (Config, error) {
	var cfg = Config{MTU: DefaultMTU}
	var device strings.Builder
	var section string
	var peer *peerConfig
	var scanner = bufio.NewScanner(r)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		var line = strings.TrimSpace(scanner.Text())
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			switch section {
			case "interface":
			case "peer":
				cfg.peers = append(cfg.peers, peerConfig{})
				peer = &cfg.peers[len(cfg.peers)-1]
			default:
				return Config{}, fmt.Errorf("line %d: unknown section [%s]", lineNumber, section)
			}
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return Config{}, fmt.Errorf("line %d: want key = value", lineNumber)
		}
		key, value = strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(value)
		var err error
		switch section {
		case "interface":
			err = cfg.setInterfaceKey(&device, key, value)
		case "peer":
			err = peer.setKey(key, value)
		default:
			err = fmt.Errorf("%s outside of [Interface] and [Peer]", key)
		}
		if err != nil {
			return Config{}, fmt.Errorf("line %d: %w", lineNumber, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return Config{}, err
	}
	if !strings.Contains(device.String(), "private_key=") {
		return Config{}, fmt.Errorf("no PrivateKey in [Interface]")
	}
	if len(cfg.Addresses) == 0 {
		return Config{}, fmt.Errorf("no Address in [Interface]")
	}
	if len(cfg.peers) == 0 {
		return Config{}, fmt.Errorf("no [Peer]")
	}
	cfg.uapiDevice = device.String()
	return cfg, nil
}

func (cfg *Config) setInterfaceKey(device *strings.Builder, key, value string) error {
	switch key {
	case "privatekey":
		hexKey, err := keyToHex(value)
		if err != nil {
			return fmt.Errorf("PrivateKey: %w", err)
		}
		fmt.Fprintf(device, "private_key=%s\n", hexKey)
	case "address":
		for _, item := range splitList(value) {
			prefix, err := netip.ParsePrefix(item)
			if err != nil {
				addr, addrErr := netip.ParseAddr(item)
				if addrErr != nil {
					return fmt.Errorf("Address %q: %w", item, err)
				}
				prefix = netip.PrefixFrom(addr, addr.BitLen())
			}
			cfg.Addresses = append(cfg.Addresses, prefix.Addr())
		}
	case "dns":
		for _, item := range splitList(value) {
			// wg-quick also takes search domains here; only addresses matter
			if addr, err := netip.ParseAddr(item); err == nil {
				cfg.DNSServers = append(cfg.DNSServers, addr)
			}
		}
	case "mtu":
		mtu, err := strconv.Atoi(value)
		if err != nil || mtu < 576 || mtu > 65535 {
			return fmt.Errorf("MTU %q: want a number from 576 to 65535", value)
		}
		cfg.MTU = mtu
	case "listenport":
		fmt.Fprintf(device, "listen_port=%s\n", value)
	default:
		if wgQuickOnlyKeys[key] {
			return nil
		}
		// the obfuscation settings: jc, jmin, jmax, s1..s4, h1..h4, i1..i5
		fmt.Fprintf(device, "%s=%s\n", key, value)
	}
	return nil
}

func (peer *peerConfig) setKey(key, value string) error {
	var uapi strings.Builder
	switch key {
	case "publickey", "presharedkey":
		hexKey, err := keyToHex(value)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		if key == "publickey" {
			// public_key starts the peer section of the UAPI config
			peer.uapi = fmt.Sprintf("public_key=%s\n", hexKey) + peer.uapi
			return nil
		}
		fmt.Fprintf(&uapi, "preshared_key=%s\n", hexKey)
	case "allowedips":
		for _, item := range splitList(value) {
			fmt.Fprintf(&uapi, "allowed_ip=%s\n", item)
		}
	case "endpoint":
		peer.endpoint = value
	case "persistentkeepalive":
		if value == "off" {
			value = "0"
		}
		fmt.Fprintf(&uapi, "persistent_keepalive_interval=%s\n", value)
	default:
		fmt.Fprintf(&uapi, "%s=%s\n", key, value)
	}
	peer.uapi += uapi.String()
	return nil
}

func splitList(value string) []string {
	var items []string
	for item := range strings.SplitSeq(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

// keyToHex turns a base64 key of a config file into the hex amneziawg-go
// takes.
func keyToHex(value string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", fmt.Errorf("not base64: %w", err)
	}
	if len(key) != 32 {
		return "", fmt.Errorf("want a 32-byte key, got %d bytes", len(key))
	}
	return hex.EncodeToString(key), nil
}

// LoadConfig reads and parses the config file at path.
func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read amneziawg config: %w", err)
	}
	defer file.Close()
	cfg, err := ParseConfig(file)
	if err != nil {
		return Config{}, fmt.Errorf("failed to parse amneziawg config %s: %w", path, err)
	}
	return cfg, nil
}

// Tunnel is a running AmneziaWG tunnel.
type Tunnel struct {
	device *device.Device
	net    *netstack.Net
	dns    bool // the config names DNS servers to resolve names in the tunnel
}

// Open brings the tunnel up. The handshake with the peer happens on the
// first packet sent, so a wrong key or an unreachable endpoint shows up as
// dials that time out, not as an error here. Peer endpoints given by name
// are resolved now, by the system resolver.
func Open(ctx context.Context, cfg Config) (*Tunnel, error) {
	var uapi strings.Builder
	uapi.WriteString(cfg.uapiDevice)
	uapi.WriteString("replace_peers=true\n")
	for _, peer := range cfg.peers {
		uapi.WriteString(peer.uapi)
		if peer.endpoint != "" {
			endpoint, err := resolveEndpoint(ctx, peer.endpoint)
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(&uapi, "endpoint=%s\n", endpoint)
		}
	}

	tunDevice, tunNet, err := netstack.CreateNetTUN(cfg.Addresses, cfg.DNSServers, cfg.MTU)
	if err != nil {
		return nil, fmt.Errorf("failed to create the tunnel network stack: %w", err)
	}
	var logger = &device.Logger{
		Verbosef: device.DiscardLogf,
		Errorf: func(format string, args ...any) {
			log.Printf("amneziawg: "+format, args...)
		},
	}
	var dev = device.NewDevice(tunDevice, conn.NewDefaultBind(), logger)
	if err := dev.IpcSet(uapi.String()); err != nil {
		dev.Close()
		return nil, fmt.Errorf("failed to configure amneziawg: %w", err)
	}
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, fmt.Errorf("failed to bring amneziawg up: %w", err)
	}
	return &Tunnel{device: dev, net: tunNet, dns: len(cfg.DNSServers) > 0}, nil
}

func resolveEndpoint(ctx context.Context, endpoint string) (string, error) {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", fmt.Errorf("endpoint %q: %w", endpoint, err)
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return endpoint, nil
	}
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
	if err != nil || len(addrs) == 0 {
		addrs, err = net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	}
	if err != nil {
		return "", fmt.Errorf("failed to resolve endpoint %s: %w", host, err)
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("failed to resolve endpoint %s: no addresses", host)
	}
	return net.JoinHostPort(addrs[0].Unmap().String(), port), nil
}

// DialContext connects to addr (host:port) through the tunnel; network is
// "udp", "udp4", "tcp" or "tcp4" and the like. A host name is resolved by
// the DNS servers of the config, through the tunnel, or by the system
// resolver if the config names none.
func (t *Tunnel) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if _, err := netip.ParseAddr(host); err != nil && !t.dns {
		addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve %s: %w", host, err)
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("failed to resolve %s: no addresses", host)
		}
		addr = net.JoinHostPort(addrs[0].Unmap().String(), port)
	}
	return t.net.DialContext(ctx, network, addr)
}

// ListenUDP opens a UDP socket inside the tunnel, at one of its addresses.
func (t *Tunnel) ListenUDP(addr netip.AddrPort) (net.PacketConn, error) {
	return t.net.ListenUDPAddrPort(addr)
}

// ListenTCP listens on TCP inside the tunnel, at one of its addresses.
func (t *Tunnel) ListenTCP(addr netip.AddrPort) (net.Listener, error) {
	return t.net.ListenTCPAddrPort(addr)
}

// Close takes the tunnel down; connections through it fail.
func (t *Tunnel) Close() error {
	t.device.Close()
	return nil
}
