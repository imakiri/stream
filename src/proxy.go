package src

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/imakiri/stream/src/amneziawg"
	"golang.org/x/net/proxy"
)

// ProxyConfig is a way to the relay through a proxy, for networks where the
// relay cannot be reached directly or its traffic is blocked. At most one of
// the fields is set; none means a direct connection.
type ProxyConfig struct {
	Socks5    *Socks5ProxyConfig
	AmneziaWG *AmneziaWGProxyConfig
}

// Socks5ProxyConfig reaches the relay through a SOCKS5 proxy, such as a
// local sing-box. TCP only: UDP over SOCKS5 (UDP ASSOCIATE) is not
// supported.
type Socks5ProxyConfig struct {
	URL string // e.g. socks5://127.0.0.1:2080
}

// AmneziaWGProxyConfig reaches the relay through an AmneziaWG tunnel run
// inside the process (see package amneziawg), over UDP or TCP. Only the
// stream goes into the tunnel, the rest of the machine's traffic does not.
type AmneziaWGProxyConfig struct {
	ConfigPath string // AmneziaWG or WireGuard config, as the Amnezia app exports it
}

// proxyDialer dials the relay through a proxy.
type proxyDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
	Close() error
}

func (cfg ProxyConfig) validate(network RelayNetwork) error {
	if cfg.Socks5 != nil && cfg.AmneziaWG != nil {
		return errors.New("set one proxy at most: socks5 or amneziawg")
	}
	if cfg.Socks5 != nil && network != RelayNetworkTCP {
		return fmt.Errorf("the socks5 proxy works only over %q, relay network is %q", RelayNetworkTCP, network)
	}
	return nil
}

// open returns the dialer of the configured proxy, or nil for a direct
// connection. An AmneziaWG tunnel comes up here and stays up until Close.
func (cfg ProxyConfig) open(ctx context.Context) (proxyDialer, error) {
	switch {
	case cfg.Socks5 != nil:
		socksURL, err := url.Parse(cfg.Socks5.URL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse socks5 proxy url: %w", err)
		}
		dialer, err := proxy.FromURL(socksURL, &net.Dialer{})
		if err != nil {
			return nil, fmt.Errorf("failed to create socks5 dialer for %s: %w", socksURL.Redacted(), err)
		}
		contextDialer, ok := dialer.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("socks5 dialer for %s does not support context", socksURL.Redacted())
		}
		return socks5Dialer{contextDialer}, nil
	case cfg.AmneziaWG != nil:
		tunnelConfig, err := amneziawg.LoadConfig(cfg.AmneziaWG.ConfigPath)
		if err != nil {
			return nil, err
		}
		tunnel, err := amneziawg.Open(ctx, tunnelConfig)
		if err != nil {
			return nil, err
		}
		return tunnel, nil
	default:
		return nil, nil
	}
}

func (cfg ProxyConfig) String() string {
	switch {
	case cfg.Socks5 != nil:
		if socksURL, err := url.Parse(cfg.Socks5.URL); err == nil {
			return "socks5 proxy " + socksURL.Redacted()
		}
		return "socks5 proxy"
	case cfg.AmneziaWG != nil:
		return "amneziawg tunnel " + cfg.AmneziaWG.ConfigPath
	default:
		return "no proxy"
	}
}

type socks5Dialer struct {
	proxy.ContextDialer
}

func (socks5Dialer) Close() error { return nil }
