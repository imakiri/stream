package src

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/imakiri/stream/src/amneziawg"
	"golang.org/x/net/proxy"
)

// ProxyConfig is a way to the relay through a proxy, for networks where the
// relay cannot be reached directly or its traffic is blocked. At most one of
// the fields is set; none means a direct connection.
type ProxyConfig struct {
	Socks5    *Socks5ProxyConfig
	HTTP      *HTTPProxyConfig
	AmneziaWG *AmneziaWGProxyConfig
}

// Socks5ProxyConfig reaches the relay through a SOCKS5 proxy, such as a
// local sing-box. TCP only: UDP over SOCKS5 (UDP ASSOCIATE) is not
// supported.
type Socks5ProxyConfig struct {
	URL string // e.g. socks5://127.0.0.1:2080
}

// HTTPProxyConfig reaches the relay through an HTTP CONNECT proxy. TCP only:
// a CONNECT proxy tunnels TCP, it does not forward UDP.
type HTTPProxyConfig struct {
	URL string // e.g. http://127.0.0.1:8080; userinfo becomes Proxy-Authorization
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
	var n int
	if cfg.Socks5 != nil {
		n++
	}
	if cfg.HTTP != nil {
		n++
	}
	if cfg.AmneziaWG != nil {
		n++
	}
	if n > 1 {
		return errors.New("set one proxy at most: socks5, http or amneziawg")
	}
	if cfg.Socks5 != nil && network != RelayNetworkTCP {
		return fmt.Errorf("the socks5 proxy works only over %q, relay network is %q", RelayNetworkTCP, network)
	}
	if cfg.HTTP != nil && network != RelayNetworkTCP {
		return fmt.Errorf("the http proxy works only over %q, relay network is %q", RelayNetworkTCP, network)
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
	case cfg.HTTP != nil:
		proxyURL, err := url.Parse(cfg.HTTP.URL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse http proxy url: %w", err)
		}
		if proxyURL.Scheme != "http" {
			return nil, fmt.Errorf("http proxy url scheme is %q, want %q", proxyURL.Scheme, "http")
		}
		if proxyURL.Host == "" {
			return nil, fmt.Errorf("http proxy url %s has no host", proxyURL.Redacted())
		}
		return httpProxyDialer{proxyURL: proxyURL}, nil
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
	case cfg.HTTP != nil:
		if proxyURL, err := url.Parse(cfg.HTTP.URL); err == nil {
			return "http proxy " + proxyURL.Redacted()
		}
		return "http proxy"
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

type httpProxyDialer struct {
	proxyURL *url.URL
}

func (httpProxyDialer) Close() error { return nil }

// DialContext connects to addr through an HTTP CONNECT proxy.
func (d httpProxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("http proxy works only over tcp, got %s", network)
	}
	proxyAddr := d.proxyURL.Host
	if _, _, err := net.SplitHostPort(proxyAddr); err != nil {
		proxyAddr = net.JoinHostPort(proxyAddr, "80")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial http proxy %s: %w", d.proxyURL.Redacted(), err)
	}
	// Let ctx cancel interrupt the handshake.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: addr},
		Host:   addr,
		Header: make(http.Header),
	}
	if user := d.proxyURL.User; user != nil {
		username := user.Username()
		password, _ := user.Password()
		creds := username + ":" + password
		req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(creds)))
	}
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to send CONNECT to http proxy %s: %w", d.proxyURL.Redacted(), err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to read CONNECT response from http proxy %s: %w", d.proxyURL.Redacted(), err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_ = conn.Close()
		return nil, fmt.Errorf("http proxy %s refused CONNECT to %s: %s", d.proxyURL.Redacted(), addr, resp.Status)
	}
	_ = conn.SetDeadline(time.Time{})
	return &httpProxyConn{Conn: conn, r: br}, nil
}

// httpProxyConn keeps the bytes http.ReadResponse may have buffered after the
// CONNECT response.
type httpProxyConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *httpProxyConn) Read(b []byte) (int, error) { return c.r.Read(b) }
