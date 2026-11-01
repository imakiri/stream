// Package streammobile is the stream client for the Android app, bound with
// gomobile (see android/build-go.sh). Its API keeps to the types gomobile
// binds: strings, ints, errors, structs by pointer and interfaces.
//
// A profile is one directory, the app's files dir for it: client.cfg, the
// AmneziaWG config if any, client.key and client.enrollment.
package streammobile

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/imakiri/stream/src"
)

// ConfigFileName is the client config in a profile directory.
const ConfigFileName = "client.cfg"

// Proxy kinds of Profile.ProxyKind.
const (
	ProxyNone      = ""
	ProxySocks5    = "socks5"
	ProxyHTTP      = "http"
	ProxyAmneziaWG = "amneziawg"
)

// Profile is client.cfg as flat fields for the app's form.
type Profile struct {
	RelayAddr    string
	RelayPort    int
	RelayNetwork string // "udp" or "tcp"

	ProxyKind       string // ProxyNone, ProxySocks5, ProxyHTTP or ProxyAmneziaWG
	ProxyURL        string // for socks5 and http
	AmneziaWGConfig string // for amneziawg: a file name in the profile directory

	PlayerPort int // local UDP port the player reads the stream from

	DecoderDispatcherSize  int
	DecoderReorderDeadline int // milliseconds; 0: none

	NTPServer string // empty: no NTP
}

// DefaultProfile is the profile of the example client config, without the
// relay address.
func DefaultProfile() *Profile {
	return profileOf(src.DefaultClientConfigFile())
}

// ParseProfile reads the text of a client.cfg, as the app imports it.
func ParseProfile(text string) (*Profile, error) {
	f, err := src.ParseClientConfigFile([]byte(text))
	if err != nil {
		return nil, err
	}
	return profileOf(f), nil
}

// LoadProfile reads the profile in dir; nil, nil if there is none yet.
func LoadProfile(dir string) (*Profile, error) {
	text, err := os.ReadFile(filepath.Join(dir, ConfigFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ParseProfile(string(text))
}

// Save checks the profile and writes it to dir as client.cfg.
func (p *Profile) Save(dir string) error {
	var f = p.configFile()
	if err := f.Validate(); err != nil {
		return err
	}
	if f.Proxy.AmneziaWG != nil {
		if _, err := os.Stat(filepath.Join(dir, f.Proxy.AmneziaWG.Config)); err != nil {
			return fmt.Errorf("the amneziawg config: %w", err)
		}
	}
	text, err := f.Marshal()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	var path = filepath.Join(dir, ConfigFileName)
	if err := os.WriteFile(path+".new", text, 0o600); err != nil {
		return err
	}
	return os.Rename(path+".new", path)
}

func profileOf(f src.ClientConfigFile) *Profile {
	var p = &Profile{
		RelayAddr:              f.Relay.Addr,
		RelayPort:              int(f.Relay.Port),
		RelayNetwork:           f.Relay.Network,
		PlayerPort:             int(f.Player.Port),
		DecoderDispatcherSize:  int(f.Decoder.DispatcherSize),
		DecoderReorderDeadline: int(f.Decoder.ReorderDeadlineMilli()),
		NTPServer:              f.Clock.NTPServer,
	}
	if p.RelayNetwork == "" {
		p.RelayNetwork = "udp"
	}
	switch {
	case f.Proxy.Socks5 != nil:
		p.ProxyKind, p.ProxyURL = ProxySocks5, f.Proxy.Socks5.URL
	case f.Proxy.HTTP != nil:
		p.ProxyKind, p.ProxyURL = ProxyHTTP, f.Proxy.HTTP.URL
	case f.Proxy.AmneziaWG != nil:
		p.ProxyKind, p.AmneziaWGConfig = ProxyAmneziaWG, f.Proxy.AmneziaWG.Config
	}
	return p
}

func (p *Profile) configFile() src.ClientConfigFile {
	var reorderDeadline = uint64(p.DecoderReorderDeadline)
	var f = src.ClientConfigFile{
		Relay:   src.RelayConfigFile{Addr: strings.TrimSpace(p.RelayAddr), Port: uint16(p.RelayPort), Network: p.RelayNetwork},
		Player:  src.PlayerConfigFile{Port: uint16(p.PlayerPort)},
		Decoder: src.DecoderConfigFile{DispatcherSize: uint64(p.DecoderDispatcherSize), ReorderDeadline: &reorderDeadline},
		Clock:   src.ClockConfigFile{NTPServer: strings.TrimSpace(p.NTPServer)},
	}
	switch p.ProxyKind {
	case ProxySocks5:
		f.Proxy.Socks5 = &src.ProxyURLConfigFile{URL: strings.TrimSpace(p.ProxyURL)}
	case ProxyHTTP:
		f.Proxy.HTTP = &src.ProxyURLConfigFile{URL: strings.TrimSpace(p.ProxyURL)}
	case ProxyAmneziaWG:
		f.Proxy.AmneziaWG = &src.AmneziaWGConfigFile{Config: p.AmneziaWGConfig}
	}
	return f
}

// ClientPublicKey returns the client's public key in base64, the text to
// send to the server owner for server -sign; the key is made on the first
// call.
func ClientPublicKey(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	publicKey, _, err := src.LoadOrCreateClientKey(dir)
	return publicKey, err
}

// Enroll checks and saves the enrollment the server owner sent back and
// describes it, see EnrolledAs.
func Enroll(dir, enrollment string) (string, error) {
	identity, err := src.SaveClientEnrollment(dir, enrollment)
	if err != nil {
		return "", err
	}
	return describe(identity), nil
}

// EnrolledAs describes the enrollment in dir, the client ID and until when
// it is valid; "" while the client is not enrolled.
func EnrolledAs(dir string) (string, error) {
	identity, err := src.LoadClientIdentity(dir)
	if errors.Is(err, src.ErrClientNotEnrolled) || errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return describe(identity), nil
}

func describe(identity src.ClientIdentity) string {
	return fmt.Sprintf("client %s, valid until %s", identity.Credentials.Certificate.ID, identity.Credentials.Certificate.NotAfter.Format(time.DateOnly))
}

// DeleteProfile removes the profile directory with its keys.
func DeleteProfile(dir string) error {
	return os.RemoveAll(dir)
}

// Listener gets what the client tells while it runs; it is called from Go
// threads.
type Listener interface {
	// Log gets each line of the client log: handshakes, the end-to-end
	// delay and losses every 5 s, link breaks.
	Log(line string)
	// Speed gets the rate of what comes from the relay, every second.
	Speed(kbitPerSecond int)
}

// Session is a running client.
type Session struct {
	client *src.Client
	cancel context.CancelFunc
	done   chan struct{}
}

// sessionMu keeps one session at a time: two would take the same client
// slot on the relay from each other.
var (
	sessionMu sync.Mutex
	running   *Session
)

// Start runs the client of the profile in dir until Stop; a running session
// is stopped first. It returns once the client is set up; errors after that
// go to the log.
func Start(dir string, listener Listener) (*Session, error) {
	sessionMu.Lock()
	defer sessionMu.Unlock()
	if running != nil {
		running.stop()
		running = nil
	}

	identity, err := src.LoadClientIdentity(dir)
	if err != nil {
		return nil, err
	}
	config, err := src.LoadClientConfig(filepath.Join(dir, ConfigFileName))
	if err != nil {
		return nil, err
	}
	config.Credentials = identity.Credentials
	config.Verifier = identity.Verifier()
	config.ServerID = identity.Enrollment.ServerID
	config.InputReporter = speedReporter{listener}
	client, err := src.NewClient(config)
	if err != nil {
		return nil, err
	}

	log.SetFlags(log.Ltime)
	log.SetOutput(lineWriter{listener})
	log.Printf("%s", describe(identity))

	ctx, cancel := context.WithCancel(context.Background())
	var s = &Session{client: client, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		if err := client.Run(ctx); err != nil {
			log.Printf("client stopped: %s", err)
		}
	}()
	running = s
	return s, nil
}

// Stop stops the client and waits until it has.
func (s *Session) Stop() {
	sessionMu.Lock()
	defer sessionMu.Unlock()
	s.stop()
	if running == s {
		running = nil
	}
}

// StartTrace starts writing the latency trace of the client to path, see
// cmd/latency-report.
func (s *Session) StartTrace(path string) error {
	return s.client.StartTrace(path)
}

// StopTrace stops the latency trace and writes it out.
func (s *Session) StopTrace() error {
	return s.client.StopTrace()
}

// TrueUnixMicro returns the current NTP time in unix microseconds; an
// error while the clock has not synced, or NTP is off in the profile.
func (s *Session) TrueUnixMicro() (int64, error) {
	now, synced := s.client.TrueNow()
	if !synced {
		return 0, errors.New("the clock is not synced to NTP: set the NTP server in the profile")
	}
	return now.UnixMicro(), nil
}

func (s *Session) stop() {
	s.cancel()
	<-s.done
}

type speedReporter struct{ listener Listener }

func (r speedReporter) Report(total, delta uint64) {
	r.listener.Speed(int(delta * 8 / 1000))
}

// lineWriter hands each log line to the listener.
type lineWriter struct{ listener Listener }

func (w lineWriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		w.listener.Log(line)
	}
	return len(p), nil
}
