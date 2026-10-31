package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/gofrs/uuid/v5"
	"github.com/gosuri/uilive"
	"github.com/imakiri/stream/src"
	"github.com/imakiri/stream/src/frame"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// handshakeSize is the size of the hello a client sends first and the server
// echoes back: 16 bytes of src.UID followed by the 16-byte peer ID.
const handshakeSize = 32

// packetBufferSize fits any UDP datagram and any frame.
const packetBufferSize = 1<<16 - 1

const tcpWriteTimeout = time.Second

// udpPeerTimeout drops a UDP peer that sent nothing, not even a keepalive
// hello (src.KeepaliveInterval), for this long. A TCP peer is dropped when its
// connection closes instead.
const udpPeerTimeout = 30 * time.Second

// Peer is one connected client. A peer is reached either over UDP, by the
// address its handshake came from, or over TCP, through its framed connection.
type Peer struct {
	id      uuid.UUID
	addr    net.Addr
	udpAddr *net.UDPAddr // set for UDP peers
	tcpConn *frame.Conn  // set for TCP peers

	lastSeenUnixNano atomic.Int64 // UDP peers: when the last packet came from udpAddr
}

func (p *Peer) seen(now time.Time) {
	p.lastSeenUnixNano.Store(now.UnixNano())
}

func (p *Peer) network() string {
	if p.tcpConn != nil {
		return "tcp"
	}
	return "udp"
}

func (p *Peer) close() {
	if p.tcpConn != nil {
		_ = p.tcpConn.Close()
	}
}

// Server relays every packet from a peer to all other peers, whichever
// transport each of them used to connect: it listens on UDP and TCP at the
// same address.
type Server struct {
	mu     sync.RWMutex
	peers  []*Peer
	closed bool // set by Close under mu, so no peer is added after Close took its snapshot

	done        chan struct{}
	wg          sync.WaitGroup
	udpConn     *net.UDPConn
	tcpListener net.Listener

	udpPeerTimeout time.Duration

	addr                string
	logger              *uilive.Writer
	totalReceivedBefore uint64
	totalSentBefore     uint64
	totalReceived       atomic.Uint64
	totalSent           atomic.Uint64
}

func NewServer(addr string, logger *uilive.Writer) *Server {
	return &Server{addr: addr, logger: logger, udpPeerTimeout: udpPeerTimeout}
}

// addPeer registers the peer, replacing a previous connection with the same ID.
// It returns false, and closes the peer, if the server is closing.
func (s *Server) addPeer(peer *Peer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		peer.close()
		return false
	}
	for i := range s.peers {
		if s.peers[i].id == peer.id {
			s.peers[i].close()
			s.peers[i] = peer
			log.Printf("updated peer: id %s at %s/%s", peer.id, peer.network(), peer.addr)
			return true
		}
	}
	s.peers = append(s.peers, peer)
	log.Printf("added peer: id %s at %s/%s", peer.id, peer.network(), peer.addr)
	return true
}

// removePeer removes this very peer; it does nothing if the peer has already
// been replaced by a newer connection with the same ID.
func (s *Server) removePeer(peer *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var i = slices.Index(s.peers, peer)
	if i < 0 {
		return
	}
	peer.close()
	s.peers = slices.Delete(s.peers, i, i+1)
	log.Printf("removed peer: id %s at %s/%s", peer.id, peer.network(), peer.addr)
}

func (s *Server) appendPeers(dst []*Peer) []*Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append(dst, s.peers...)
}

func (s *Server) findUDPPeer(addr *net.UDPAddr) *Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var addrPort = addr.AddrPort()
	for _, peer := range s.peers {
		if peer.udpAddr != nil && peer.udpAddr.AddrPort() == addrPort {
			return peer
		}
	}
	return nil
}

// parseHandshake returns the peer ID from a client hello, or uuid.Nil if
// hello is not one.
func parseHandshake(hello []byte) uuid.UUID {
	if len(hello) != handshakeSize || string(hello[:16]) != string(src.UID) {
		return uuid.Nil
	}
	return uuid.FromBytesOrNil(hello[16:])
}

func (s *Server) sendToPeer(peer *Peer, packet []byte) error {
	if peer.tcpConn != nil {
		_ = peer.tcpConn.SetWriteDeadline(time.Now().Add(tcpWriteTimeout))
		_, err := peer.tcpConn.Write(packet)
		return err
	}
	n, err := s.udpConn.WriteToUDP(packet, peer.udpAddr)
	if err == nil && n != len(packet) {
		err = fmt.Errorf("written %d of %d bytes", n, len(packet))
	}
	return err
}

// relay sends the packet to every peer except the sender. A TCP peer that
// fails to take it is dropped; UDP has no connection to drop.
func (s *Server) relay(from *Peer, packet []byte, peers []*Peer) []*Peer {
	s.totalReceived.Add(uint64(len(packet)))
	peers = s.appendPeers(peers[:0])
	for _, peer := range peers {
		if peer.id == from.id {
			continue
		}
		if err := s.sendToPeer(peer, packet); err != nil {
			log.Printf("failed to send to peer %s at %s/%s: %s", peer.id, peer.network(), peer.addr, err)
			if peer.tcpConn != nil {
				s.removePeer(peer)
			}
			continue
		}
		s.totalSent.Add(uint64(len(packet)))
	}
	return peers
}

func (s *Server) serveUDP() {
	var buf = make([]byte, packetBufferSize)
	var peers []*Peer
	for {
		n, addr, err := s.udpConn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("failed to read udp: %s", err)
			continue
		}

		if peerID := parseHandshake(buf[:n]); peerID != uuid.Nil {
			// A hello is both the first handshake and the keepalive: the
			// same peer from the same address only refreshes lastSeen.
			if peer := s.findUDPPeer(addr); peer != nil && peer.id == peerID {
				peer.seen(time.Now())
			} else {
				var peer = &Peer{id: peerID, addr: addr, udpAddr: addr}
				peer.seen(time.Now())
				if !s.addPeer(peer) {
					return
				}
			}
			if _, err := s.udpConn.WriteToUDP(buf[:n], addr); err != nil {
				log.Printf("failed to answer udp handshake from %s: %s", addr, err)
			}
			continue
		}

		var from = s.findUDPPeer(addr)
		if from == nil {
			log.Printf("dropped udp packet from unknown peer at %s", addr)
			continue
		}
		from.seen(time.Now())
		peers = s.relay(from, buf[:n], peers)
	}
}

func (s *Server) serveTCP() {
	for {
		conn, err := s.tcpListener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("failed to accept tcp connection: %s", err)
			continue
		}
		s.wg.Go(func() { s.serveTCPConn(conn) })
	}
}

func (s *Server) serveTCPConn(conn net.Conn) {
	var addr = conn.RemoteAddr()
	var hello = make([]byte, handshakeSize)

	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(conn, hello); err != nil {
		_ = conn.Close()
		log.Printf("failed tcp handshake from %s: %s", addr, err)
		return
	}
	var peerID = parseHandshake(hello)
	if peerID == uuid.Nil {
		_ = conn.Close()
		log.Printf("failed tcp handshake from %s: bad hello", addr)
		return
	}
	if _, err := conn.Write(hello); err != nil {
		_ = conn.Close()
		log.Printf("failed tcp handshake from %s: %s", addr, err)
		return
	}
	_ = conn.SetDeadline(time.Time{})

	var peer = &Peer{id: peerID, addr: addr, tcpConn: frame.NewConn(conn)}
	if !s.addPeer(peer) {
		return
	}

	var buf = make([]byte, packetBufferSize)
	var peers []*Peer
	for {
		n, err := peer.tcpConn.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("failed to read from peer %s at tcp/%s: %s", peer.id, addr, err)
			}
			s.removePeer(peer)
			return
		}
		peers = s.relay(peer, buf[:n], peers)
	}
}

// expireUDPPeers drops UDP peers that went silent for udpPeerTimeout.
func (s *Server) expireUDPPeers() {
	var ticker = time.NewTicker(time.Second)
	defer ticker.Stop()
	var peers []*Peer
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			peers = s.appendPeers(peers[:0])
			for _, peer := range peers {
				if peer.udpAddr == nil {
					continue
				}
				var silent = now.Sub(time.Unix(0, peer.lastSeenUnixNano.Load()))
				if silent > s.udpPeerTimeout {
					log.Printf("peer %s at udp/%s silent for %s", peer.id, peer.addr, silent.Round(time.Second))
					s.removePeer(peer)
				}
			}
		}
	}
}

func (s *Server) reportSpeed() {
	var ticker = time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			var totalReceived = s.totalReceived.Load()
			var totalSent = s.totalSent.Load()

			if totalReceived > s.totalReceivedBefore {
				_, _ = fmt.Fprintf(s.logger.Newline(), "incoming speed: %6d KBit/sec, outgoing speed: %6d KBit/sec, transmited: %6d MB\n",
					8*(totalReceived-s.totalReceivedBefore)/(1<<10), 8*(totalSent-s.totalSentBefore)/(1<<10), totalReceived/(1<<20))
			}

			s.totalReceivedBefore = totalReceived
			s.totalSentBefore = totalSent
		}
	}
}

// Serve starts listening on UDP and TCP at the server address.
func (s *Server) Serve() error {
	if s.done != nil {
		return errors.New("server can be started only once")
	}

	udpAddr, err := net.ResolveUDPAddr("udp4", s.addr)
	if err != nil {
		return fmt.Errorf("failed to resolve udp address %s: %w", s.addr, err)
	}
	udpConn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen udp at %s: %w", s.addr, err)
	}
	tcpListener, err := net.Listen("tcp4", s.addr)
	if err != nil {
		_ = udpConn.Close()
		return fmt.Errorf("failed to listen tcp at %s: %w", s.addr, err)
	}
	log.Printf("listening at udp/%s and tcp/%s", udpConn.LocalAddr(), tcpListener.Addr())

	s.done = make(chan struct{})
	s.udpConn = udpConn
	s.tcpListener = tcpListener

	s.wg.Go(s.reportSpeed)
	s.wg.Go(s.expireUDPPeers)
	s.wg.Go(s.serveUDP)
	s.wg.Go(s.serveTCP)
	return nil
}

func (s *Server) Close() {
	close(s.done)
	_ = s.udpConn.Close()
	_ = s.tcpListener.Close()

	s.mu.Lock()
	s.closed = true
	var peers = slices.Clone(s.peers)
	s.mu.Unlock()
	for _, peer := range peers {
		s.removePeer(peer)
	}
	s.wg.Wait()
}

func main() {
	var addr = flag.String("addr", "127.0.0.1:25565", "address to listen at, over both udp and tcp")
	flag.Parse()

	var logger = uilive.New()
	logger.Start()
	logger.RefreshInterval = time.Second
	log.SetOutput(logger.Bypass())

	var server = NewServer(*addr, logger)
	if err := server.Serve(); err != nil {
		log.Fatalln(err)
	}

	var stop = make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	server.Close()
	logger.Stop()
}
