// Package proxy relays wire packets between the stream server and its
// clients. It sees only the clear routing fields of the packets: the
// handshake and the stream are end-to-end between server and clients.
package proxy

import (
	"errors"
	"fmt"
	"log"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/gosuri/uilive"
	"github.com/imakiri/stream/src/frame"
	"github.com/imakiri/stream/src/wire"
)

// packetBufferSize fits any UDP datagram and any frame.
const packetBufferSize = 1<<16 - 1

const tcpWriteTimeout = time.Second

// PeerTimeout drops a peer that sent nothing, not even a keepalive proxy
// hello (src.KeepaliveInterval), for this long, over UDP and TCP. A TCP peer
// is also dropped when its connection closes.
const PeerTimeout = 30 * time.Second

// unknownPeerLogInterval limits the log of packets from unknown UDP peers: a
// server whose registration the proxy lost, e.g. after a proxy restart, sends
// a thousand data packets a second until its next hello.
const unknownPeerLogInterval = 5 * time.Second

// Peer is one connected server or client. A peer is reached either over UDP,
// by the address its last proxy hello came from, or over TCP, through its
// framed connection.
type Peer struct {
	role    wire.Role
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

func (p *Peer) String() string {
	return fmt.Sprintf("%s %s at %s/%s", p.role, p.id, p.network(), p.addr)
}

func (p *Peer) close() {
	if p.tcpConn != nil {
		_ = p.tcpConn.Close()
	}
}

// Proxy routes packets between one server and its clients over UDP and TCP
// at the same address, whichever transport each peer connected with:
//
//	client hello  client -> the server
//	server hello  server -> the client it names
//	data          server -> every client
//
// Anything else, and data from clients, is dropped.
type Proxy struct {
	mu     sync.RWMutex
	peers  []*Peer
	closed bool // set by Close under mu, so no peer is added after Close took its snapshot

	done        chan struct{}
	wg          sync.WaitGroup
	udpConn     *net.UDPConn
	tcpListener net.Listener

	peerTimeout time.Duration

	addr                string
	speedOutput         *uilive.Writer
	totalReceivedBefore uint64
	totalSentBefore     uint64
	totalReceived       atomic.Uint64
	totalSent           atomic.Uint64
}

func New(addr string, speedOutput *uilive.Writer) *Proxy {
	return &Proxy{addr: addr, speedOutput: speedOutput, peerTimeout: PeerTimeout}
}

// addPeer registers the peer, replacing a previous connection with the same
// ID; a new server also replaces any other server. It returns false, and
// closes the peer, if the proxy is closing.
func (p *Proxy) addPeer(peer *Peer) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		peer.close()
		return false
	}
	var replaced bool
	p.peers = slices.DeleteFunc(p.peers, func(old *Peer) bool {
		if old.id == peer.id || (peer.role == wire.RoleServer && old.role == wire.RoleServer) {
			old.close()
			log.Printf("replaced %s by %s", old, peer)
			replaced = true
			return true
		}
		return false
	})
	p.peers = append(p.peers, peer)
	if !replaced {
		log.Printf("added %s", peer)
	}
	return true
}

// removePeer removes this very peer; it does nothing if the peer has already
// been replaced by a newer connection.
func (p *Proxy) removePeer(peer *Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var i = slices.Index(p.peers, peer)
	if i < 0 {
		return
	}
	peer.close()
	p.peers = slices.Delete(p.peers, i, i+1)
	log.Printf("removed %s", peer)
}

func (p *Proxy) appendPeers(dst []*Peer) []*Peer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append(dst, p.peers...)
}

func (p *Proxy) findPeer(match func(*Peer) bool) *Peer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, peer := range p.peers {
		if match(peer) {
			return peer
		}
	}
	return nil
}

func (p *Proxy) findUDPPeer(addr *net.UDPAddr) *Peer {
	var addrPort = addr.AddrPort()
	return p.findPeer(func(peer *Peer) bool { return peer.udpAddr != nil && peer.udpAddr.AddrPort() == addrPort })
}

func (p *Proxy) sendToPeer(peer *Peer, packet []byte) error {
	if peer.tcpConn != nil {
		_ = peer.tcpConn.SetWriteDeadline(time.Now().Add(tcpWriteTimeout))
		_, err := peer.tcpConn.Write(packet)
		return err
	}
	n, err := p.udpConn.WriteToUDP(packet, peer.udpAddr)
	if err == nil && n != len(packet) {
		err = fmt.Errorf("written %d of %d bytes", n, len(packet))
	}
	return err
}

func (p *Proxy) send(peer *Peer, packet []byte) {
	if err := p.sendToPeer(peer, packet); err != nil {
		log.Printf("failed to send to %s: %s", peer, err)
		if peer.tcpConn != nil {
			p.removePeer(peer)
		}
		return
	}
	p.totalSent.Add(uint64(len(packet)))
}

// route forwards a packet of from by its kind. A TCP peer that fails to take
// a packet is dropped; UDP has no connection to drop.
func (p *Proxy) route(from *Peer, packet []byte, peers []*Peer) []*Peer {
	p.totalReceived.Add(uint64(len(packet)))
	switch wire.PacketKind(packet) {
	case wire.KindProxyHello:
		// A keepalive over TCP: echo it, the peer checks the link by the
		// echoes. UDP hellos never get here, serveUDP answers them.
		p.send(from, packet)
	case wire.KindClientHello:
		if from.role != wire.RoleClient {
			break
		}
		if server := p.findPeer(func(peer *Peer) bool { return peer.role == wire.RoleServer }); server != nil {
			p.send(server, packet)
		}
	case wire.KindServerHello:
		if from.role != wire.RoleServer {
			break
		}
		clientID, ok := wire.ServerHelloClientID(packet)
		if !ok {
			break
		}
		if client := p.findPeer(func(peer *Peer) bool { return peer.role == wire.RoleClient && peer.id == clientID }); client != nil {
			p.send(client, packet)
		}
	case wire.KindData:
		if from.role != wire.RoleServer {
			break
		}
		peers = p.appendPeers(peers[:0])
		for _, peer := range peers {
			if peer.role == wire.RoleClient {
				p.send(peer, packet)
			}
		}
	}
	return peers
}

func (p *Proxy) serveUDP() {
	var buf = make([]byte, packetBufferSize)
	var peers []*Peer
	var unknownPeerLoggedAt time.Time
	var unknownPeerPackets int
	for {
		n, addr, err := p.udpConn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("failed to read udp: %s", err)
			continue
		}

		if wire.PacketKind(buf[:n]) == wire.KindProxyHello {
			role, peerID, err := wire.ParseProxyHello(buf[:n])
			if err != nil {
				log.Printf("bad proxy hello from udp/%s: %s", addr, err)
				continue
			}
			// The hello is both the registration and the keepalive: the same
			// peer from the same address only refreshes lastSeen.
			if peer := p.findUDPPeer(addr); peer != nil && peer.id == peerID && peer.role == role {
				peer.seen(time.Now())
			} else {
				var peer = &Peer{role: role, id: peerID, addr: addr, udpAddr: addr}
				peer.seen(time.Now())
				if !p.addPeer(peer) {
					return
				}
			}
			if _, err := p.udpConn.WriteToUDP(buf[:n], addr); err != nil {
				log.Printf("failed to answer proxy hello from udp/%s: %s", addr, err)
			}
			continue
		}

		var from = p.findUDPPeer(addr)
		if from == nil {
			unknownPeerPackets++
			if now := time.Now(); now.Sub(unknownPeerLoggedAt) > unknownPeerLogInterval {
				log.Printf("dropped %d udp packets from unknown peers, the last from %s", unknownPeerPackets, addr)
				unknownPeerLoggedAt, unknownPeerPackets = now, 0
			}
			continue
		}
		from.seen(time.Now())
		peers = p.route(from, buf[:n], peers)
	}
}

func (p *Proxy) serveTCP() {
	for {
		conn, err := p.tcpListener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("failed to accept tcp connection: %s", err)
			continue
		}
		p.wg.Go(func() { p.serveTCPConn(conn) })
	}
}

// serveTCPConn takes the first frame of a TCP peer as its proxy hello.
func (p *Proxy) serveTCPConn(conn net.Conn) {
	var addr = conn.RemoteAddr()
	var framed = frame.NewConn(conn)
	var buf = make([]byte, packetBufferSize)

	_ = conn.SetDeadline(time.Now().Add(time.Second))
	n, err := framed.Read(buf)
	if err != nil {
		_ = conn.Close()
		log.Printf("failed proxy hello from tcp/%s: %s", addr, err)
		return
	}
	role, peerID, err := wire.ParseProxyHello(buf[:n])
	if err != nil {
		_ = conn.Close()
		log.Printf("failed proxy hello from tcp/%s: %s", addr, err)
		return
	}
	if _, err := framed.Write(buf[:n]); err != nil {
		_ = conn.Close()
		log.Printf("failed to answer proxy hello from tcp/%s: %s", addr, err)
		return
	}
	_ = conn.SetDeadline(time.Time{})

	var peer = &Peer{role: role, id: peerID, addr: addr, tcpConn: framed}
	if !p.addPeer(peer) {
		return
	}

	var peers []*Peer
	for {
		_ = conn.SetReadDeadline(time.Now().Add(p.peerTimeout))
		n, err := framed.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("failed to read from %s: %s", peer, err)
			}
			p.removePeer(peer)
			return
		}
		peers = p.route(peer, buf[:n], peers)
	}
}

// expireUDPPeers drops UDP peers that went silent for peerTimeout; a silent
// TCP peer runs into the read deadline in serveTCPConn.
func (p *Proxy) expireUDPPeers() {
	var ticker = time.NewTicker(time.Second)
	defer ticker.Stop()
	var peers []*Peer
	for {
		select {
		case <-p.done:
			return
		case now := <-ticker.C:
			peers = p.appendPeers(peers[:0])
			for _, peer := range peers {
				if peer.udpAddr == nil {
					continue
				}
				var silent = now.Sub(time.Unix(0, peer.lastSeenUnixNano.Load()))
				if silent > p.peerTimeout {
					log.Printf("%s silent for %s", peer, silent.Round(time.Second))
					p.removePeer(peer)
				}
			}
		}
	}
}

func (p *Proxy) reportSpeed() {
	var ticker = time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			var totalReceived = p.totalReceived.Load()
			var totalSent = p.totalSent.Load()

			if totalReceived > p.totalReceivedBefore {
				_, _ = fmt.Fprintf(p.speedOutput.Newline(), "incoming speed: %6d KBit/sec, outgoing speed: %6d KBit/sec, transmited: %6d MB\n",
					8*(totalReceived-p.totalReceivedBefore)/(1<<10), 8*(totalSent-p.totalSentBefore)/(1<<10), totalReceived/(1<<20))
			}

			p.totalReceivedBefore = totalReceived
			p.totalSentBefore = totalSent
		}
	}
}

// Serve starts listening on UDP and TCP at the proxy address.
func (p *Proxy) Serve() error {
	if p.done != nil {
		return errors.New("proxy can be started only once")
	}

	udpAddr, err := net.ResolveUDPAddr("udp4", p.addr)
	if err != nil {
		return fmt.Errorf("failed to resolve udp address %s: %w", p.addr, err)
	}
	udpConn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen udp at %s: %w", p.addr, err)
	}
	tcpListener, err := net.Listen("tcp4", p.addr)
	if err != nil {
		_ = udpConn.Close()
		return fmt.Errorf("failed to listen tcp at %s: %w", p.addr, err)
	}
	log.Printf("listening at udp/%s and tcp/%s", udpConn.LocalAddr(), tcpListener.Addr())

	p.done = make(chan struct{})
	p.udpConn = udpConn
	p.tcpListener = tcpListener

	p.wg.Go(p.reportSpeed)
	p.wg.Go(p.expireUDPPeers)
	p.wg.Go(p.serveUDP)
	p.wg.Go(p.serveTCP)
	return nil
}

func (p *Proxy) Close() {
	close(p.done)
	_ = p.udpConn.Close()
	_ = p.tcpListener.Close()

	p.mu.Lock()
	p.closed = true
	var peers = slices.Clone(p.peers)
	p.mu.Unlock()
	for _, peer := range peers {
		p.removePeer(peer)
	}
	p.wg.Wait()
}
