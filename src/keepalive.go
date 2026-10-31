package src

import (
	"bytes"
	"context"
	"log"
	"net"
	"time"
)

// KeepaliveInterval is how often a UDP client repeats its hello to the
// server. It must stay well below the server's UDP peer timeout and below
// typical NAT and proxy UDP session timeouts (from about 30 s).
const KeepaliveInterval = 5 * time.Second

// sendKeepaliveHellos repeats the handshake hello over a UDP server
// connection until ctx is done. The server takes each hello as the peer's
// current address, so the stream follows a NAT or tunnel exit that changed
// the client's port, and the server sees the peer as alive.
func sendKeepaliveHellos(ctx context.Context, conn net.Conn, hello []byte) {
	var ticker = time.NewTicker(KeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := conn.Write(hello); err != nil {
				log.Printf("failed to send keepalive hello to %s: %s", conn.RemoteAddr(), err)
			}
		}
	}
}

// helloEchoFilterConn drops the server's echoes of keepalive hellos, so that
// readers of the UDP server connection get stream packets only.
type helloEchoFilterConn struct {
	net.Conn
	hello []byte
}

func (c *helloEchoFilterConn) Read(p []byte) (int, error) {
	for {
		n, err := c.Conn.Read(p)
		if err != nil || !bytes.Equal(p[:n], c.hello) {
			return n, err
		}
	}
}
