package src

import (
	"context"
	"log"
	"net"
	"time"
)

// KeepaliveInterval is how often a UDP peer repeats its proxy hello. It must
// stay well below the proxy's UDP peer timeout (proxy.UDPPeerTimeout) and
// below typical NAT and socks/tunnel UDP session timeouts (from about 30 s).
const KeepaliveInterval = 5 * time.Second

// sendKeepaliveHellos repeats the proxy hello over a UDP proxy connection
// until ctx is done. The proxy takes each hello as the peer's current
// address, so the stream follows a NAT or tunnel exit that changed the peer's
// port, and the proxy sees the peer as alive.
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
