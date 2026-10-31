package src

import (
	"net"
	"testing"

	"golang.org/x/net/ipv4"
)

func TestMarkRealTimeSetsDSCP(t *testing.T) {
	for _, network := range []string{"udp4", "tcp4"} {
		var conn net.Conn
		var err error
		if network == "udp4" {
			conn, err = net.Dial(network, "127.0.0.1:9")
		} else {
			l, lerr := net.Listen(network, "127.0.0.1:0")
			if lerr != nil {
				t.Fatal(lerr)
			}
			defer l.Close()
			conn, err = net.Dial(network, l.Addr().String())
		}
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		markRealTime(conn)
		tos, err := ipv4.NewConn(conn).TOS()
		if err != nil {
			t.Fatal(err)
		}
		if tos>>2 != dscpExpeditedForwarding {
			t.Fatalf("%s: dscp %d, want %d", network, tos>>2, dscpExpeditedForwarding)
		}
	}
}
