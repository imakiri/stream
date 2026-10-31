package proxy

import (
	"crypto/rand"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/imakiri/stream/src/wire"
)

// TestProxyDropsHellosFromTheWrongRole checks that a client cannot send
// server hellos to other clients, nor client hellos to them.
func TestProxyDropsHellosFromTheWrongRole(t *testing.T) {
	var p, addr = startProxy(t)
	var victimID = uuid.Must(uuid.NewV4())
	var server = connectUDP(t, addr, wire.RoleServer, uuid.Must(uuid.NewV4()))
	var attacker = connectUDP(t, addr, wire.RoleClient, uuid.Must(uuid.NewV4()))
	var victim = connectTCP(t, addr, wire.RoleClient, victimID)
	waitPeers(t, p, 3)

	if _, err := attacker.Write(serverHelloFor(victimID)); err != nil {
		t.Fatal(err)
	}
	expectNothing(t, victim, "a client's server hello at another client")

	if _, err := server.Write(packetOfKind(wire.KindClientHello, wire.ClientHelloSize, 3)); err != nil {
		t.Fatal(err)
	}
	expectNothing(t, victim, "a server's client hello at a client")
}

// TestProxySurvivesGarbage throws random datagrams and frames of every size
// at the proxy, over UDP and TCP, and checks it still routes the stream.
func TestProxySurvivesGarbage(t *testing.T) {
	var p, addr = startProxy(t)
	var server = connectUDP(t, addr, wire.RoleServer, uuid.Must(uuid.NewV4()))
	var udpClient = connectUDP(t, addr, wire.RoleClient, uuid.Must(uuid.NewV4()))
	var tcpClient = connectTCP(t, addr, wire.RoleClient, uuid.Must(uuid.NewV4()))
	var udpJunk = connectUDP(t, addr, wire.RoleClient, uuid.Must(uuid.NewV4()))
	var tcpJunk = connectTCP(t, addr, wire.RoleClient, uuid.Must(uuid.NewV4()))
	waitPeers(t, p, 5)

	var junk = make([]byte, 2000)
	for size := range len(junk) {
		_, _ = rand.Read(junk[:size])
		if size >= 2 && size%3 == 0 {
			junk[0] = wire.Version // let some get past the version check
			junk[1] = byte(size % 5)
		}
		_, _ = udpJunk.Write(junk[:size])
		_, _ = tcpJunk.Write(junk[:size])
	}

	var data = packetOfKind(wire.KindData, 1086, 0x42)
	if _, err := server.Write(data); err != nil {
		t.Fatal(err)
	}
	expectPacket(t, udpClient, data, "data at udp client after garbage")
	expectPacket(t, tcpClient, data, "data at tcp client after garbage")
}
