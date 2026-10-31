// Package wire is the v3 protocol between the stream server (the source),
// its clients (the viewers) and the relay that forwards packets between them.
//
// Every packet starts with a version byte and a kind byte:
//
//	KindRelayHello  peer -> relay, echoed back: registers the peer's role and
//	                ID at the relay; repeated over UDP as keepalive
//	KindClientHello client -> relay -> server: client certificate and
//	                ephemeral key, see handshake.go
//	KindServerHello server -> relay -> one client: server certificate and the
//	                stream key wrapped for that client
//	KindData        server -> relay -> all clients: one FEC packet of the
//	                stream, sealed with the stream key, see stream.go
//	KindClientReport client -> relay -> server: the delay and losses the
//	                client sees, sealed with its own report key, see report.go
//
// The relay reads only the version, the kind and the clear routing fields;
// everything it relays between server and clients is end-to-end protected.
//
// The certificates, the handshake and the stream cipher sit behind the
// interfaces CertificateVerifier, ServerHandshaker, ClientHandshaker,
// StreamSealer and StreamOpener, so each can be replaced on its own.
package wire

import (
	"errors"
	"fmt"

	"github.com/gofrs/uuid/v5"
)

// Version is the first byte of every packet.
const Version = 3

// Kind is the second byte of every packet.
type Kind uint8

const (
	KindRelayHello   Kind = 1
	KindClientHello  Kind = 2
	KindServerHello  Kind = 3
	KindData         Kind = 4
	KindClientReport Kind = 5
)

// PacketKind returns the kind of a packet, or 0 if it is not a packet of
// this protocol version.
func PacketKind(packet []byte) Kind {
	if len(packet) < 2 || packet[0] != Version {
		return 0
	}
	return Kind(packet[1])
}

// Role is what a peer is to the relay.
type Role uint8

const (
	RoleServer Role = 1 // the stream source; its data goes to every client
	RoleClient Role = 2 // a viewer
)

func (r Role) String() string {
	switch r {
	case RoleServer:
		return "server"
	case RoleClient:
		return "client"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// RelayHelloSize is the size of a relay hello:
//
//	0      version
//	1      kind = KindRelayHello
//	2      role
//	3..19  peer ID (the ID of the peer's certificate)
const RelayHelloSize = 19

// MarshalRelayHello returns the hello a peer sends to the relay.
func MarshalRelayHello(role Role, peerID uuid.UUID) []byte {
	var b = make([]byte, RelayHelloSize)
	b[0] = Version
	b[1] = byte(KindRelayHello)
	b[2] = byte(role)
	copy(b[3:19], peerID.Bytes())
	return b
}

var ErrInvalidRelayHello = errors.New("invalid relay hello")

// ParseRelayHello returns the role and the peer ID of a relay hello.
func ParseRelayHello(packet []byte) (Role, uuid.UUID, error) {
	if len(packet) != RelayHelloSize || PacketKind(packet) != KindRelayHello {
		return 0, uuid.Nil, ErrInvalidRelayHello
	}
	var role = Role(packet[2])
	if role != RoleServer && role != RoleClient {
		return 0, uuid.Nil, ErrInvalidRelayHello
	}
	var peerID = uuid.FromBytesOrNil(packet[3:19])
	if peerID == uuid.Nil {
		return 0, uuid.Nil, ErrInvalidRelayHello
	}
	return role, peerID, nil
}
