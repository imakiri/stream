// Package frame keeps packet boundaries over a stream connection (TCP):
// every Write goes out as one frame, a 2-byte big-endian payload length
// followed by the payload, and every Read returns exactly one frame.
// Over UDP the datagram itself is the boundary and this package is not used.
package frame

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// HeaderSize is the size of the length prefix in front of every frame.
const HeaderSize = 2

// MaxPayloadSize is the largest payload one frame can carry.
const MaxPayloadSize = 1<<16 - 1

// Conn wraps a stream connection so that its Read and Write work with whole
// packets, like a connected UDP socket. Write is safe for concurrent use.
type Conn struct {
	net.Conn

	readMu  sync.Mutex
	header  [HeaderSize]byte
	writeMu sync.Mutex
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{Conn: conn}
}

// Read reads one frame into p and returns its payload size. If the payload
// does not fit into p, the frame is consumed and io.ErrShortBuffer returned.
func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	if _, err := io.ReadFull(c.Conn, c.header[:]); err != nil {
		return 0, err
	}
	var size = int(binary.BigEndian.Uint16(c.header[:]))
	if size > len(p) {
		if _, err := io.CopyN(io.Discard, c.Conn, int64(size)); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("frame of %d bytes does not fit into %d: %w", size, len(p), io.ErrShortBuffer)
	}
	if _, err := io.ReadFull(c.Conn, p[:size]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return size, nil
}

// Write sends p as one frame and returns len(p) on success.
func (c *Conn) Write(p []byte) (int, error) {
	if len(p) > MaxPayloadSize {
		return 0, fmt.Errorf("payload of %d bytes exceeds frame limit %d", len(p), MaxPayloadSize)
	}
	var buf = make([]byte, HeaderSize+len(p))
	binary.BigEndian.PutUint16(buf, uint16(len(p)))
	copy(buf[HeaderSize:], p)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.Conn.Write(buf); err != nil {
		return 0, err
	}
	return len(p), nil
}
