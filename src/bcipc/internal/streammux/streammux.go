// Package streammux multiplexes streams over a single connection.
//
// Each frame on the wire consists of a 4-byte stream header followed by a
// bcp message. The stream header is a little-endian uint32 where bit 31
// is the FIN flag and bits 0-30 are the stream ID.
//
// Clients create streams with Open; servers receive them with Accept.
// Stream IDs are assigned by the opener using a pre-increment counter
// starting at 1 (ID 0 is never used).
package streammux

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"blobcache.io/blobcache/src/internal/bcp"
)

const frameHeaderLen = 4

const finBit = uint32(1 << 31)

// Mux multiplexes multiple streams over a single connection.
type Mux struct {
	conn    io.ReadWriteCloser
	writeMu sync.Mutex

	nextID atomic.Uint32

	mu       sync.Mutex
	streams  map[uint32]chan frame

	incoming chan incoming

	done    chan struct{}
	readErr error
}

type frame struct {
	msg bcp.Message
	fin bool
}

type incoming struct {
	streamID uint32
	msg      bcp.Message
}

// New creates a Mux wrapping conn. A background goroutine is started
// to read and demux incoming frames.
func New(conn io.ReadWriteCloser) *Mux {
	m := &Mux{
		conn:     conn,
		streams:  make(map[uint32]chan frame),
		incoming: make(chan incoming, 16),
		done:     make(chan struct{}),
	}
	go m.readLoop()
	return m
}

func (m *Mux) readLoop() {
	defer close(m.done)
	defer func() {
		m.mu.Lock()
		for _, ch := range m.streams {
			close(ch)
		}
		m.streams = nil
		m.mu.Unlock()
		close(m.incoming)
	}()

	for {
		var fh [frameHeaderLen]byte
		if _, err := io.ReadFull(m.conn, fh[:]); err != nil {
			m.readErr = err
			return
		}

		v := binary.LittleEndian.Uint32(fh[:])
		streamID := v &^ finBit
		fin := v&finBit != 0

		var msg bcp.Message
		if _, err := msg.ReadFrom(m.conn); err != nil {
			m.readErr = err
			return
		}

		m.mu.Lock()
		ch, ok := m.streams[streamID]
		if ok && fin {
			delete(m.streams, streamID)
		}
		m.mu.Unlock()

		if ok {
			ch <- frame{msg: msg, fin: fin}
			if fin {
				close(ch)
			}
		} else {
			m.incoming <- incoming{streamID: streamID, msg: msg}
		}
	}
}

// Open sends msg on a new stream and returns a Stream for reading responses.
func (m *Mux) Open(msg bcp.Message) (*Stream, error) {
	id := m.nextID.Add(1)
	ch := make(chan frame, 4)

	m.mu.Lock()
	if m.streams == nil {
		m.mu.Unlock()
		return nil, errors.New("streammux: closed")
	}
	m.streams[id] = ch
	m.mu.Unlock()

	if err := m.writeFrame(id, false, msg); err != nil {
		m.mu.Lock()
		delete(m.streams, id)
		m.mu.Unlock()
		return nil, err
	}

	return &Stream{ch: ch, mux: m}, nil
}

// Accept blocks until the peer opens a new stream.
// Returns the request message and a ResponseWriter for sending responses.
func (m *Mux) Accept() (bcp.Message, *ResponseWriter, error) {
	inc, ok := <-m.incoming
	if !ok {
		if m.readErr != nil {
			return bcp.Message{}, nil, m.readErr
		}
		return bcp.Message{}, nil, io.EOF
	}
	rw := &ResponseWriter{
		mux:      m,
		streamID: inc.streamID,
	}
	return inc.msg, rw, nil
}

func (m *Mux) writeFrame(streamID uint32, fin bool, msg bcp.Message) error {
	var fh [frameHeaderLen]byte
	v := streamID
	if fin {
		v |= finBit
	}
	binary.LittleEndian.PutUint32(fh[:], v)

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if _, err := m.conn.Write(fh[:]); err != nil {
		return err
	}
	_, err := msg.WriteTo(m.conn)
	return err
}

// Close closes the Mux and the underlying connection.
func (m *Mux) Close() error {
	err := m.conn.Close()
	<-m.done
	return err
}

// Stream reads response messages from a multiplexed stream.
type Stream struct {
	ch  chan frame
	mux *Mux
}

// Recv reads the next response message.
// Returns io.EOF after the final (FIN) message has been received.
func (s *Stream) Recv() (bcp.Message, error) {
	f, ok := <-s.ch
	if !ok {
		if s.mux.readErr != nil {
			return bcp.Message{}, s.mux.readErr
		}
		return bcp.Message{}, io.EOF
	}
	return f.msg, nil
}

// ResponseWriter sends response messages on a stream.
type ResponseWriter struct {
	mux      *Mux
	streamID uint32
}

// Send sends a response message. Set fin to true on the last response.
func (rw *ResponseWriter) Send(msg bcp.Message, fin bool) error {
	return rw.mux.writeFrame(rw.streamID, fin, msg)
}
