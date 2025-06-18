package bcnet

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"blobcache.io/blobcache/src/blobcache"
)

type MessageType uint8

const (
	MT_UNKNOWN MessageType = iota

	MT_PING
	MT_PONG

	MT_OPEN
	MT_DROP
	MT_KEEP_ALIVE

	MT_VOLUME_CREATE
	MT_VOLUME_INSPECT
	MT_VOLUME_AWAIT

	MT_BEGIN_TX
	MT_TX_COMMIT
	MT_TX_ABORT
	MT_TX_LOAD
	MT_TX_POST
	MT_TX_GET
	MT_TX_EXISTS
	MT_TX_DELETE

	MT_ERROR = 255
)

const HeaderLen = 1 + 4

type MessageHeader [HeaderLen]byte

func (h MessageHeader) SetCode(code MessageType) {
	h[0] = byte(code)
}

func (h MessageHeader) Code() MessageType {
	return MessageType(h[0])
}

func (h MessageHeader) BodyLen() int {
	return int(binary.BigEndian.Uint32(h[1:]))
}

func (h MessageHeader) SetBodyLen(bodyLen int) {
	binary.BigEndian.PutUint32(h[1:], uint32(bodyLen))
}

type Message struct {
	buf []byte
}

func (m *Message) Header() MessageHeader {
	return MessageHeader(m.buf[:HeaderLen])
}

func (m *Message) setHeader(header MessageHeader) {
	m.buf = append(m.buf[:0], header[:]...)
}

func (m *Message) SetCode(code MessageType) {
	h := m.Header()
	h.SetCode(code)
	m.setHeader(h)
}

func (m *Message) SetBody(body []byte) {
	h := m.Header()
	h.SetBodyLen(len(body))
	m.setHeader(h)
	m.buf = append(m.buf[:0], body...)
}

func (m *Message) Body() []byte {
	return m.buf[HeaderLen:]
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	_, err := w.Write(m.buf)
	return int64(len(m.buf)), err
}

func (m *Message) ReadFrom(r io.Reader) (int64, error) {
	var header MessageHeader
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, err
	}
	bodyLen := header.BodyLen()
	m.buf = append(m.buf[:0], make([]byte, bodyLen)...)
	if _, err := io.ReadFull(r, m.buf[len(m.buf)-bodyLen:]); err != nil {
		return 0, err
	}
	return int64(len(m.buf)), nil
}

func (m *Message) SetError(err error) {
	m.SetCode(MT_ERROR)
	m.SetBody(MarshalWireError(err))
}

type WireError struct {
	InvalidHandle *blobcache.ErrInvalidHandle `json:"invalid_handle,omitempty"`
	NotFound      *blobcache.ErrNotFound      `json:"not_found,omitempty"`
	Unknown       *string                     `json:"unknown,omitempty"`
}

func ParseWireError(x []byte) error {
	var werr WireError
	if err := json.Unmarshal(x, &werr); err != nil {
		return err
	}
	switch {
	case werr.InvalidHandle != nil:
		return werr.InvalidHandle
	case werr.NotFound != nil:
		return werr.NotFound
	case werr.Unknown != nil:
		return errors.New(*werr.Unknown)
	default:
		return errors.New("bcnet: error parsing an error")
	}
}

func MarshalWireError(err error) []byte {
	switch x := err.(type) {
	case *blobcache.ErrInvalidHandle:
		return jsonMarshal(x)
	default:
		return []byte(err.Error())
	}
}

func jsonMarshal(x any) []byte {
	buf, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return buf
}
