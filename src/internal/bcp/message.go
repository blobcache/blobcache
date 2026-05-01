package bcp

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"blobcache.io/blobcache/src/blobcache"
)

type MessageCode uint16

// ObjectType is the first 8 bits on the wire
func (mt MessageCode) ObjectType() uint8 {
	return uint8(mt >> 8)
}

// OpCode is the second 8 bits on the wire.
func (mt MessageCode) OpCode() uint8 {
	return uint8(mt & 0xff)
}

func (mt *MessageCode) SetObjectType(oc uint8) {
	*mt = (*mt & 0x00ff) | (MessageCode(oc) << 8)
}

func (mt *MessageCode) SetOpCode(oc uint8) {
	*mt = (*mt & 0xff00) | MessageCode(oc)
}

func (mt MessageCode) IsError() bool {
	return mt > MT_OK
}

func (mt MessageCode) IsOK() bool {
	return mt == MT_OK
}

const sectionSize = 256

const (
	MT_UNKNOWN MessageCode = iota
	// MT_PING is a request to ping the remote peer.
	MT_PING
	// MT_ENDPOINT is a request for the remote to respond with its Endpoint
	MT_ENDPOINT
	// MT_INSPECT can be used to inspect any object
	MT_INSPECT
	// MT_OPEN_FIAT calls the OpenFiat method.
	MT_OPEN_FIAT
)

// Handle messages
const (
	MT_HANDLE_INSPECT MessageCode = (1 * sectionSize) + iota
	MT_HANDLE_DROP
	MT_HANDLE_KEEP_ALIVE
	MT_HANDLE_SHARE_OUT
	MT_HANDLE_SHARE_IN
)

// Volume messages
const (
	MT_VOLUME_INSPECT MessageCode = (2 * sectionSize) + iota
	MT_VOLUME_BEGIN_TX
	MT_OPEN_FROM

	MT_CREATE_VOLUME MessageCode = (3 * sectionSize) - 1
)

// Tx messages
const (
	MT_TX_INSPECT MessageCode = (3 * sectionSize) + iota

	MT_TX_ABORT
	MT_TX_COMMIT

	MT_TX_LOAD
	MT_TX_SAVE

	MT_TX_POST
	MT_TX_POST_SALT
	MT_TX_GET
	MT_TX_GET_SALT
	MT_TX_EXISTS
	MT_TX_DELETE
	MT_TX_COPY
	MT_TX_LINK
	MT_TX_UNLINK

	MT_TX_VISIT
	MT_TX_IS_VISITED
	MT_TX_VISIT_LINKS
)

const (
	MT_QUEUE_INSPECT MessageCode = (4 * sectionSize) + iota
	MT_QUEUE_ENQUEUE
	MT_QUEUE_DEQUEUE
	MT_QUEUE_SUB_TO_VOLUME

	MT_QUEUE_CREATE MessageCode = (5 * sectionSize) - 1
)

// Response messages
const (
	MT_OK = MessageCode((255 * sectionSize) + iota)

	MT_ERROR_TIMEOUT
	MT_ERROR_INVALID_HANDLE
	MT_ERROR_NOT_FOUND
	MT_ERROR_NO_PERMISSION
	MT_ERROR_NO_LINK
	MT_ERROR_TOO_LARGE

	MT_ERROR_UNKNOWN = MessageCode(256*sectionSize - 1)
)

const HeaderLen = 8

type MessageHeader [HeaderLen]byte

func (h *MessageHeader) SetCode(code MessageCode) {
	h[0] = code.ObjectType()
	h[1] = code.OpCode()
}

func (h MessageHeader) Code() (ret MessageCode) {
	ret.SetObjectType(h[0])
	ret.SetOpCode(h[1])
	return ret
}

func (h MessageHeader) BodyLen() int {
	return int(binary.LittleEndian.Uint32(h[4:]))
}

func (h *MessageHeader) SetBodyLen(bodyLen int) {
	binary.LittleEndian.PutUint32(h[4:], uint32(bodyLen))
}

type Message struct {
	buf []byte
}

func NewMessage(x []byte) Message {
	return Message{buf: x}
}

func (m *Message) Header() MessageHeader {
	m.buf = extendToLen(m.buf, HeaderLen)
	return MessageHeader(m.buf[:HeaderLen])
}

func (m *Message) setHeader(header MessageHeader) {
	m.buf = extendToLen(m.buf, HeaderLen)
	m.buf = append(m.buf[:0], header[:]...)
}

func (m *Message) SetCode(code MessageCode) {
	m.buf = extendToLen(m.buf, HeaderLen)
	h := m.Header()
	h.SetCode(code)
	m.setHeader(h)
}

func (m *Message) SetBody(body []byte) {
	h := m.Header()
	h.SetBodyLen(len(body))
	m.setHeader(h)
	m.buf = append(m.buf[:HeaderLen], body...)
}

// Sendable is a type that can be sent in a message
type Sendable interface {
	Marshal(out []byte) []byte
}

// SetSendable sets the body of the message to a Sendable
func (m *Message) SetSendable(x Sendable) {
	m.buf = x.Marshal(m.buf[:HeaderLen])
	h := m.Header()
	h.SetBodyLen(len(m.buf) - HeaderLen)
	m.setHeader(h)
}

func (m *Message) Body() []byte {
	return m.buf[HeaderLen:]
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	_, err := w.Write(m.buf)
	return int64(len(m.buf)), err
}

func (m *Message) ReadFrom(r io.Reader) (int64, error) {
	m.buf = m.buf[:0]
	var header MessageHeader
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, err
	}
	m.buf = append(m.buf, header[:]...)
	bodyLen := header.BodyLen()
	m.buf = extendToLen(m.buf, len(m.buf)+bodyLen)
	if _, err := io.ReadFull(r, m.buf[HeaderLen:]); err != nil {
		return 0, err
	}
	return int64(len(m.buf)), nil
}

func (m *Message) ReadDatagramFrom(r io.Reader, alloc int) error {
	if len(m.buf) < alloc {
		m.buf = append(m.buf[:0], make([]byte, alloc)...)
	}
	n, err := r.Read(m.buf)
	if err != nil {
		return err
	}
	m.buf = m.buf[:n]
	return nil
}

func (m *Message) SetError(err error) {
	switch err.(type) {
	case blobcache.ErrInvalidHandle:
		m.SetCode(MT_ERROR_INVALID_HANDLE)
	case blobcache.ErrNotFound:
		m.SetCode(MT_ERROR_NOT_FOUND)
	case blobcache.ErrNoLink:
		m.SetCode(MT_ERROR_NO_LINK)
	case blobcache.ErrTooLarge:
		m.SetCode(MT_ERROR_TOO_LARGE)
	default:
		m.SetCode(MT_ERROR_UNKNOWN)
		m.SetBody([]byte(err.Error()))
		return
	}
	m.SetBody(jsonMarshal(err))
}

func parseWireError(code MessageCode, x []byte) error {
	var ret error
	switch code {
	case MT_ERROR_INVALID_HANDLE:
		var e blobcache.ErrInvalidHandle
		if err := json.Unmarshal(x, &e); err != nil {
			return err
		}
		ret = &e
	case MT_ERROR_NOT_FOUND:
		var e blobcache.ErrNotFound
		if err := json.Unmarshal(x, &e); err != nil {
			return err
		}
		ret = &e
	case MT_ERROR_NO_LINK:
		var e blobcache.ErrNoLink
		if err := json.Unmarshal(x, &e); err != nil {
			return err
		}
		ret = &e
	default:
		return fmt.Errorf("unknown error code: %d. data=%q", code, string(x))
	}
	return ret
}

func jsonMarshal(x any) []byte {
	buf, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return buf
}

func extendToLen(buf []byte, n int) []byte {
	if len(buf) < n {
		buf = append(buf, make([]byte, n-len(buf))...)
	}
	return buf
}
