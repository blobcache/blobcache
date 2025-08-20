package bcnet

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"blobcache.io/blobcache/src/blobcache"
)

type MessageType uint8

func (mt MessageType) IsError() bool {
	return mt > MT_OK
}

func (mt MessageType) IsOK() bool {
	return mt == MT_OK
}

const (
	MT_UNKNOWN MessageType = iota
	// MT_PING is a request to ping the remote peer.
	MT_PING
)

// Handle messages
const (
	MT_HANDLE_INSPECT MessageType = 16 + iota
	MT_HANDLE_DROP
	MT_HANDLE_KEEP_ALIVE
)

// Volume messages
const (
	MT_OPEN_AS MessageType = 32 + iota
	MT_OPEN_FROM
	MT_CREATE_VOLUME
	MT_VOLUME_INSPECT
	MT_VOLUME_AWAIT
	MT_VOLUME_BEGIN_TX
)

// Tx messages
const (
	MT_TX_INSPECT MessageType = 48 + iota

	MT_TX_COMMIT
	MT_TX_ABORT

	MT_TX_LOAD
	MT_TX_SAVE

	MT_TX_POST
	MT_TX_POST_SALT
	MT_TX_GET
	MT_TX_EXISTS
	MT_TX_DELETE
	MT_TX_ALLOW_LINK
)

const (
	// MT_LAYER2_TELL is used for volume implementations to communicate with other volumes.
	MT_LAYER2_TELL MessageType = 64 + iota
	// MT_LAYER2_ASK is used for volume implementations to communicate with other volumes.
	MT_LAYER2_ASK
)

// Response messages
const (
	MT_OK = 128 + iota

	MT_ERROR_TIMEOUT
	MT_ERROR_INVALID_HANDLE
	MT_ERROR_NOT_FOUND
	MT_ERROR_NO_PERMISSION
	MT_ERROR_NO_LINK

	MT_ERROR_UNKNOWN = 255
)

const HeaderLen = 1 + 4

type MessageHeader [HeaderLen]byte

func (h *MessageHeader) SetCode(code MessageType) {
	h[0] = byte(code)
}

func (h MessageHeader) Code() MessageType {
	return MessageType(h[0])
}

func (h MessageHeader) BodyLen() int {
	return int(binary.BigEndian.Uint32(h[1:]))
}

func (h *MessageHeader) SetBodyLen(bodyLen int) {
	binary.BigEndian.PutUint32(h[1:], uint32(bodyLen))
}

type Message struct {
	buf []byte
}

func (m *Message) Header() MessageHeader {
	m.buf = extendToLen(m.buf, HeaderLen)
	return MessageHeader(m.buf[:HeaderLen])
}

func (m *Message) setHeader(header MessageHeader) {
	m.buf = extendToLen(m.buf, HeaderLen)
	m.buf = append(m.buf[:0], header[:]...)
}

func (m *Message) SetCode(code MessageType) {
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

func (m *Message) SetError(err error) {
	switch err.(type) {
	case *blobcache.ErrInvalidHandle:
		m.SetCode(MT_ERROR_INVALID_HANDLE)
	case *blobcache.ErrNotFound:
		m.SetCode(MT_ERROR_NOT_FOUND)
	case *blobcache.ErrNoLink:
		m.SetCode(MT_ERROR_NO_LINK)
	default:
		m.SetCode(MT_ERROR_UNKNOWN)
	}
	data := jsonMarshal(err)
	m.SetBody(data)
}

func ParseWireError(code MessageType, x []byte) error {
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
