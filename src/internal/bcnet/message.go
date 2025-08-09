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

	// MT_OK is the success response to a request.
	MT_OK

	// MT_PING is a request to ping the remote peer.
	MT_PING

	MT_HANDLE_INSPECT
	MT_HANDLE_DROP
	MT_HANDLE_KEEP_ALIVE
	MT_OPEN

	MT_VOLUME_INSPECT
	MT_VOLUME_AWAIT
	MT_VOLUME_BEGIN_TX

	MT_TX_COMMIT
	MT_TX_ABORT
	MT_TX_LOAD
	MT_TX_POST
	MT_TX_POST_SALT
	MT_TX_GET
	MT_TX_EXISTS
	MT_TX_DELETE
	MT_TX_ALLOW_LINK
	MT_TX_CREATE_SUBVOLUME

	// MT_LAYER2_TELL is used for volume implementations to communicate with other volumes.
	MT_LAYER2_TELL
	// MT_LAYER2_ASK is used for volume implementations to communicate with other volumes.
	MT_LAYER2_ASK

	MT_ERROR = 255
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
	var werr WireError
	switch x := err.(type) {
	case *blobcache.ErrInvalidHandle:
		werr.InvalidHandle = x
	case *blobcache.ErrNotFound:
		werr.NotFound = x
	default:
		estr := x.Error()
		werr.Unknown = &estr
	}
	return jsonMarshal(werr)
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
