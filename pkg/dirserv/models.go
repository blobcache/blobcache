package dirserv

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
)

// OID is an Object ID
type OID uint64

func (id OID) String() string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[:], uint64(id))
	return hex.EncodeToString(buf[:])
}

const (
	NullOID = 0
	RootOID = math.MaxUint64
)

type Handle struct {
	ID     OID      `json:"id"`
	Secret [16]byte `json:"secret"`
}

func ParseHandle(x []byte) (*Handle, error) {
	var h Handle
	if err := h.UnmarshalText(x); err != nil {
		return nil, err
	}
	return &h, nil
}

func (h Handle) String() string {
	data, _ := h.MarshalText()
	return string(data)
}

func (h Handle) MarshalText() (ret []byte, _ error) {
	ret = append(ret, []byte(h.ID.String())...)
	ret = append(ret, '#')
	ret = append(ret, []byte(base64.RawURLEncoding.EncodeToString(h.Secret[:]))...)
	return ret, nil
}

func (h *Handle) UnmarshalText(data []byte) error {
	parts := bytes.SplitN(data, []byte("#"), 2)
	if len(parts) != 2 {
		return errors.New("parsing handle: missing key")
	}
	oid, err := strconv.ParseUint(string(parts[0]), 16, 64)
	if err != nil {
		return err
	}
	h.ID = OID(oid)
	if _, err := base64.RawURLEncoding.Decode(h.Secret[:], parts[1]); err != nil {
		return err
	}
	return nil
}

type Path []string

type Entry struct {
	Name string
	ID   OID
}
