package blobcache

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"go.brendoncarroll.net/tai64"
)

type Handle struct {
	OID    OID
	Secret [16]byte
}

func ParseHandle(s string) (Handle, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return Handle{}, fmt.Errorf("invalid handle: %s", s)
	}
	var ret Handle
	oid, err := ParseOID(parts[0])
	if err != nil {
		return Handle{}, err
	}
	ret.OID = oid
	if _, err := hex.Decode(ret.Secret[:], []byte(parts[1])); err != nil {
		return Handle{}, err
	}
	return ret, nil
}

func (h Handle) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.String())
}

func (h *Handle) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	newH, err := ParseHandle(s)
	if err != nil {
		return err
	}
	*h = newH
	return nil
}

func (h Handle) String() string {
	return h.OID.String() + "." + hex.EncodeToString(h.Secret[:])
}

func (h *Handle) UnmarshalBinary(data []byte) error {
	if len(data) != 32 {
		return fmt.Errorf("invalid handle length: %d", len(data))
	}
	copy(h.OID[:], data[:32])
	copy(h.Secret[:], data[32:])
	return nil
}

func (h Handle) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 32)
	copy(buf[:32], h.OID[:])
	copy(buf[32:], h.Secret[:])
	return buf, nil
}

// HandleInfo is information about a handle, *NOT* the object it points to.
type HandleInfo struct {
	OID OID `json:"oid"`

	CreatedAt tai64.TAI64N `json:"created_at"`
	ExpiresAt tai64.TAI64N `json:"expires_at"`
}

// Rights is a bitmask of the actions that can be performed using a handle.
type Rights uint64

const (
	Rights_Inspect = (1 << iota)

	Rights_Tx_Load
	Rights_Tx_Post
	Rights_Tx_Get
	Rights_Tx_Exists
	Rights_Tx_Delete

	Rights_Volume_BeginTx
	Rights_Volume_Await

	Rights_Namespace_Open
	Rights_Namespace_List
	Rights_Namespace_Put
	Rights_Namespace_Delete
)

const Rights_ALL = ^Rights(0)

func (r Rights) String() string {
	parts := []string{}
	rs := map[Rights]string{
		Rights_Inspect: "INSPECT",

		Rights_Tx_Load:   "LOAD",
		Rights_Tx_Post:   "POST",
		Rights_Tx_Get:    "GET",
		Rights_Tx_Exists: "EXISTS",
		Rights_Tx_Delete: "DELETE",

		Rights_Volume_BeginTx: "BEGIN_TX",
		Rights_Volume_Await:   "AWAIT",

		Rights_Namespace_Open:   "OPEN",
		Rights_Namespace_List:   "LIST",
		Rights_Namespace_Put:    "PUT",
		Rights_Namespace_Delete: "DELETE",
	}
	for r2, str := range rs {
		if r&r2 != 0 {
			parts = append(parts, str)
		}
	}
	return strings.Join(parts, "|")
}

// Entry is an entry in a namespace.
type Entry struct {
	Name   string `json:"name"`
	Target OID    `json:"target"`
	Rights Rights `json:"rights"`

	Volume *VolumeInfo `json:"volume,omitempty"`
}
