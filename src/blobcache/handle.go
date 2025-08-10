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
	copy(h.OID[:], data[:16])
	copy(h.Secret[:], data[16:])
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

// ActionSet is a bitmask of the actions that can be performed using a handle.
type ActionSet uint64

const (
	Action_Inspect = (1 << iota)

	Action_Tx_Load
	Action_Tx_Post
	Action_Tx_Get
	Action_Tx_Exists
	Action_Tx_Delete

	Action_Volume_BeginTx
	Action_Volume_Await

	Action_Namespace_Open
	Action_Namespace_List
	Action_Namespace_Put
	Action_Namespace_Delete
)

const Action_ALL = ^ActionSet(0)

func (r ActionSet) String() string {
	parts := []string{}
	rs := map[ActionSet]string{
		Action_Inspect: "INSPECT",

		Action_Tx_Load:   "LOAD",
		Action_Tx_Post:   "POST",
		Action_Tx_Get:    "GET",
		Action_Tx_Exists: "EXISTS",
		Action_Tx_Delete: "DELETE",

		Action_Volume_BeginTx: "BEGIN_TX",
		Action_Volume_Await:   "AWAIT",

		Action_Namespace_Open:   "OPEN",
		Action_Namespace_List:   "LIST",
		Action_Namespace_Put:    "PUT",
		Action_Namespace_Delete: "DELETE",
	}
	for r2, str := range rs {
		if r&r2 != 0 {
			parts = append(parts, str)
		}
	}
	return strings.Join(parts, "|")
}
