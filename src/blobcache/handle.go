package blobcache

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"go.brendoncarroll.net/tai64"
)

// HandleSize is the number of bytes in a handle.
const HandleSize = OIDSize + 16

type Handle struct {
	OID    OID
	Secret [16]byte
}

// ParseHandle parses a handle from a string.
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

func (h Handle) Marshal(out []byte) []byte {
	buf := make([]byte, HandleSize)
	copy(buf[:OIDSize], h.OID[:])
	copy(buf[OIDSize:], h.Secret[:])
	return buf
}

// Unmarshal unmarshals a handle from it's binary representation.
func (h *Handle) Unmarshal(data []byte) error {
	if len(data) != HandleSize {
		return fmt.Errorf("invalid handle length: %d", len(data))
	}
	copy(h.OID[:], data[:OIDSize])
	copy(h.Secret[:], data[OIDSize:])
	return nil
}

// HandleInfo is information about a handle, *NOT* the object it points to.
type HandleInfo struct {
	OID OID `json:"oid"`

	CreatedAt tai64.TAI64 `json:"created_at"`
	ExpiresAt tai64.TAI64 `json:"expires_at"`
}

func (hi HandleInfo) Marshal(out []byte) []byte {
	ret := out
	ret = append(ret, hi.OID[:]...)
	ret = append(ret, hi.CreatedAt.Marshal()...)
	ret = append(ret, hi.ExpiresAt.Marshal()...)
	return ret
}

func (hi *HandleInfo) Unmarshal(data []byte) error {
	if len(data) < 16+2*8 {
		return fmt.Errorf("invalid HandleInfo length: %d", len(data))
	}
	hi.OID = OID(data[:16])
	createdAt, err := tai64.Parse(data[16 : 16+tai64.TAI64Size])
	if err != nil {
		return err
	}
	hi.CreatedAt = createdAt
	expiresAt, err := tai64.Parse(data[16+tai64.TAI64Size : 16+2*tai64.TAI64Size])
	if err != nil {
		return err
	}
	hi.ExpiresAt = expiresAt
	return nil
}

// ActionSet is a bitmask of the actions that can be performed using a handle.
type ActionSet uint64

const (
	// Action_ACK is set on any valid handle.
	Action_ACK = (1 << iota)

	Action_Tx_Inspect

	Action_Tx_Load
	Action_Tx_Post
	Action_Tx_Get
	Action_Tx_Exists
	Action_Tx_Delete
	Action_Tx_AddFrom
	Action_Tx_AllowLink
	Action_Tx_Visited
	Action_Tx_IsVisited

	Action_Volume_BeginTx
	Action_Volume_Await
	Action_Volume_Inspect
)

const Action_ALL = ^ActionSet(0)

func (r *ActionSet) Scan(x any) error {
	switch x := x.(type) {
	case []byte:
		if len(x) != 8 {
			return fmt.Errorf("invalid ActionSet bytes length: %d", len(x))
		}
		*r = ActionSet(binary.BigEndian.Uint64(x))
	case uint64:
		*r = ActionSet(x)
	default:
		return fmt.Errorf("cannot scan %T into ActionSet", x)
	}
	return nil
}

func (r ActionSet) String() string {
	parts := []string{}
	rs := map[ActionSet]string{
		Action_Tx_Inspect: "INSPECT",

		Action_Tx_Load:      "LOAD",
		Action_Tx_Post:      "POST",
		Action_Tx_Get:       "GET",
		Action_Tx_Exists:    "EXISTS",
		Action_Tx_Delete:    "DELETE",
		Action_Tx_AddFrom:   "ADD_FROM",
		Action_Tx_AllowLink: "ALLOW_LINK",
		Action_Tx_Visited:   "VISITED",
		Action_Tx_IsVisited: "IS_VISITED",

		Action_Volume_BeginTx: "BEGIN_TX",
		Action_Volume_Await:   "AWAIT",
	}
	for r2, str := range rs {
		if r&r2 != 0 {
			parts = append(parts, str)
		}
	}
	return strings.Join(parts, "|")
}
