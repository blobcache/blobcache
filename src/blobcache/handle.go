package blobcache

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"go.brendoncarroll.net/tai64"
)

type HandleAPI interface {
	// Drop causes a handle to be released immediately.
	// If all the handles to an object are dropped, the object is deleted.
	Drop(ctx context.Context, h Handle) error
	// KeepAlive extends the TTL for some handles.
	KeepAlive(ctx context.Context, hs []Handle) error
	// InspectHandle returns info about a handle.
	InspectHandle(ctx context.Context, h Handle) (*HandleInfo, error)
	// ShareOut creates a copy of the handle according to the mask, and the
	// sharing rules for the handles actions.
	ShareOut(ctx context.Context, h Handle, to NodeID, mask ActionSet) (*Handle, error)
	// ShareIn takes a Handle for an object on another Node and
	// creates a remote/peer object on the local node to access it.
	// it returns the handle to the local object.
	ShareIn(ctx context.Context, host NodeID, h Handle) (Handle, error)
	// Inspect inspects whatever the object is and returns an Info object.
	Inspect(ctx context.Context, h Handle) (Info, error)
}

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
	out = append(out, h.OID[:]...)
	out = append(out, h.Secret[:]...)
	return out
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
	OID    OID       `json:"oid"`
	Rights ActionSet `json:"rights"`

	CreatedAt tai64.TAI64 `json:"created_at"`
	ExpiresAt tai64.TAI64 `json:"expires_at"`
}

func (hi HandleInfo) Marshal(out []byte) []byte {
	ret := out
	ret = append(ret, hi.OID[:]...)
	ret = hi.Rights.Marshal(ret)
	ret = append(ret, hi.CreatedAt.Marshal()...)
	ret = append(ret, hi.ExpiresAt.Marshal()...)
	return ret
}

func (hi *HandleInfo) Unmarshal(data []byte) error {
	if len(data) < 16+2*8 {
		return fmt.Errorf("invalid HandleInfo length: %d", len(data))
	}
	hi.OID = OID(data[:16])
	if err := hi.Rights.Unmarshal(data[16 : 16+8]); err != nil {
		return err
	}
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

func (a ActionSet) Marshal(out []byte) []byte {
	return binary.LittleEndian.AppendUint64(out, uint64(a))
}

func (a *ActionSet) Unmarshal(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("invalid ActionSet length: %d", len(data))
	}
	*a = ActionSet(binary.LittleEndian.Uint64(data))
	return nil
}

func (a ActionSet) Share() ActionSet {
	if a&Action_SHARE_ACK == 0 {
		return 0
	}
	const top32Bits = math.MaxUint32 << 32
	// shareMask is the mask of the actions that are allowed when the handle is shared.
	// The top 32 bits are copied unchanged.
	// The bottom 32 bits are masked using the top 32 bits.
	shareMask := a>>32 | a&top32Bits
	return a & shareMask
}

const (
	// Action_ACK is set on any valid handle.
	// Valid handles can always be inspected, dropped, and kept alive.
	Action_ACK = ActionSet(1 << iota)
	// Action_INSPECT allows the object to be inspected.
	// This is universal across all object types
	Action_INSPECT
)

const (
	// Action_SHARE_ACK is set if the handle is allowed to be shared.
	Action_SHARE_ACK = Action_ACK << 32
)

// SharedAction returns the shared ActionSet for an ActionSet.
func SharedAction(x ActionSet) ActionSet {
	return x << 32
}

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
	switch r {
	case math.MaxUint64:
		return "ALL"
	case 0:
		return "NONE"
	}
	return fmt.Sprintf("%064b", r)
}

func (a ActionSet) Has(b ActionSet) bool {
	return a&b == b
}
