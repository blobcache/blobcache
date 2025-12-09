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
	// Share creates a copy of the handle according to the mask, and the
	// sharing rules for the handles actions.
	Share(ctx context.Context, h Handle, to PeerID, mask ActionSet) (*Handle, error)
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
	Action_ACK = (1 << iota)

	// Action_TX_INSPECT allows the transaction to be inspected.
	// On a Transaction handle it gates the InspectTx operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_INSPECT
	// Action_TX_LOAD allows Load operations in the transaction.
	// On a Transaction handle it gates the Load operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_LOAD
	// Action_TX_SAVE allows Save operations in the transaction.
	// On a Transaction handle it gates the Save operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_SAVE
	// Action_TX_POST allows Post operations in the transaction.
	// On a Transaction handle it gates the Post operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_POST
	// Action_TX_GET allows Get operations in the transaction.
	// On a Transaction handle it gates the Get operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_GET
	// Action_TX_EXISTS allows Exists operations in the transaction.
	// On a Transaction handle it gates the Exists operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_EXISTS
	// Action_TX_DELETE allows Delete operations in the transaction.
	// On a Transaction handle it gates the Delete operation.
	// On a Volume handle:
	// - constrains the operations that can be performed in transactions created with that handle.
	// - gates opening GC transactions on the volume.
	Action_TX_DELETE
	// Action_TX_COPY_FROM allows Copy operations to pull from this transaction.
	// On a Transaction handle is gates using the handle as a source in a Copy operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_COPY_FROM
	// Action_TX_COPY_TO allows a Transaction to be written to in an Copy operation.
	// On a Transaction handle it gates the Copy operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_COPY_TO
	// Action_TX_LINK_FROM allows a transaction to add a link to another Volume.
	// It gates the AllowLink operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_LINK_FROM
	// Action_TX_UNLINK_FROM allows a transaction to remove a link from another Volume.
	// It gates the Unlink operation.
	// On a Volume handle it constrains the operations that can be performed in transactions created with that handle.
	Action_TX_UNLINK_FROM
)

const (
	// Action_VOLUME_INSPECT allows the inspection of volumes.
	// It has no effect on a Transaction handle.
	Action_VOLUME_INSPECT = 1 << 24
	// Action_VOLUME_BEGIN_TX allows the beginning of transactions on a volume
	// It has no effect on a Transaction handle.
	Action_VOLUME_BEGIN_TX = 1 << 25
	// Action_VOLUME_LINK_TO allows a volume to be linked to in an AllowLink operation.
	// If this is not set, then there is no way for the volume to be persisted.
	// It has no effect on a Transaction handle.
	Action_VOLUME_LINK_TO = 1 << 26
	// Action_VOLUME_SUB_TO allows a Queue to be subscribed
	// to a Volume's changes.
	Action_VOLUME_SUB_TO = 1 << 27

	// Action_VOLUME_CLONE allows the Volume to be cloned.
	// It has no effect on a Transaction handle.
	Action_VOLUME_CLONE = 1 << 30
	// Action_VOLUME_CREATE allows the creation of volumes.
	// This is never used on a Volume or Transaction handle.
	// But it is useful to be able to refer to it with the other actions using the ActionSet type.
	Action_VOLUME_CREATE = 1 << 31
)

const (
	//Action_QUEUE_INSPECT allows the queue to be inspected
	Action_QUEUE_INSPECT = 1 << (iota + 1)
	// Action_QUEUE_NEXT allows items to be read form the Queue
	Action_QUEUE_NEXT
	// Action_QUEUE_INSERT allows items to be inserted into the Queue
	Action_QUEUE_INSERT
	// Action_QUEUE_SUB_VOLUME allows the Queue to be subscribe to volumes
	Action_QUEUE_SUB_VOLUME

	// Action_QUEUE_CREATE allows creation of queues.
	// This is never used on a handle
	// But it is useful to be able to refer to it with the other actions using the ActionSet type.
	Action_QUEUE_CREATE = 1 << 31
)

const (
	// Action_SHARE_ACK is set if the handle is allowed to be shared.
	Action_SHARE_ACK = Action_ACK << 32
	// Action_SHARE_TX_INSPECT masks TX_INSPECT if the handle is shared.
	Action_SHARE_TX_INSPECT = Action_TX_INSPECT << 32
	// Action_SHARE_TX_LOAD masks TX_LOAD if the handle is shared.
	Action_SHARE_TX_LOAD = Action_TX_LOAD << 32
	// Action_SHARE_TX_POST masks TX_POST if the handle is shared.
	Action_SHARE_TX_POST = Action_TX_POST << 32
	// Action_SHARE_TX_GET masks TX_GET if the handle is shared.
	Action_SHARE_TX_GET = Action_TX_GET << 32
	// Action_SHARE_TX_EXISTS masks TX_EXISTS if the handle is shared.
	Action_SHARE_TX_EXISTS = Action_TX_EXISTS << 32
	// Action_SHARE_TX_DELETE masks TX_DELETE if the handle is shared.
	Action_SHARE_TX_DELETE = Action_TX_DELETE << 32
	// Action_SHARE_TX_ADD_FROM masks TX_ADD_FROM if the handle is shared.
	Action_SHARE_TX_ADD_FROM = Action_TX_COPY_FROM << 32
	// Action_SHARE_TX_LINK_FROM masks TX_LINK_FROM if the handle is shared.
	Action_SHARE_TX_LINK_FROM = Action_TX_LINK_FROM << 32
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
	switch r {
	case math.MaxUint64:
		return "ALL"
	case 0:
		return "NONE"
	}
	parts := []string{}
	rs := map[ActionSet]string{
		Action_ACK: "ACK",

		Action_TX_INSPECT:   "TX_INSPECT",
		Action_TX_LOAD:      "TX_LOAD",
		Action_TX_POST:      "TX_POST",
		Action_TX_GET:       "TX_GET",
		Action_TX_EXISTS:    "TX_EXISTS",
		Action_TX_DELETE:    "TX_DELETE",
		Action_TX_COPY_FROM: "TX_COPY_FROM",
		Action_TX_COPY_TO:   "TX_COPY_TO",
		Action_TX_LINK_FROM: "TX_LINK_FROM",

		Action_VOLUME_INSPECT:  "VOLUME_INSPECT",
		Action_VOLUME_BEGIN_TX: "VOLUME_BEGIN_TX",
		Action_VOLUME_SUB_TO:   "VOLUME_SUB_TO",
		Action_VOLUME_LINK_TO:  "VOLUME_LINK_TO",
		Action_VOLUME_CLONE:    "VOLUME_CLONE",
		Action_VOLUME_CREATE:   "VOLUME_CREATE",
	}
	for r2, str := range rs {
		if r&r2 != 0 {
			parts = append(parts, str)
		}
	}
	return strings.Join(parts, "|")
}
