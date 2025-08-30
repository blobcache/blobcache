package blobcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha3"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/crypto/blake2b"
	"lukechampine.com/blake3"
)

// CID is a content identifier.
// It is produced by hashing data.
// CIDs can be used as salts.
// CIDs are cannonically printed in an order-preserving base64 encoding, which distinguishes
// them from OIDs which are printed as hex.
type CID = cadata.ID

// CIDSize is the number of bytes in a CID.
const CIDSize = cadata.IDSize

func ParseCID(s string) (CID, error) {
	var ret CID
	if err := ret.UnmarshalBase64([]byte(s)); err != nil {
		return CID{}, err
	}
	return ret, nil
}

// HashFunc is a cryptographic hash function.
type HashFunc func(salt *CID, data []byte) CID

// HashAlgo is a cryptographic hash algorithm.
type HashAlgo string

const (
	HashAlgo_BLAKE3_256  HashAlgo = "blake3-256"
	HashAlgo_BLAKE2b_256 HashAlgo = "blake2b-256"
	HashAlgo_SHA2_256    HashAlgo = "sha2-256"
	HashAlgo_SHA3_256    HashAlgo = "sha3-256"
)

func (h HashAlgo) Validate() error {
	switch h {
	case HashAlgo_BLAKE3_256, HashAlgo_BLAKE2b_256, HashAlgo_SHA2_256, HashAlgo_SHA3_256:
		return nil
	}
	return fmt.Errorf("unknown hash algo: %q", h)
}

func (h HashAlgo) HashFunc() HashFunc {
	switch h {
	case HashAlgo_SHA2_256:
		return func(salt *CID, x []byte) CID {
			if salt != nil {
				panic("salt not supported for sha2-256")
			}
			return sha256.Sum256(x)
		}
	case HashAlgo_SHA3_256:
		return func(salt *CID, x []byte) CID {
			if salt == nil {
				return sha3.Sum256(x)
			}
			h := sha3.NewCSHAKE256(nil, salt[:])
			h.Write(x)
			var ret CID
			if _, err := h.Read(ret[:]); err != nil {
				panic(err)
			}
			return ret
		}
	case HashAlgo_BLAKE2b_256:
		return func(salt *CID, x []byte) CID {
			if salt == nil {
				return blake2b.Sum256(x)
			}
			h, err := blake2b.New(32, salt[:])
			if err != nil {
				panic(err)
			}
			h.Write(x)
			var ret CID
			copy(ret[:], h.Sum(nil))
			return ret
		}
	case HashAlgo_BLAKE3_256:
		return func(salt *CID, x []byte) CID {
			if salt == nil {

				return blake3.Sum256(x)
			}
			h := blake3.New(32, salt[:])
			h.Write(x)
			var ret CID
			copy(ret[:], h.Sum(nil))
			return ret
		}
	default:
		panic(h)
	}
}

// OIDSize is the number of bytes in an OID.
const OIDSize = 16

// OID is an object identifier.
type OID [OIDSize]byte

func (o OID) Compare(other OID) int {
	return bytes.Compare(o[:], other[:])
}

func (o OID) Marshal(out []byte) []byte {
	return append(out, o[:]...)
}

func (o *OID) Unmarshal(data []byte) error {
	if len(data) < OIDSize {
		return fmt.Errorf("OID: data too short: %d", len(data))
	}
	copy(o[:], data)
	return nil
}

func NewOID() (ret OID) {
	rand.Read(ret[:])
	return ret
}

func ParseOID(s string) (OID, error) {
	var ret OID
	if len(s) != hex.EncodedLen(len(ret)) {
		return OID{}, fmt.Errorf("invalid OID: %s", s)
	}
	hex.Decode(ret[:], []byte(s))
	return ret, nil
}

func (o OID) String() string {
	return strings.ToUpper(hex.EncodeToString(o[:]))
}

// Value implements the driver.Valuer interface.
func (o OID) Value() (driver.Value, error) {
	return o[:], nil
}

// Scan implements the sql.Scanner interface.
func (o *OID) Scan(src interface{}) error {
	if src == nil {
		return fmt.Errorf("OID: cannot scan nil src")
	}
	// TODO: should we support string scanning?
	switch src := src.(type) {
	case []byte:
		if len(src) != len(o) {
			return fmt.Errorf("OID: cannot scan []byte of len %d", len(src))
		}
		copy(o[:], src)
		return nil
	}
	return fmt.Errorf("OID: cannot scan %T", src)
}

// Conditions is a set of conditions to await.
type Conditions struct {
	AllEqual []Handle  `json:"all_equal,omitempty"`
	NOTEqual *NOTEqual `json:"not,omitempty"`
}

func (c Conditions) Marshal(out []byte) []byte {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (c *Conditions) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}

type NOTEqual struct {
	Volume Handle
	Value  []byte
}

// TxParams are parameters for a transaction.
// The zero value is a read-only transaction.
type TxParams struct {
	Mutate bool
}

func (tp TxParams) Marshal(out []byte) []byte {
	data, err := json.Marshal(tp)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (tp *TxParams) Unmarshal(data []byte) error {
	return json.Unmarshal(data, tp)
}

type TxInfo struct {
	ID       OID
	Volume   OID
	MaxSize  int64
	HashAlgo HashAlgo
}

func (ti TxInfo) Marshal(out []byte) []byte {
	data, err := json.Marshal(ti)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (ti *TxInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, ti)
}

type Service interface {
	// Endpoint returns the endpoint of the service.
	// If the endpoint is the zero value, the service is not listening for peers.
	Endpoint(ctx context.Context) (Endpoint, error)

	////
	// Handle methods.
	////

	// Drop causes a handle to be released immediately.
	// If all the handles to an object are dropped, the object is deleted.
	Drop(ctx context.Context, h Handle) error
	// KeepAlive extends the TTL for some handles.
	KeepAlive(ctx context.Context, hs []Handle) error
	// InspectHandle returns info about a handle.
	InspectHandle(ctx context.Context, h Handle) (*HandleInfo, error)

	////
	// Volume methods.
	////

	// CreateVolume creates a new volume.
	// CreateVolume always creates a Volume on the local Node.
	// CreateVolume returns a handle to the Volume.  If no other references to the Volume
	// have been created by the time the handle expires, the Volume will be deleted.
	// Leave caller nil to skip Authorization checks.
	CreateVolume(ctx context.Context, caller *PeerID, vspec VolumeSpec) (*Handle, error)
	// InspectVolume returns info about a Volume.
	InspectVolume(ctx context.Context, h Handle) (*VolumeInfo, error)
	// OpenAs returns a handle to an object by it's ID.
	// PeerID is the peer that is opening the handle.
	// This is where any Authorization checks are done.
	OpenAs(ctx context.Context, caller *PeerID, x OID, mask ActionSet) (*Handle, error)
	// OpenFrom returns a handle to an object by it's ID.
	// base is the handle of a Volume, which links to the object.
	// the base Volume's schema must be a Container.
	OpenFrom(ctx context.Context, base Handle, x OID, mask ActionSet) (*Handle, error)
	// Await waits for a set of conditions to be met.
	Await(ctx context.Context, cond Conditions) error
	// BeginTx begins a new transaction, on a Volume.
	BeginTx(ctx context.Context, volh Handle, txp TxParams) (*Handle, error)

	////
	// Transactions methods.
	////
	// InspectTx returns info about a transaction.
	InspectTx(ctx context.Context, tx Handle) (*TxInfo, error)
	// Commit commits a transaction.
	Commit(ctx context.Context, tx Handle) error
	// Abort aborts a transaction.
	Abort(ctx context.Context, tx Handle) error
	// Load loads the volume root into dst
	Load(ctx context.Context, tx Handle, dst *[]byte) error
	// Save writes to the volume root.
	// Like all operations in a transaction, Save will not be visible until Commit is called.
	Save(ctx context.Context, tx Handle, src []byte) error
	// Post posts data to the volume
	Post(ctx context.Context, tx Handle, salt *CID, data []byte) (CID, error)
	// Exists checks if a CID exists in the volume
	Exists(ctx context.Context, tx Handle, cid CID) (bool, error)
	// Delete deletes a CID from the volume
	Delete(ctx context.Context, tx Handle, cid CID) error
	// Get returns the data for a CID.
	Get(ctx context.Context, tx Handle, cid CID, salt *CID, buf []byte) (int, error)
	// AllowLink allows the Volume to reference another volume.
	// The volume must still have a recognized Container Schema for the volumes to be persisted.
	AllowLink(ctx context.Context, tx Handle, subvol Handle) error
}

// CheckBlob checks that the data matches the expected CID.
// If there is a problem, it returns an ErrBadData.
func CheckBlob(hf HashFunc, salt, cid *CID, data []byte) error {
	actualCID := hf(salt, data)
	if *cid != actualCID {
		return ErrBadData{
			Salt:     salt,
			Expected: *cid,
			Actual:   actualCID,
			Len:      len(data),
		}
	}
	return nil
}
