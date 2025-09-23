package blobcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"go.brendoncarroll.net/state/cadata"
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

func RandomOID() (ret OID) {
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
	// GC causes the transaction to remove all blobs that have not been
	// observed in the transaction.
	// This happens at the end of the transaction.
	// Mutate must be true if GC is set, or BeginTx will return an error.
	GC bool
}

func (tp TxParams) Validate() error {
	if tp.Mutate && !tp.GC {
		return fmt.Errorf("mutate must be true if GC is set")
	}
	return nil
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
	Params   TxParams
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

// PostOpts contains options for the Post method.
type PostOpts struct {
	Salt *CID
}

// GetOpts contains options for the Get method.
type GetOpts struct {
	// Salt is required to verify the data, if the volume uses salts.
	Salt *CID
	// SkipVerify causes the retrieved data not to be verified against the CID.
	// This should only be done if you are going to verify the data at a higher level
	// or if you consider the specific volume backend to be inside your security perimeter.
	SkipVerify bool
}

type HandleAPI interface {
	// Drop causes a handle to be released immediately.
	// If all the handles to an object are dropped, the object is deleted.
	Drop(ctx context.Context, h Handle) error
	// KeepAlive extends the TTL for some handles.
	KeepAlive(ctx context.Context, hs []Handle) error
	// InspectHandle returns info about a handle.
	InspectHandle(ctx context.Context, h Handle) (*HandleInfo, error)
}

type VolumeAPI interface {
	// CreateVolume creates a new volume.
	// CreateVolume always creates a Volume on the local Node.
	// CreateVolume returns a handle to the Volume.  If no other references to the Volume
	// have been created by the time the handle expires, the Volume will be deleted.
	// Leave caller nil to skip Authorization checks.
	// Host describes where the Volume should be created.
	// If the Host is nil, the Volume will be created on the local Node.
	CreateVolume(ctx context.Context, host *Endpoint, vspec VolumeSpec) (*Handle, error)
	// InspectVolume returns info about a Volume.
	InspectVolume(ctx context.Context, h Handle) (*VolumeInfo, error)
	// OpenAs returns a handle to an object by it's ID.
	// PeerID is the peer that is opening the handle.
	// This is where any Authorization checks are done.
	OpenAs(ctx context.Context, x OID, mask ActionSet) (*Handle, error)
	// OpenFrom returns a handle to an object by it's ID.
	// base is the handle of a Volume, which links to the object.
	// the base Volume's schema must be a Container.
	OpenFrom(ctx context.Context, base Handle, x OID, mask ActionSet) (*Handle, error)
	// Await waits for a set of conditions to be met.
	Await(ctx context.Context, cond Conditions) error
	// BeginTx begins a new transaction, on a Volume.
	BeginTx(ctx context.Context, volh Handle, txp TxParams) (*Handle, error)
	// CloneVolume clones a Volume, copying it's configuration, blobs, and cell data.
	CloneVolume(ctx context.Context, caller *PeerID, volh Handle) (*Handle, error)
}

type TxAPI interface {
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
	Post(ctx context.Context, tx Handle, data []byte, opts PostOpts) (CID, error)
	// Get returns the data for a CID.
	Get(ctx context.Context, tx Handle, cid CID, buf []byte, opts GetOpts) (int, error)
	// Exists checks if several CID exists in the volume
	// len(dst) must be equal to len(cids), or Exists will return an error.
	Exists(ctx context.Context, tx Handle, cids []CID, dst []bool) error
	// Delete deletes a CID from the volume
	Delete(ctx context.Context, tx Handle, cids []CID) error
	// AddFrom has the same effect as Post, but it does not require sending the data to Blobcache.
	// It returns a slice of booleans, indicating if the CID could be added.
	AddFrom(ctx context.Context, tx Handle, cids []CID, srcTxns []Handle, success []bool) error
	// AllowLink allows the Volume to reference another volume.
	// The volume must still have a recognized Container Schema for the volumes to be persisted.
	AllowLink(ctx context.Context, tx Handle, subvol Handle) error
	// Visit is only usable in a GC transaction.
	// It marks each CID as being visited, so it will not be removed by GC.
	Visit(ctx context.Context, tx Handle, cids []CID) error
	// IsVisited is only usable in a GC transaction.
	// It checks if each CID has been visited.
	IsVisited(ctx context.Context, tx Handle, cids []CID, yesVisited []bool) error
}

type Service interface {
	// Endpoint returns the endpoint of the service.
	// If the endpoint is the zero value, the service is not listening for peers.
	Endpoint(ctx context.Context) (Endpoint, error)

	HandleAPI
	VolumeAPI
	TxAPI
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
