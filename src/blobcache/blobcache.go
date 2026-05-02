package blobcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"go.brendoncarroll.net/exp/sbe"
	"go.inet256.org/inet256/src/inet256"
)

// OIDSize is the number of bytes in an OID.
const OIDSize = 16

// OID is an object identifier.
type OID [OIDSize]byte

type OIDPath = []OID

func RandomOID() (ret OID) {
	rand.Read(ret[:])
	return ret
}

func ParseOID(s string) (OID, error) {
	var ret OID
	err := ret.UnmarshalText([]byte(s))
	return ret, err
}

func (o OID) String() string {
	data, _ := o.MarshalText()
	return string(data)
}

func (o OID) Compare(other OID) int {
	return bytes.Compare(o[:], other[:])
}

func (o OID) MarshalText() ([]byte, error) {
	return bytes.Join([][]byte{
		bytes.ToUpper(hex.AppendEncode(nil, o[:4])),
		bytes.ToUpper(hex.AppendEncode(nil, o[4:12])),
		bytes.ToUpper(hex.AppendEncode(nil, o[12:16])),
	}, []byte{'_'}), nil
}

func (o *OID) UnmarshalText(data []byte) error {
	data = bytes.ReplaceAll(data, []byte("_"), nil)
	if len(data) != hex.EncodedLen(len(o)) {
		return fmt.Errorf("invalid OID: %q", data)
	}
	_, err := hex.Decode(o[:], data)
	return err
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

// Value implements the driver.Valuer interface.
func (o OID) Value() (driver.Value, error) {
	return o[:], nil
}

// Scan implements the sql.Scanner interface.
func (o *OID) Scan(src any) error {
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

// NodeID uniquely identifies a peer by hash of the public key.
type NodeID = inet256.ID

const NodeIDSize = inet256.AddrSize

// TxLimits enforce limits on a transaction
// TxLimits are not for implementing read-write-delete permissions;
// they are to be used for quality of service only.
type TxLimits struct {
	// Duration is the number of milliseconds to limit the transaction to before it is automatically closed.
	Duration *uint32 `json:"duration,omitempty"`
	// PostedBlobs is the total number of blobs which can be uploaded in the transaction
	PostedBlobs *uint64 `json:"posted_blobs,omitempty"`
	// PostedBytes are the number of total bytes which can be uploaded in the transaction
	PostedBytes *uint64 `json:"posted_bytes,omitempty"`
}

// TxParams are parameters for a transaction.
// The zero value is a read-only transaction.
type TxParams struct {
	// Modify is true if the transaction will change the Volume's state.
	Modify bool `json:"modify,omitempty"`
	// GCBlobs causes the transaction to remove all blobs that have not been
	// visited in the transaction.
	// This happens at the end of the transaction.
	// Modify must be true if GCBlobs is set, or BeginTx will return an error.
	GCBlobs bool `json:"gc_blobs,omitempty"`
	// GCLinks causes the transaction to remove, on commit, all links that have not been
	// Visited in the transaction
	// Modify must be true if GCLinks is set, or BeginTx will return an error
	GCLinks bool `json:"gc_links,omitempty"`

	// Limits
	Limits TxLimits `json:"limits,omitempty"`
}

func (tp TxParams) Validate() error {
	if (tp.GCBlobs || tp.GCLinks) && !tp.Modify {
		return fmt.Errorf("mutate must be true if GC is set")
	}
	return nil
}

func (tp TxParams) Marshal(out []byte) []byte {
	flags := uint32(0)
	for i, b := range []bool{
		tp.Modify,
		tp.GCBlobs,
		tp.GCLinks,
		tp.Limits.Duration != nil,
		tp.Limits.PostedBlobs != nil,
		tp.Limits.PostedBytes != nil,
	} {
		if b {
			flags |= 1 << i
		}
	}
	out = sbe.AppendUint32(out, flags)
	if tp.Limits.Duration != nil {
		out = sbe.AppendUint32(out, *tp.Limits.Duration)
	}
	if tp.Limits.PostedBlobs != nil {
		out = sbe.AppendUint64(out, *tp.Limits.PostedBlobs)
	}
	if tp.Limits.PostedBytes != nil {
		out = sbe.AppendUint64(out, *tp.Limits.PostedBytes)
	}
	return out
}

func (tp *TxParams) Unmarshal(data []byte) error {
	flags, data, err := sbe.ReadUint32(data)
	if err != nil {
		return err
	}

	tp.Modify = flags&(1<<0) != 0
	tp.GCBlobs = flags&(1<<1) != 0
	tp.GCLinks = flags&(1<<2) != 0

	if flags&(1<<3) != 0 {
		x, rest, err := sbe.ReadUint32(data)
		if err != nil {
			return err
		}
		tp.Limits.Duration = &x
		data = rest
	} else {
		tp.Limits.Duration = nil
	}

	if flags&(1<<4) != 0 {
		x, rest, err := sbe.ReadUint64(data)
		if err != nil {
			return err
		}
		tp.Limits.PostedBlobs = &x
		data = rest
	} else {
		tp.Limits.PostedBlobs = nil
	}

	if flags&(1<<5) != 0 {
		x, rest, err := sbe.ReadUint64(data)
		if err != nil {
			return err
		}
		tp.Limits.PostedBytes = &x
		data = rest
	} else {
		tp.Limits.PostedBytes = nil
	}

	if flags&^uint32((1<<6)-1) != 0 {
		return fmt.Errorf("TxParams: unknown flags set: 0x%x", flags&^uint32((1<<6)-1))
	}
	if len(data) > 0 {
		return fmt.Errorf("TxParams: trailing bytes: %d", len(data))
	}
	return nil
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

// LinkTokenSize is the size of a LinkToken in bytes
const LinkTokenSize = 16 + 8 + 24

// LTSecret is a LinkToken secret
type LTSecret [24]byte

func (lt LTSecret) MarshalText() ([]byte, error) {
	return hex.AppendEncode(nil, lt[:]), nil
}

func (lt *LTSecret) UnmarshalText(data []byte) error {
	n, err := hex.Decode(lt[:], data)
	if err != nil {
		return err
	}
	if n != len(lt) {
		return fmt.Errorf("wrong size for LinkToken secret")
	}
	return nil
}

// LinkToken provides proof of Access to another Volume.
type LinkToken struct {
	Target OID       `json:"target"`
	Rights ActionSet `json:"rights"`
	Secret LTSecret  `json:"secret"`
}

func (lt LinkToken) Marshal(out []byte) []byte {
	out = append(out, lt.Target[:]...)
	out = lt.Rights.Marshal(out)
	out = append(out, lt.Secret[:]...)
	return out
}

func (lt *LinkToken) Unmarshal(data []byte) error {
	if len(data) != LinkTokenSize {
		return fmt.Errorf("wrong size for link token. HAVE: %d", len(data))
	}
	copy(lt.Target[:], data[:OIDSize])
	if err := lt.Rights.Unmarshal(data[OIDSize : OIDSize+8]); err != nil {
		return err
	}
	copy(lt.Secret[:], data[OIDSize+8:])
	return nil
}

func (lt LinkToken) String() string {
	return hex.EncodeToString(lt.Marshal(nil))
}

type Info struct {
	Handle HandleInfo `json:"handle"`

	Volume *VolumeInfo `json:"volume,omitempty"`
	Tx     *TxInfo     `json:"tx,omitempty"`
	Queue  *QueueInfo  `json:"queue,omitempty"`
}

type Service interface {
	// Endpoint returns the endpoint of the service.
	// If the endpoint is the zero value, the service is not listening for peers.
	Endpoint(ctx context.Context) (Endpoint, error)

	// OpenFiat returns a handle to an object by it's ID.
	// This is where any Authorization checks are done.
	// It's called "fiat" because it's up to the Node to say yes or no.
	// The result is implementation dependent, unlike OpenFrom, which should behave
	// the same way on any Node.
	// If not nil, then the Endpoint is used for a remote OpenFiat call
	OpenFiat(ctx context.Context, x OID, mask ActionSet) (*Handle, error)

	HandleAPI
	VolumeAPI
	TxAPI
	QueueAPI
}

// CheckBlob checks that the data matches the expected CID.
// If there is a problem, it returns an ErrBadData.
func CheckBlob(ha HashAlgo, salt, cid *CID, data []byte) error {
	var actualCID CID
	if salt == nil {
		actualCID = ha.Hash(data)
	} else {
		hf := ha.KeyedHash
		actualCID = hf(salt, data)
	}
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
