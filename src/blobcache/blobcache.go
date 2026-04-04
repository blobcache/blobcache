package blobcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"

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

// PeerID uniquely identifies a peer by hash of the public key.
type PeerID = inet256.ID

const PeerIDSize = inet256.AddrSize

// TxParams are parameters for a transaction.
// The zero value is a read-only transaction.
type TxParams struct {
	// Modify is true if the transaction will change the Volume's state.
	Modify bool
	// GCBlobs causes the transaction to remove all blobs that have not been
	// visited in the transaction.
	// This happens at the end of the transaction.
	// Modify must be true if GCBlobs is set, or BeginTx will return an error.
	GCBlobs bool
	// GCLinks causes the transaction to remove, on commit, all links that have not been
	// Visited in the transaction
	// Modify must be true if GCLinks is set, or BeginTx will return an error
	GCLinks bool
}

func (tp TxParams) Validate() error {
	if (tp.GCBlobs || tp.GCLinks) && !tp.Modify {
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

	HandleAPI
	VolumeAPI
	TxAPI
	QueueAPI
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
