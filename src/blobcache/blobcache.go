// Package blobcache is a temporary standin for the Blobcache API at github.com/blobcache/blobcache
package blobcache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"go.brendoncarroll.net/state/cadata"
)

// CID is a content identifier.
type CID = cadata.ID

// HashFunc is a cryptographic hash function.
type HashFunc = cadata.HashFunc

// HashAlgo is a cryptographic hash algorithm.
type HashAlgo string

const (
	HashAlgo_BLAKE3_256  HashAlgo = "blake3-256"
	HashAlgo_BLAKE2b_256 HashAlgo = "blake2b-256"
	HashAlgo_SHA2_256    HashAlgo = "sha2-256"
	HashAlgo_SHA3_256    HashAlgo = "sha3-256"
)

// OID is an object identifier.
type OID [16]byte

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

// Conditions is a set of conditions to await.
type Conditions struct {
	AllEqual []Handle `json:"all_equal,omitempty"`
}

type Service interface {
	// CreateVolume creates a new volume.
	CreateVolume(ctx context.Context, vspec VolumeSpec) (*Handle, error)
	// Anchor causes a handle to be kept alive indefinitely.
	// This should only be called after a Volume has been successfully created
	// and the handle has been saved.
	Anchor(ctx context.Context, h Handle) error
	// Drop causes a handle to be released immediately.
	// If all the handles to an object are dropped, the object is deleted.
	Drop(ctx context.Context, h Handle) error
	// KeepAlive keeps a handle alive.
	KeepAlive(ctx context.Context, hs []Handle) error
	// Await waits for a set of conditions to be met.
	Await(ctx context.Context, cond Conditions) error

	// StartSync begins syncing from src to dst.
	// Await can be called to wait for the 2 volumes to become equal.
	StartSync(ctx context.Context, src Handle, dst Handle) error

	// CreateRule creates a new rule.
	CreateRule(ctx context.Context, rspec RuleSpec) (*Handle, error)

	// BeginTx begins a new transaction.
	BeginTx(ctx context.Context, base Handle, mutate bool) (*Handle, error)
	// Commit commits a transaction.
	Commit(ctx context.Context, tx Handle, root []byte) error
	// Abort aborts a transaction.
	Abort(ctx context.Context, tx Handle) error
	// Load loads the volume root into dst
	Load(ctx context.Context, tx Handle, dst *[]byte) error
	// Post posts data to the volume
	Post(ctx context.Context, tx Handle, data []byte) (CID, error)
	// Exists checks if a CID exists in the volume
	Exists(ctx context.Context, tx Handle, cid CID) (bool, error)
	// Delete deletes a CID from the volume
	Delete(ctx context.Context, tx Handle, cid CID) error
	// Get returns the data for a CID.
	Get(ctx context.Context, tx Handle, cid CID, buf []byte) (int, error)
}

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

type RuleSpec struct {
	Inputs  []Handle
	Outputs []Handle
	Op      Op
}

type RuleInfo struct {
	ID      OID
	Inputs  []OID
	Outputs []OID
	Op      Op
}

type Op struct {
	Sync  *struct{} `json:"push,omitempty"`
	Merge *struct{} `json:"merge,omitempty"`
}
