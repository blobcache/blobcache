// Package schema implements Schemas for blobcache volumes.
package schema

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
)

type Spec = blobcache.SchemaSpec

// Factory creates a Schema from a Spec.
type Factory = func(blobcache.SchemaSpec) (Schema, error)

// Constructor is a function that constructs a Schema from its parameters.
type Constructor = func(params json.RawMessage, mkSchema Factory) (Schema, error)

// Value is the contents of a volume.
type Value struct {
	Cell  []byte
	Store bcsdk.RO
}

// Change is a change to a Volume.
type Change struct {
	Prev, Next Value
}

// Schema is the most general Schema type.
// All a Schema has to be able to do is validate the contents of a Volume.
type Schema interface {
	// ValidateChange returns nil if the state transition is valid.
	ValidateChange(ctx context.Context, change Change) error
}

// None is a Schema which does not impose any constraints on the contents of a volume.
type None struct{}

func NoneConstructor(_ json.RawMessage, _ Factory) (Schema, error) {
	return None{}, nil
}

func (None) ValidateChange(ctx context.Context, change Change) error {
	return nil
}

type Exists interface {
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
}

func ExistsUnit(ctx context.Context, s Exists, cid blobcache.CID) (bool, error) {
	var dst [1]bool
	if err := s.Exists(ctx, []blobcache.CID{cid}, dst[:]); err != nil {
		return false, err
	}
	return dst[0], nil
}

type (
	RO = bcsdk.RO
	RW = bcsdk.RW
	WO = bcsdk.WO
)
