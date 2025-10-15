// Package schema implements Schemas for blobcache volumes.
package schema

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
)

type Spec = blobcache.SchemaSpec

// Factory creates a Schema from a Spec.
type Factory = func(blobcache.SchemaSpec) (Schema, error)

// Constructor is a function that constructs a Schema from its parameters.
type Constructor = func(params json.RawMessage, mkSchema Factory) (Schema, error)

// Change is a change to a Volume.
type Change struct {
	PrevCell  []byte
	NextCell  []byte
	PrevStore RO
	NextStore RO
}

// Schema is the most general Schema type.
// All a Schema has to be able to do is validate the contents of a Volume.
type Schema interface {
	// ValidateChange returns nil if the state transition is valid.
	ValidateChange(ctx context.Context, change Change) error
}

// Link is a reference from one volume to another.
type Link struct {
	// Target is the OID of the volume being referenced.
	Target blobcache.OID
	// Rights are the set of actions on the target, which are granted to the caller.
	Rights blobcache.ActionSet
}

// Container is a Schema which can store Links to other volumes.
type Container interface {
	Schema

	// ReadLinks returns a list of links for a given root.
	ReadLinks(ctx context.Context, s RO, root []byte, dst map[blobcache.OID]blobcache.ActionSet) error
}

// None is a Schema which does not impose any constraints on the contents of a volume.
type None struct{}

func NoneConstructor(_ json.RawMessage, _ Factory) (Schema, error) {
	return None{}, nil
}

func (None) ValidateChange(ctx context.Context, change Change) error {
	return nil
}

// RO is read-only Store methods
type RO interface {
	Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error)
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
	Hash(data []byte) blobcache.CID
	MaxSize() int
}

type WO interface {
	Post(ctx context.Context, data []byte) (blobcache.CID, error)
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
	Hash(data []byte) blobcache.CID
	MaxSize() int
}

// RW is Read-Write Store methods
type RW interface {
	RO
	WO
}

// RWD is Read-Write-Delete Store methods
type RWD interface {
	RW
	Delete(ctx context.Context, cids []blobcache.CID) error
}
