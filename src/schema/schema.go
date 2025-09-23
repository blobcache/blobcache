// Package schema implements Schemas for blobcache volumes.
package schema

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

// Schema is the most general Schema type.
// All a Schema has to be able to do is validate the contents of a Volume.
type Schema interface {
	// Validate returns nil if the contents of the volume are valid.
	Validate(ctx context.Context, s RO, prev, next []byte) error
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

func (None) Validate(ctx context.Context, s RO, prev, next []byte) error {
	return nil
}

// RO is read-only Store methods
type RO interface {
	Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error)
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
	MaxSize() int
}

// RW is Read-Write Store methods
type RW interface {
	RO
	Post(ctx context.Context, data []byte) (blobcache.CID, error)
}

// RWD is Read-Write-Delete Store methods
type RWD interface {
	RW
	Delete(ctx context.Context, cids []blobcache.CID) error
}
