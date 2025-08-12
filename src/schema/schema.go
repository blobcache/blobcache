// Package schema implements Schemas for blobcache volumes.
package schema

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/cadata"
)

// Schema is the most general Schema type.
// All a Schema has to be able to do is validate the contents of a Volume.
type Schema interface {
	// Validate returns nil if the contents of the volume are valid.
	Validate(ctx context.Context, s cadata.Getter, prev, next []byte) error
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
	ReadLinks(ctx context.Context, s cadata.Getter, root []byte, dst map[blobcache.OID]blobcache.ActionSet) error
}

// None is a Schema which does not impose any constraints on the contents of a volume.
type None struct{}

func (None) Validate(ctx context.Context, s cadata.Getter, prev, next []byte) error {
	return nil
}
