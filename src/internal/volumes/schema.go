package volumes

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/cadata"
)

type Schema interface {
	Validate(ctx context.Context, s cadata.Getter, root []byte) error
}

// Link references another subvolume, with a set of rights.
type Link struct {
	Rights blobcache.ActionSet
	Target blobcache.OID
}

// Container is a Schema which can store Links to other volumes.
type Container interface {
	Schema
	// ListLinks returns a list of links for a given root.
	ListLinks(ctx context.Context, s cadata.Getter, root []byte) ([]Link, error)
}
