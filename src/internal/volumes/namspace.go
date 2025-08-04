package volumes

import (
	"context"
	"iter"

	"blobcache.io/blobcache/src/blobcache"
)

type Namespace interface {
	// GetEntry returns the entry with the given name.
	// If the entry does not exist, it returns (nil, nil)
	GetEntry(ctx context.Context, name string) (*blobcache.Entry, error)
	// PutEntry creates or updates the entry with the given name.
	PutEntry(ctx context.Context, x blobcache.Entry) error
	// ListEntries returns all the entries in the namespace.
	ListEntries(ctx context.Context) ([]blobcache.Entry, error)
	// DeleteEntry deletes the entry with the given name.
	DeleteEntry(ctx context.Context, name string) error
}

// AllOIDs returns an iterator over all the object IDs referenced by the entries.
func AllOIDs(ents []blobcache.Entry) iter.Seq[blobcache.OID] {
	return func(yield func(blobcache.OID) bool) {
		for _, ent := range ents {
			if !yield(ent.Target) {
				return
			}
		}
	}
}
