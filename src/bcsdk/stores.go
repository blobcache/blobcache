package bcsdk

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

// RO is read-only Store methods
type RO interface {
	Exists
	Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error)
	Hash(data []byte) blobcache.CID
	MaxSize() int
}

// WO contains write-only methods
type WO interface {
	Exists
	Post(ctx context.Context, data []byte) (blobcache.CID, error)
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

// Exists contains the Exists method
type Exists interface {
	Exists(ctx context.Context, cids []blobcache.CID, dst *blobcache.BitMap) error
}

// ExistsUnit checks if a single CID exists in a Store
func ExistsUnit(ctx context.Context, s Exists, cid blobcache.CID) (bool, error) {
	var dst blobcache.BitMap
	if err := s.Exists(ctx, []blobcache.CID{cid}, &dst); err != nil {
		return false, err
	}
	return dst.IsSet(0), nil
}
