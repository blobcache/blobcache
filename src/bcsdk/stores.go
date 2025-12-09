package bcsdk

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

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
