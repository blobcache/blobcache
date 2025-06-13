package volumes

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type Volume[Root any] interface {
	BeginTx(ctx context.Context, mutate bool) (Tx[Root], error)
}

// Tx is a consistent view of a volume, during a transaction.
type Tx[Root any] interface {
	Commit(ctx context.Context, root Root) error
	Abort(ctx context.Context) error

	Load(ctx context.Context, dst *Root) error

	Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error)
	Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error)
	Delete(ctx context.Context, cid blobcache.CID) error
	Exists(ctx context.Context, cid blobcache.CID) (bool, error)
	MaxSize() int
	Hash(data []byte) blobcache.CID
}
