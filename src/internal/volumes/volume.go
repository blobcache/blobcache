package volumes

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type Volume interface {
	BeginTx(ctx context.Context, spec blobcache.TxParams) (Tx, error)
	// Await blocks until the volume root changes away from prev to something else.
	// The next state is written to next.
	Await(ctx context.Context, prev []byte, next *[]byte) error
}

// Tx is a consistent view of a volume, during a transaction.
type Tx interface {
	Commit(ctx context.Context, root []byte) error
	Abort(ctx context.Context) error

	Load(ctx context.Context, dst *[]byte) error

	Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error)
	Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error)
	Delete(ctx context.Context, cid blobcache.CID) error
	Exists(ctx context.Context, cid blobcache.CID) (bool, error)
	MaxSize() int
	Hash(salt *blobcache.CID, data []byte) blobcache.CID
}

type TypedVolume[Root any] interface {
	BeginTx(ctx context.Context, spec blobcache.TxParams) (TypedTx[Root], error)
	// Await blocks until the volume root changes away from prev to something else.
	Await(ctx context.Context, prev []byte, next *[]byte) error
}

type TypedTx[Root any] interface {
	Commit(ctx context.Context, root Root) error
	Abort(ctx context.Context) error

	Load(ctx context.Context, dst *Root) error

	Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error)
	Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error)
	Delete(ctx context.Context, cid blobcache.CID) error
	Exists(ctx context.Context, cid blobcache.CID) (bool, error)
	MaxSize() int
	Hash(salt *blobcache.CID, data []byte) blobcache.CID
}
