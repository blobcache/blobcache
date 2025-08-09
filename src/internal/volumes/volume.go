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
	// Volume should return the volume that this tx is operating on.
	Volume() Volume

	// AllowLink creates adds a handle to prove access to a volume.
	AllowLink(ctx context.Context, subvol blobcache.Handle) error
}

func ViewUnsalted(ctx context.Context, tx Tx) (*UnsaltedStore, []byte, error) {
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return nil, nil, err
	}
	return NewUnsaltedStore(tx), root, nil
}
