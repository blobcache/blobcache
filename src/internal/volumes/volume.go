package volumes

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type System[K any] interface {
	Create(ctx context.Context, k K, spec blobcache.VolumeSpec) error
	Open(ctx context.Context, k K) (Volume, error)
	Drop(ctx context.Context, k K) error
	Clone(ctx context.Context, k K) (Volume, error)
}

type Volume interface {
	BeginTx(ctx context.Context, spec blobcache.TxParams) (Tx, error)
	// Await blocks until the volume root changes away from prev to something else.
	// The next state is written to next.
	Await(ctx context.Context, prev []byte, next *[]byte) error
}

// Tx is a consistent view of a volume, during a transaction.
type Tx interface {
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error

	Save(ctx context.Context, src []byte) error
	Load(ctx context.Context, dst *[]byte) error

	Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error)
	Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error)
	Delete(ctx context.Context, cids []blobcache.CID) error
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
	IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error
	Visit(ctx context.Context, cids []blobcache.CID) error

	MaxSize() int
	Hash(salt *blobcache.CID, data []byte) blobcache.CID

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

// This is an adapter to a store, since we added salts to the API.
type UnsaltedStore struct {
	inner Tx
}

func NewUnsaltedStore(inner Tx) *UnsaltedStore {
	return &UnsaltedStore{inner: inner}
}

func (v UnsaltedStore) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return v.inner.Post(ctx, nil, data)
}

func (v UnsaltedStore) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return v.inner.Get(ctx, cid, nil, buf)
}

func (v UnsaltedStore) Delete(ctx context.Context, cid blobcache.CID) error {
	return v.inner.Delete(ctx, []blobcache.CID{cid})
}

func (v UnsaltedStore) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	dst := [1]bool{}
	if err := v.inner.Exists(ctx, []blobcache.CID{cid}, dst[:]); err != nil {
		return false, err
	}
	return dst[0], nil
}

func (v UnsaltedStore) MaxSize() int {
	return v.inner.MaxSize()
}

func (v UnsaltedStore) Hash(data []byte) blobcache.CID {
	return v.inner.Hash(nil, data)
}
