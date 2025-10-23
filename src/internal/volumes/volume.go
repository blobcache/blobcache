package volumes

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type System[Params any, V Volume] interface {
	// Up loads volume state into memory.
	// This should be called to begin using the Volume.
	Up(ctx context.Context, spec Params) (V, error)

	// Drop should remove all state associated with the volume
	Drop(ctx context.Context, vol V) error
}

type Volume interface {
	BeginTx(ctx context.Context, spec blobcache.TxParams) (Tx, error)
	// Await blocks until the volume root changes away from prev to something else.
	// The next state is written to next.
	Await(ctx context.Context, prev []byte, next *[]byte) error
	// GetLink returns the set of actions associated with the link.
	GetLink(ctx context.Context, target blobcache.OID) (blobcache.ActionSet, error)
}

// Tx is a consistent view of a volume, during a transaction.
type Tx interface {
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error

	Save(ctx context.Context, src []byte) error
	Load(ctx context.Context, dst *[]byte) error

	Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error)
	Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error)
	Delete(ctx context.Context, cids []blobcache.CID) error
	Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error
	IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error
	Visit(ctx context.Context, cids []blobcache.CID) error

	MaxSize() int
	Hash(salt *blobcache.CID, data []byte) blobcache.CID

	// Link creates adds a handle to prove access to a volume.
	Link(ctx context.Context, target blobcache.OID, rights blobcache.ActionSet) error
	// Unlink removes a link from the volume.
	Unlink(ctx context.Context, targets []blobcache.OID) error
	// VisitLinks visits a link to another volume.
	// This is only usable in a GC transaction.
	VisitLinks(ctx context.Context, targets []blobcache.OID) error
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
	return v.inner.Post(ctx, data, blobcache.PostOpts{})
}

func (v UnsaltedStore) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return v.inner.Get(ctx, cid, buf, blobcache.GetOpts{})
}

func (v UnsaltedStore) Delete(ctx context.Context, cids []blobcache.CID) error {
	return v.inner.Delete(ctx, cids)
}

func (v UnsaltedStore) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	if err := v.inner.Exists(ctx, cids, dst[:]); err != nil {
		return err
	}
	return nil
}

func (v UnsaltedStore) MaxSize() int {
	return v.inner.MaxSize()
}

func (v UnsaltedStore) Hash(data []byte) blobcache.CID {
	return v.inner.Hash(nil, data)
}
