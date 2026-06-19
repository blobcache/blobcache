package backend

import (
	"context"

	"blobcache.io/blobcache/src/bccore"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

type (
	Volume = bccore.Volume
	Tx     = bccore.Tx
	Queue  = bccore.Queue
)

type QueueSystem[Spec any, Q Queue] interface {
	// CreateQueue creates a new queue from the spec and returns it.
	CreateQueue(ctx context.Context, spec Spec) (Q, error)
}

type VolumeSystem[Params any, V Volume] interface {
	// VolumeUp loads volume state into memory.
	// This should be called to begin using the Volume.
	VolumeUp(ctx context.Context, spec Params) (V, error)

	// VolumeDestroy should remove all state associated with the volume
	VolumeDestroy(ctx context.Context, vol V) error
}

// System is a full backend System, supporting Volumes, Queues
// and subscriptions on Volumes.
type System[VP any, V Volume, QP any, Q Queue] interface {
	VolumeSystem[VP, V]
	QueueSystem[QP, Q]
}

type LinkSet = map[[32]byte]blobcache.OID

func ViewUnsalted(ctx context.Context, tx Tx) (*UnsaltedStore, []byte, error) {
	var root []byte
	if err := tx.Load(ctx, 0, &root); err != nil {
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

func (v UnsaltedStore) Exists(ctx context.Context, cids []blobcache.CID, dst *blobcache.BitMap) error {
	return v.inner.Exists(ctx, cids, dst)
}

func (v UnsaltedStore) MaxSize() int {
	return v.inner.MaxSize()
}

func (v UnsaltedStore) Hash(data []byte) blobcache.CID {
	return v.inner.HashAlgo().Hash(data)
}

func (v UnsaltedStore) HashAlgo() blobcache.HashAlgo {
	return v.inner.HashAlgo()
}

// TxBased is a Tx which can return a read-only view of it's base.
type TxBased interface {
	Tx
	// Base returns a read-only transaction representing the inital state of the transaction
	Base() Tx
}

func CheckSchema(ctx context.Context, sch schema.Schema, tx TxBased, loaded []blobcache.CellKey) error {
	baseTx := tx.Base()
	var prevCell []byte
	if err := baseTx.Load(ctx, 0, &prevCell); err != nil {
		return err
	}
	var nextCell []byte
	if err := tx.Load(ctx, 0, &nextCell); err != nil {
		return err
	}
	return sch.ValidateChange(ctx, schema.Change{
		Prev: schema.Value{
			Cell:  prevCell,
			Store: NewUnsaltedStore(baseTx),
		},
		Next: schema.Value{
			Cell:  nextCell,
			Store: NewUnsaltedStore(tx),
		},
	})
}
