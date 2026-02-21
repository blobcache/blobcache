package backend

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type QueueSystem[Spec any, Q Queue] interface {
	// CreateQueue creates a new queue from the spec and returns it.
	CreateQueue(ctx context.Context, spec Spec) (Q, error)
}

type Queue interface {
	// Enqueue adds messages to the Queue.
	// It returns the number of messages accepted.
	Enqueue(ctx context.Context, msgs []blobcache.Message) (int, error)
	// Dequeue removes messages from the Queue.
	Dequeue(ctx context.Context, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error)
	// QueueDown is called when the queue has no remaining handles
	QueueDown(ctx context.Context) error
	// Config returns the QueueConfig for this queue.
	Config() blobcache.QueueConfig
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

	SubToVol(ctx context.Context, vol V, q Queue, spec blobcache.VolSubSpec) error
}

type LinkSet = map[[32]byte]blobcache.OID

type Volume interface {
	// GetParams returns the effective parameters of the volume.
	GetParams() blobcache.VolumeConfig
	// GetBackend returns the backend of the volume.
	GetBackend() blobcache.VolumeBackend[blobcache.OID]

	BeginTx(ctx context.Context, spec blobcache.TxParams) (Tx, error)

	// VolumeDown is called when the Volume has no remaining handles.
	VolumeDown(ctx context.Context) error

	// AccessSubVolume returns the rights granted to access a subvolume.
	// Returns 0 if there is no link to the target.
	AccessSubVolume(ctx context.Context, target blobcache.LinkToken) (blobcache.ActionSet, error)
}

// Tx is a consistent view of a volume, during a transaction.
type Tx interface {
	Params() blobcache.TxParams
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
	Link(ctx context.Context, svoid blobcache.OID, rights blobcache.ActionSet, subvol Volume) (*blobcache.LinkToken, error)
	// Unlink removes a link from the volume.
	Unlink(ctx context.Context, targets []blobcache.LinkToken) error
	// VisitLinks visits a link to another volume.
	// This is only usable in a GC transaction.
	VisitLinks(ctx context.Context, targets []blobcache.LinkToken) error
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
