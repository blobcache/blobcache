package bccore

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type Volume interface {
	AnyObject

	// GetParams returns the effective parameters of the volume.
	GetParams() blobcache.VolumeConfig
	// GetBackend returns the backend of the volume.
	GetBackend() blobcache.VolumeBackend[blobcache.OID]

	BeginTx(ctx context.Context, spec blobcache.TxParams) (Tx, error)

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
	Exists(ctx context.Context, cids []blobcache.CID, dst *blobcache.BitMap) error

	IsVisited(ctx context.Context, cids []blobcache.CID, dst *blobcache.BitMap) error
	Visit(ctx context.Context, cids []blobcache.CID) error

	MaxSize() int
	HashAlgo() blobcache.HashAlgo

	// Link creates adds a handle to prove access to a volume.
	Link(ctx context.Context, svoid blobcache.OID, rights blobcache.ActionSet, subvol AnyObject) (*blobcache.LinkToken, error)
	// Unlink removes a link from the volume.
	Unlink(ctx context.Context, targets []blobcache.LinkID) error
	// VisitLinks visits a link to another volume.
	// This is only usable in a GC transaction.
	VisitLinks(ctx context.Context, targets []blobcache.LinkID) error
}

type Queue interface {
	AnyObject

	// Enqueue adds messages to the Queue.
	// It returns the number of messages accepted.
	Enqueue(ctx context.Context, msgs []blobcache.Message) (int, error)
	// Dequeue removes messages from the Queue.
	Dequeue(ctx context.Context, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error)
	// Down is called when the queue has no remaining handles
	Down(ctx context.Context) error
	// Config returns the QueueConfig for this queue.
	Config() blobcache.QueueConfig
	// Backend returns the QueueSpec for this queue.
	Backend() blobcache.QueueBackend[blobcache.OID]
}
