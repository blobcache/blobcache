package control

import (
	"context"
	"time"

	"github.com/brendoncarroll/go-state/cadata"
)

type ReadOnlyStore interface {
	cadata.Getter
	cadata.Lister
}

// ReadOnlySet is a read-only cadata.Set
type ReadOnlySet interface {
	cadata.Lister
	cadata.Exister
}

// Source provides blobs to be maintained by the controller
type Source struct {
	Set              ReadOnlySet
	ExpectedReplicas float64
}

type Cost struct {
	// Post is the cost to Post a blob
	Post float64
	// Get is the cost to retreive a blob
	Get float64
	// Delete is the cost to delete a blob
	Delete float64
	// List is the cost to list n blobs
	List func(n int) float64
	// BlobSecond is the cost to store 1 blob for 1 second
	BlobSecond float64
}

// Sinks are places where blobs can be stored.
type Sink struct {
	Locus    [32]byte
	MaxCount int64
	Cost     Cost

	// BatchSize is the size of batches passed to flush.
	// If BatchSize == 0. Then there is no batching for the Sink.
	// If BatchSize < 0.  Then the magnitude is interpretted as the maximum number of concurrent operations.
	BatchSize int
	// BatchDelay is the longest an operation should be queued in a batch before pre-emptively
	// flushing a batch below BatchSize.
	BatchDelay time.Duration
	Target     Target
}

// Op is an operation to be done to a sink
type Op struct {
	ID  cadata.ID
	Src ReadOnlyStore
}

func (op *Op) IsDelete() bool {
	return op.Src == nil
}

type Target interface {
	// Actual returns a read only store with the actual content of the sink.
	Actual() ReadOnlyStore
	// FlushFunc is called to apply a batch of point operations to the target.
	Flush(ctx context.Context, ops []Op) error
	// Refresh is called to resync a target.
	Refresh(ctx context.Context, src ReadOnlyStore, desired ReadOnlySet) error
}

type BasicTarget struct {
	Store cadata.Store
}

func (t BasicTarget) Actual() ReadOnlyStore {
	return t.Store
}

func (t BasicTarget) Flush(ctx context.Context, ops []Op) error {
	dst := t.Store
	for _, op := range ops {
		if op.IsDelete() {
			if err := dst.Delete(ctx, op.ID); err != nil {
				return err
			}
		} else {
			if err := cadata.Copy(ctx, dst, op.Src, op.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t BasicTarget) Refresh(ctx context.Context, src ReadOnlyStore, desired ReadOnlySet) error {
	dst := t.Store
	return cadata.ForEach(ctx, dst, func(id cadata.ID) error {
		exists, err := desired.Exists(ctx, id)
		if err != nil {
			return err
		}
		if !exists {
			return dst.Delete(ctx, id)
		}
		return nil
	})
}
