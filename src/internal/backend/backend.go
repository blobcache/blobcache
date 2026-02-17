package backend

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type System[QS any] interface {
	// CreateQueue creates a new queue from the spec and returns it.
	CreateQueue(ctx context.Context, spec QS) (Queue, error)
	// QueueDown is called when the queue has no remaining handles
	QueueDown(ctx context.Context, q Queue) error
}

type Queue interface {
	Enqueue(ctx context.Context, msgs []blobcache.Message) error
	Dequeue(ctx context.Context, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error)
}
