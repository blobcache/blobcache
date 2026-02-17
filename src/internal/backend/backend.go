package backend

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type System[QS any] interface {
	CreateQueue(ctx context.Context, spec QS) (Queue, error)
}

type Queue interface {
	Enqueue(ctx context.Context, msgs []blobcache.Message) error
	Dequeue(ctx context.Context, buf []blobcache.Message) (int, error)
}
