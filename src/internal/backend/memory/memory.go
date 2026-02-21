package memory

import (
	"context"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
)

type System struct {
}

func (sys *System) CreateQueue(ctx context.Context, spec blobcache.QueueBackend_Memory) (*Queue, error) {
	if spec.MaxDepth == 0 {
		return nil, fmt.Errorf("max depth must be positive")
	}
	if spec.MaxDepth > MaxMaxDepth {
		return nil, fmt.Errorf("max depth exceeds limit: %d", MaxMaxDepth)
	}
	q := &Queue{
		maxDepth:    spec.MaxDepth,
		evictOldest: spec.EvictOldest,
	}
	q.cond = sync.NewCond(&q.mu)
	return q, nil
}

func (sys *System) QueueDown(ctx context.Context, q *Queue) error {
	// nothing to do, it's all in memory and will be GC'd
	return nil
}
