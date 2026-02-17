package memory

import (
	"context"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
)

const MaxMaxDepth = 16

var _ backend.System[blobcache.QueueBackend_Memory] = &System{}

type System struct {
}

func (sys *System) CreateQueue(ctx context.Context, spec blobcache.QueueBackend_Memory) (backend.Queue, error) {
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

type Queue struct {
	mu          sync.Mutex
	cond        *sync.Cond
	msgs        []blobcache.Message
	maxDepth    uint32
	evictOldest bool
}

func (q *Queue) Enqueue(ctx context.Context, msgs []blobcache.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.evictOldest {
		for _, msg := range msgs {
			for q.maxDepth > 0 && uint32(len(q.msgs)) >= q.maxDepth {
				q.msgs = q.msgs[1:]
			}
			q.msgs = append(q.msgs, msg)
		}
		q.cond.Broadcast()
		return nil
	}

	space := int(q.maxDepth) - len(q.msgs)
	if space <= 0 {
		return nil
	}
	if space < len(msgs) {
		msgs = msgs[:space]
	}
	q.msgs = append(q.msgs, msgs...)
	q.cond.Broadcast()
	return nil
}

func (q *Queue) Dequeue(ctx context.Context, buf []blobcache.Message) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	q.mu.Lock()
	if len(q.msgs) == 0 {
		if err := ctx.Err(); err != nil {
			q.mu.Unlock()
			return 0, err
		}
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				q.mu.Lock()
				q.cond.Broadcast()
				q.mu.Unlock()
			case <-done:
			}
		}()
		for len(q.msgs) == 0 {
			if err := ctx.Err(); err != nil {
				close(done)
				q.mu.Unlock()
				return 0, err
			}
			q.cond.Wait()
		}
		close(done)
	}
	n := copy(buf, q.msgs)
	q.msgs = q.msgs[n:]
	q.mu.Unlock()
	return n, nil
}
