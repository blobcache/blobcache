package memory

import (
	"context"
	"errors"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
)

const (
	MaxMaxDepth      = 16
	MaxMaxHandlesPer = 16
	MaxMaxBytesPer   = 1 << 20
)

var _ backend.QueueSystem[blobcache.QueueBackend_Memory, *Queue] = &System{}

type Queue struct {
	mu          sync.Mutex
	cond        *sync.Cond
	msgs        []blobcache.Message
	maxDepth    uint32
	evictOldest bool
}

func (q *Queue) QueueDown(ctx context.Context) error {
	return nil
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

func (q *Queue) Dequeue(ctx context.Context, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := opts.Validate(); err != nil {
		return 0, err
	}

	waitCtx := ctx
	cancel := func() {}
	if opts.Min > 0 && opts.MaxWait != nil {
		waitCtx, cancel = context.WithTimeout(ctx, *opts.MaxWait)
	}
	defer cancel()

	q.mu.Lock()
	defer q.mu.Unlock()

	waitFor := func(min int) error {
		if len(q.msgs) >= min {
			return nil
		}
		if err := waitCtx.Err(); err != nil {
			return err
		}
		done := make(chan struct{})
		go func() {
			select {
			case <-waitCtx.Done():
				q.mu.Lock()
				q.cond.Broadcast()
				q.mu.Unlock()
			case <-done:
			}
		}()
		for len(q.msgs) < min {
			if err := waitCtx.Err(); err != nil {
				close(done)
				return err
			}
			q.cond.Wait()
		}
		close(done)
		return nil
	}

	remainingSkip := int(opts.Skip)
	for remainingSkip > 0 {
		if len(q.msgs) == 0 {
			if opts.Min == 0 {
				return 0, nil
			}
			if err := waitFor(1); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return 0, nil
				}
				if ctx.Err() != nil {
					return 0, ctx.Err()
				}
				return 0, err
			}
		}
		if len(q.msgs) == 0 {
			return 0, nil
		}
		drop := remainingSkip
		if drop > len(q.msgs) {
			drop = len(q.msgs)
		}
		q.msgs = q.msgs[drop:]
		remainingSkip -= drop
	}

	if len(buf) == 0 {
		return 0, nil
	}

	minNeeded := 0
	if opts.Min > 0 {
		minNeeded = int(opts.Min)
		if minNeeded > len(buf) {
			minNeeded = len(buf)
		}
	}
	if minNeeded > 0 {
		if err := waitFor(minNeeded); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, err
		}
	}

	n := copy(buf, q.msgs)
	if !opts.LeaveIn {
		q.msgs = q.msgs[n:]
	}
	return n, nil
}
