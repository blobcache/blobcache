package remotebe

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/internal/bcp"
)

var _ backend.Queue = (*Queue)(nil)

type QueueParams = blobcache.QueueBackend_Remote

// Queue is a remote queue.
type Queue struct {
	sys *System
	n   bcp.Asker
	ep  blobcache.Endpoint
	h   blobcache.Handle
	cfg blobcache.QueueConfig
}

func NewQueue(sys *System, node bcp.Asker, ep blobcache.Endpoint, h blobcache.Handle, cfg blobcache.QueueConfig) *Queue {
	return &Queue{
		sys: sys,
		n:   node,
		ep:  ep,
		h:   h,
		cfg: cfg,
	}
}

func (q *Queue) Config() blobcache.QueueConfig {
	return q.cfg
}

func (q *Queue) Enqueue(ctx context.Context, msgs []blobcache.Message) (int, error) {
	resp, err := bcp.Enqueue(ctx, q.n, q.ep, q.h, msgs)
	if err != nil {
		return 0, err
	}
	return int(resp.Success), nil
}

func (q *Queue) Dequeue(ctx context.Context, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	return bcp.Dequeue(ctx, q.n, q.ep, q.h, buf, opts)
}

func (q *Queue) QueueDown(ctx context.Context) error {
	return nil
}
