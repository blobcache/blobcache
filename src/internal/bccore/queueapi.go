package bccore

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var _ blobcache.QueueAPI = &System{}

// InspectQueue implements blobcache.QueueAPI.InspectQueue
func (sys *System) InspectQueue(ctx context.Context, qh blobcache.Handle) (blobcache.QueueInfo, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "InspectQueue"), zap.Stringer("oid", qh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "InspectQueue"), zap.Stringer("oid", qh.OID))
	q, _, err := sys.resolveQueue(qh, blobcache.Action_QUEUE_INSPECT)
	if err != nil {
		return blobcache.QueueInfo{}, err
	}
	return q.info, nil
}

// Dequeue implements blobcache.QueueAPI.Dequeue
func (sys *System) Dequeue(ctx context.Context, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Dequeue"), zap.Stringer("oid", qh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Dequeue"), zap.Stringer("oid", qh.OID))
	if err := opts.Validate(); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, fmt.Errorf("dequeue buffer must be non-empty")
	}
	q, _, err := sys.resolveQueue(qh, blobcache.Action_QUEUE_DEQUEUE)
	if err != nil {
		return 0, err
	}
	return q.backend.Dequeue(ctx, buf, opts)
}

// Enqueue implements blobcache.QueueAPI.Enqueue
func (sys *System) Enqueue(ctx context.Context, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Enqueue"), zap.Stringer("oid", qh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Enqueue"), zap.Stringer("oid", qh.OID))
	q, _, err := sys.resolveQueue(qh, blobcache.Action_QUEUE_ENQUEUE)
	if err != nil {
		return nil, err
	}
	maxBytes := q.info.Config.MaxBytesPerMessage
	maxHandles := q.info.Config.MaxHandlesPerMessage
	for i, msg := range msgs {
		if uint32(len(msg.Bytes)) > maxBytes {
			return nil, fmt.Errorf("message %d exceeds max bytes per message: %d", i, maxBytes)
		}
		if uint32(len(msg.Handles)) > maxHandles {
			return nil, fmt.Errorf("message %d exceeds max handles per message: %d", i, maxHandles)
		}
	}
	n, err := q.backend.Enqueue(ctx, msgs)
	if err != nil {
		return nil, err
	}
	return &blobcache.InsertResp{Success: uint32(n)}, nil
}

var _ blobcache.QueueAPI = &System{}
