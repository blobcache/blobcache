package bccore

import (
	"context"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

// SubToVolume subscribes vol to q.
// This allows aribtrary volumes to be subscribed to arbitrary queues, but
// this is not the correct thing for a Node to do in the general case,
// Nodes should forward subscriptions to remote Volumes and Queues when both
// are on the same Node.
func (sys *System) SubToVolume(ctx context.Context, qh blobcache.Handle, volh blobcache.Handle) error {
	_, rights, err := sys.resolveVol(volh)
	if err != nil {
		return err
	}
	q, rights, err := sys.resolveQueue(qh, blobcache.Action_QUEUE_SUB_VOLUME)
	if err != nil {
		return err
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	sub := sys.hub.Subscribe(volh.OID, func(ctx context.Context, k blobcache.OID, v *volume) {
		// TODO: use rights to tailor message
		_ = rights
		msg := blobcache.Message{}
		_, err := q.backend.Enqueue(ctx, []blobcache.Message{msg})
		if err != nil {
			logctx.Warn(ctx, "during subscription callback", zap.Error(err))
		}
	})
	// add the sub to the set of subs on the queue
	q.subs[sub] = struct{}{}
	// when q goes down, also need to unsubscribe the subs
	return nil
}

type hub struct {
	// mu guards pubs
	mu sync.RWMutex
	// pubs is the set of publishers
	pubs    map[blobcache.OID]map[*sub]struct{}
	lastPub uint64
}

type sub struct {
	k  blobcache.OID
	fn func(ctx context.Context, k blobcache.OID, v *volume)
}

func (h *hub) Publish(ctx context.Context, k blobcache.OID, v *volume) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for sub := range h.pubs[k] {
		sub.fn(ctx, k, v)
	}
}

func (h *hub) Subscribe(k blobcache.OID, fn func(ctx context.Context, k blobcache.OID, v *volume)) *sub {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pubs == nil {
		h.pubs = make(map[blobcache.OID]map[*sub]struct{})
	}
	if h.pubs[k] == nil {
		h.pubs[k] = make(map[*sub]struct{})
	}
	s := &sub{
		k:  k,
		fn: fn,
	}
	h.pubs[k][s] = struct{}{}
	return s
}

func (h *hub) Unsubscribe(k blobcache.OID, s *sub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pubs[s.k] != nil {
		delete(h.pubs[k], s)
		if len(h.pubs[k]) == 0 {
			delete(h.pubs, k)
		}
	}
}
