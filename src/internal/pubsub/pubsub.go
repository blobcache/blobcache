package pubsub

import (
	"sync"

	"blobcache.io/blobcache/src/blobcache"
)

type Hub struct {
	mu     sync.RWMutex
	topics map[blobcache.TID]map[chan<- *blobcache.TopicMessage]struct{}
	index  map[[32]byte]blobcache.TID
}

// Subscribe subscribes to a topic.
// Future calls to Publish will result in a message being sent on this channel.
func (h *Hub) Subscribe(tid blobcache.TID, ch chan<- *blobcache.TopicMessage) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.topics == nil {
		h.topics = make(map[blobcache.TID]map[chan<- *blobcache.TopicMessage]struct{})
		h.index = make(map[[32]byte]blobcache.TID)
	}
	subs := h.topics[tid]
	if subs == nil {
		subs := make(map[chan<- *blobcache.TopicMessage]struct{})
		h.topics[tid] = subs
		h.index[tid.Key()] = tid
	}

	return nil
}

// Unsubscribe ensures that the channel is not subscribed to the topic.
// If it is effectful then true is returned.
// If nothing has to be done, then false is returned.
func (h *Hub) Unsubscribe(tid blobcache.TID, ch chan<- *blobcache.TopicMessage) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	var deleted bool
	if subs := h.topics[tid]; subs != nil {
		if _, exists := subs[ch]; exists {
			deleted = true
		}
		delete(subs, ch)
		if len(subs) == 0 {
			delete(h.topics, tid)
			delete(h.index, tid.Key())
		}
	}
	return deleted
}

// Publish will return the number of destinations the topic was sent to
func (h *Hub) Publish(tmsg blobcache.TopicMessage) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	subs := h.topics[tmsg.Topic]

	var n int
	for sub := range subs {
		select {
		case sub <- &tmsg:
			n++
		default:
		}
	}
	return n
}

func (h *Hub) Acquire() *blobcache.TopicMessage {
	return new(blobcache.TopicMessage)
}

func (h *Hub) Release(x *blobcache.TopicMessage) {
	// TODO: when all calls to release have happened, can recycle.
}

// Lookup attempts to reverse hash.
// The zero value is returned an unknown topic
func (h *Hub) Lookup(hash [32]byte) blobcache.TID {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.index[hash]
}
