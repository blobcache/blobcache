package trieevents

import (
	"bytes"
	"sync"

	"github.com/blobcache/blobcache/pkg/blobs"
)

type EventBus struct {
	prefix []byte

	children sync.Map
	subs     sync.Map
}

func New() *EventBus {
	return &EventBus{}
}

func (eb *EventBus) Subscribe(prefix []byte, ch chan blobs.ID) {
	switch {
	case !bytes.HasPrefix(prefix, eb.prefix):
		panic("can't insert into this event bus")
	case len(prefix) == len(eb.prefix):
		eb.subs.Store(ch, nil)
	default:
		child := &EventBus{prefix: prefix[:len(eb.prefix)+1]}
		actual, _ := eb.children.LoadOrStore(string(prefix), child)
		child = actual.(*EventBus)
		child.Subscribe(prefix, ch)
	}
}

func (eb *EventBus) Unsubscribe(ch chan blobs.ID) {
	eb.subs.Delete(ch)
	eb.children.Range(func(k, v interface{}) bool {
		v.(*EventBus).Unsubscribe(ch)
		return true
	})
}

func (eb *EventBus) Publish(id blobs.ID) {
	if !bytes.HasPrefix(id[:], eb.prefix) {
		panic("can't notify on this event bus")
	}
	eb.subs.Range(func(k, v interface{}) bool {
		ch := k.(chan blobs.ID)
		ch <- id
		return true
	})
	eb.children.Range(func(k, v interface{}) bool {
		if bytes.HasPrefix(id[:], []byte(k.(string))) {
			v.(*EventBus).Publish(id)
		}
		return true
	})
}
