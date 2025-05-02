package cadutil

import (
	"context"
	"fmt"
	"sync"

	"go.brendoncarroll.net/state/cadata"
)

type lockStatus struct {
	isDelete bool
	count    uint32
	ch       chan struct{}
}

type Locker struct {
	mu sync.Mutex
	m  map[cadata.ID]*lockStatus
}

func NewLocker() *Locker {
	return &Locker{
		m: make(map[cadata.ID]*lockStatus),
	}
}

func (l *Locker) LockAdd(ctx context.Context, id cadata.ID) (func(), error) {
	if err := l.lock(ctx, id, false); err != nil {
		return nil, err
	}
	once := sync.Once{}
	uf := func() { once.Do(func() { l.unlock(id, false) }) }
	return uf, nil
}

func (l *Locker) LockDelete(ctx context.Context, id cadata.ID) (func(), error) {
	if err := l.lock(ctx, id, true); err != nil {
		return nil, err
	}
	once := sync.Once{}
	uf := func() { once.Do(func() { l.unlock(id, true) }) }
	return uf, nil
}

func (bl *Locker) lock(ctx context.Context, id cadata.ID, isDelete bool) error {
	for {
		bl.mu.Lock()
		status, exists := bl.m[id]
		switch {
		// no lock, create one and return
		case !exists:
			status = &lockStatus{
				isDelete: isDelete,
				ch:       make(chan struct{}),
			}
			bl.m[id] = status
			bl.mu.Unlock()
			return nil

		// lock is in the mode we want, increment count and return
		case status.isDelete == isDelete:
			status.count++
			bl.mu.Unlock()
			return nil

		// lock is in different mode, release bl.mu and wait, then try again.
		default:
			bl.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-status.ch:
				// keep looping
			}
		}
	}
}

func (bl *Locker) unlock(id cadata.ID, isDelete bool) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	status, exists := bl.m[id]
	if !exists {
		panic("unlock called on unlocked id")
	}
	if status.isDelete != isDelete {
		panic(fmt.Sprintf("unlock called on lock in different mode lock: %v, caller: %v", status.isDelete, isDelete))
	}
	status.count--
	if status.count == 0 {
		close(status.ch)
		delete(bl.m, id)
	}
}
