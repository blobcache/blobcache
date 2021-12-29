package calock

import (
	"context"
	"fmt"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
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

func (l *Locker) LockAdd(ctx context.Context, id cadata.ID) error {
	return l.lock(ctx, id, false)
}

func (l *Locker) LockDelete(ctx context.Context, id cadata.ID) error {
	return l.lock(ctx, id, true)
}

func (l *Locker) UnlockAdd(id cadata.ID) {
	l.unlock(id, false)
}

func (l *Locker) UnlockDelete(ctx context.Context, id cadata.ID) {
	l.unlock(id, true)
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

		// lock into the mode we want, increment count and return
		case status.isDelete == isDelete:
			status.count++
			bl.mu.Unlock()
			return nil

		// lock into different mode, release bl.mu and wait, then try again.
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
