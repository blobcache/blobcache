package bclocal

import (
	"context"
	"sync"
)

type mapOfLocks[K comparable] struct {
	mu sync.RWMutex
	m  map[K]chan struct{}
}

func (mol *mapOfLocks[K]) Lock(ctx context.Context, k K) error {
	for {
		mol.mu.Lock()
		ch, exists := mol.m[k]
		if !exists {
			ch = make(chan struct{})
			if mol.m == nil {
				mol.m = make(map[K]chan struct{})
			}
			mol.m[k] = ch
		}
		mol.mu.Unlock()
		if !exists {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			return nil
		}
	}
}

func (mol *mapOfLocks[K]) Unlock(oid K) {
	mol.mu.Lock()
	ch := mol.m[oid]
	if mol.m != nil {
		delete(mol.m, oid)
	}
	mol.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}
