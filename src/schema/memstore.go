package schema

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
)

func NewTestStore(t testing.TB) *MemStore {
	return NewMem(blobcache.HashAlgo_BLAKE3_256.HashFunc(), 1<<21)
}

// MemStore is a simple in-memory store, useful for testing.
// It has the same store methods as a transaction on an unsalted volume.
type MemStore struct {
	hf      blobcache.HashFunc
	maxSize int
	mu      sync.RWMutex
	blobs   map[blobcache.CID][]byte
}

func NewMem(hf blobcache.HashFunc, maxSize int) *MemStore {
	return &MemStore{
		hf:      hf,
		maxSize: maxSize,
		blobs:   make(map[blobcache.CID][]byte),
	}
}

func (ms *MemStore) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	if len(data) > ms.maxSize {
		return blobcache.CID{}, blobcache.ErrTooLarge{}
	}
	cid := ms.Hash(data)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, exists := ms.blobs[cid]; !exists {
		ms.blobs[cid] = slices.Clone(data)
	}

	return cid, nil
}

func (ms *MemStore) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	data, exists := ms.blobs[cid]
	if !exists {
		return 0, blobcache.ErrNotFound{Key: cid}
	}
	return copy(buf, data), nil
}

func (ms *MemStore) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for i, cid := range cids {
		_, exists := ms.blobs[cid]
		dst[i] = exists
	}
	return nil
}

func (ms *MemStore) Delete(ctx context.Context, cids []blobcache.CID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, cid := range cids {
		delete(ms.blobs, cid)
	}
	return nil
}

func (ms *MemStore) Hash(data []byte) blobcache.CID {
	return ms.hf(nil, data)
}

func (ms *MemStore) MaxSize() int {
	return ms.maxSize
}

func (ms *MemStore) Len() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.blobs)
}
