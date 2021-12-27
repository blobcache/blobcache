package bcstate

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/blobcache/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-state/cadata"
)

var _ KV = &TrieKV{}

type TrieKV struct {
	mu    sync.RWMutex
	store cadata.Store
	cell  Cell
}

func NewTrieKV(store cadata.Store, cell Cell) *TrieKV {
	return &TrieKV{
		cell:  cell,
		store: store,
	}
}

func (kv *TrieKV) GetF(k []byte, f func([]byte) error) error {
	return kv.withRead(func(ctx context.Context, root tries.Ref) error {
		value, err := tries.Get(ctx, kv.store, root, k)
		if err != nil {
			if err == tries.ErrNotExist {
				return ErrNotExist
			}
			return err
		}
		return f(value)
	})
}

func (kv *TrieKV) Put(k, v []byte) error {
	return kv.withWrite(func(ctx context.Context, root tries.Ref) (*tries.Ref, error) {
		return tries.Put(ctx, kv.store, root, k, v)
	})
}

func (kv *TrieKV) Delete(k []byte) error {
	return kv.withWrite(func(ctx context.Context, root tries.Ref) (*tries.Ref, error) {
		return tries.Delete(ctx, kv.store, root, k)
	})
}

func (TrieKV) NextSequence() (uint64, error) {
	panic("not implemented")
}

// ForEach calls fn with first <= k < last
// if last == nil ForEach will call fn with the last key
func (kv *TrieKV) ForEach(first, last []byte, fn func(k, v []byte) error) error {
	return kv.withRead(func(ctx context.Context, root tries.Ref) error {
		return tries.ForEach(ctx, kv.store, root, first, last, fn)
	})
}

func (TrieKV) Count() uint64 {
	panic("not implemented")
}

func (TrieKV) MaxCount() uint64 {
	panic("not implemented")
}

func (kv *TrieKV) withRead(fn func(ctx context.Context, ref tries.Ref) error) error {
	ctx := context.Background()
	ctx, cf := context.WithTimeout(ctx, 10*time.Second)
	defer cf()
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var root *tries.Ref
	if err := kv.cell.LoadF(func(data []byte) error {
		if len(data) == 0 {
			return ErrNotExist
		}
		root = &tries.Ref{}
		return json.Unmarshal(data, &root)
	}); err != nil {
		return err
	}
	return fn(ctx, *root)
}

func (kv *TrieKV) withWrite(fn func(ctx context.Context, root tries.Ref) (*tries.Ref, error)) error {
	ctx := context.Background()
	ctx, cf := context.WithTimeout(ctx, 10*time.Second)
	defer cf()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var root *tries.Ref
	if err := kv.cell.LoadF(func(data []byte) error {
		if len(data) == 0 {
			return ErrNotExist
		}
		root = &tries.Ref{}
		return json.Unmarshal(data, &root)
	}); err != nil {
		return err
	}
	root2, err := fn(ctx, *root)
	if err != nil {
		return err
	}
	data, err := json.Marshal(*root2)
	if err != nil {
		return err
	}
	return kv.cell.Save(data)
}
