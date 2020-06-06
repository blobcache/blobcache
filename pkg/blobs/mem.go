package blobs

import (
	"bytes"
	"context"
	"sync"
)

var _ GetPostDelete = &MemStore{}

type MemStore struct {
	m sync.Map
}

func NewMem() *MemStore {
	return &MemStore{}
}

func (s *MemStore) Post(ctx context.Context, data []byte) (ID, error) {
	data2 := make([]byte, len(data))
	copy(data2, data)

	id := Hash(data)
	s.m.Store(id, data2)
	return id, nil
}

func (s *MemStore) GetF(ctx context.Context, id ID, f func(Blob) error) error {
	data, exists := s.m.Load(id)
	if !exists {
		return ErrNotFound
	}
	return f(data.([]byte))
}

func (s *MemStore) List(ctx context.Context, prefix []byte, ids []ID) (n int, err error) {
	s.m.Range(func(k, v interface{}) bool {
		if n >= len(ids) {
			err = ErrTooMany
			return false
		}
		id := k.(ID)
		if !bytes.HasPrefix(id[:], prefix) {
			return true
		}
		ids[n] = id
		n++
		return true
	})
	return n, err
}

func (s *MemStore) Delete(ctx context.Context, id ID) error {
	s.m.Delete(id)
	return nil
}

func (s *MemStore) Exists(ctx context.Context, id ID) (bool, error) {
	_, ok := s.m.Load(id)
	return ok, nil
}

func (s *MemStore) Len() (count int) {
	s.m.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}

var _ GetPostDelete = Void{}

type Void struct{}

func (s Void) Post(ctx context.Context, data []byte) (ID, error) {
	return Hash(data), nil
}

func (s Void) GetF(ctx context.Context, id ID, f func(data []byte) error) error {
	return ErrNotFound
}

func (s Void) Exists(ctx context.Context, id ID) (bool, error) {
	return false, nil
}

func (s Void) List(ctx context.Context, prefix []byte, ids []ID) (int, error) {
	return 0, nil
}

func (s Void) Delete(ctx context.Context, id ID) error {
	return nil
}
