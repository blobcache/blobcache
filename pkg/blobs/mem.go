package blobs

import (
	"bytes"
	"context"
	"sync"
)

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

func (s *MemStore) Get(ctx context.Context, id ID) ([]byte, error) {
	data, exists := s.m.Load(id)
	if !exists {
		return nil, nil
	}
	return data.([]byte), nil
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
	data, _ := s.Get(ctx, id)
	return data == nil, nil
}

func (s *MemStore) Len() (count int) {
	s.m.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}

type Void struct{}

func (s Void) Post(ctx context.Context, data []byte) (ID, error) {
	return Hash(data), nil
}

func (s Void) Get(ctx context.Context, id ID) ([]byte, error) {
	return nil, ErrNotFound
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
