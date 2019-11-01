package blobs

import (
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

func (s *MemStore) Delete(ctx context.Context, id ID) error {
	s.m.Delete(id)
	return nil
}

func (s *MemStore) Exists(ctx context.Context, id ID) (bool, error) {
	data, _ := s.Get(ctx, id)
	return data == nil, nil
}
