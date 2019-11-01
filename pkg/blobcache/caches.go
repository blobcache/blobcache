package blobcache

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	lru "github.com/hashicorp/golang-lru"
)

type MemCache struct {
	c interface {
		Get(key interface{}) (interface{}, bool)
		Contains(interface{}) bool
	}
}

func NewMemLRU(count int) *MemCache {
	c, err := lru.New(count)
	if err != nil {
		panic(err)
	}
	return &MemCache{c: c}
}

func NewMemARC(count int) *MemCache {
	c, err := lru.NewARC(count)
	if err != nil {
		panic(err)
	}
	return &MemCache{c: c}
}

func (c *MemCache) Post(ctx context.Context, data []byte) (blobs.ID, error) {
	id := blobs.Hash(data)
	switch c2 := c.c.(type) {
	case interface{ Add(key, value interface{}) }:
		c2.Add(id, data)
	case interface {
		Add(key, value interface{}) bool
	}:
		c2.Add(id, data)
	}
	return id, nil
}

func (c *MemCache) Get(ctx context.Context, id blobs.ID) ([]byte, error) {
	x, ok := c.c.Get(id)
	if !ok {
		return nil, nil
	}
	return x.([]byte), nil
}

func (c *MemCache) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	return c.c.Contains(id), nil
}
