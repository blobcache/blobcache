package blobcache

import (
	"context"
	"errors"
)

var ErrCacheFull = errors.New("cache is full")

type Cache interface {
	Put(ctx context.Context, key, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)

	SizeUsed() uint64
	SizeTotal() uint64
}
