package schema

import (
	"context"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
)

// Initializer is an interface for Schemas which support initialization.
type Initializer interface {
	Init(ctx context.Context, s bcsdk.WO) ([]byte, error)
}

type VisitAll interface {
	VisitAll(ctx context.Context, s bcsdk.RO, root []byte, visit func(cids []blobcache.CID, links []blobcache.OID) error) error
}

// KV is an interface for Schemas which support common Key-Value operations.
type KV[K, V any] interface {
	Put(ctx context.Context, s bcsdk.RW, root []byte, key K, value V) ([]byte, error)
	Get(ctx context.Context, s bcsdk.RO, root []byte, key K, dst *V) (bool, error)
	Delete(ctx context.Context, s bcsdk.RW, root []byte, key K) ([]byte, error)
}
