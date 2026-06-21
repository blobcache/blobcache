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
	VisitAll(ctx context.Context, s bcsdk.RO, root []byte, visit func(cids []blobcache.CID, links []blobcache.LinkToken) error) error
}

type Syncer interface {
	// Sync ensures that all data transitively reachable from rootData
	// has been copied from rs to ws.
	Sync(ctx context.Context, rs bcsdk.RO, ws bcsdk.WO, rootData []byte) error
}

// KV is an interface for Schemas which support common Key-Value operations.
type KV[K, V any] interface {
	Put(ctx context.Context, s bcsdk.RW, root []byte, key K, value V) ([]byte, error)
	Get(ctx context.Context, s bcsdk.RO, root []byte, key K, dst *V) (bool, error)
	Delete(ctx context.Context, s bcsdk.RW, root []byte, key K) ([]byte, error)
}

// GC garbage collects the volume
func GC(ctx context.Context, svc blobcache.Service, sch VisitAll, volh blobcache.Handle) error {
	tx, err := bcsdk.BeginTx(ctx, svc, volh, blobcache.TxParams{Modify: true, GCBlobs: true, GCLinks: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	visit := func(cids []blobcache.CID, ltoks []blobcache.LinkToken) error {
		if len(cids) > 0 {
			if err := tx.Visit(ctx, cids); err != nil {
				return err
			}
		}
		if len(ltoks) > 0 {
			linkIDs := make([]blobcache.LinkID, len(ltoks))
			for i := range ltoks {
				linkIDs[i] = ltoks[i].GetID(tx.HashAlgo())
			}
			if err := tx.VisitLinks(ctx, linkIDs); err != nil {
				return err
			}
		}
		return nil
	}
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return err
	}
	if err := sch.VisitAll(ctx, tx, root, visit); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
