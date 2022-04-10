package blobcache

import (
	"context"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/cadutil"
	"github.com/brendoncarroll/go-state/cadata"
)

type storeMux struct {
	db    bcdb.DB
	store cadata.Store

	sm     *setManager
	locker *cadutil.Locker
}

func newStoreMux(db bcdb.DB, store cadata.Store) *storeMux {
	return &storeMux{
		db:     db,
		store:  store,
		sm:     newSetManager(db),
		locker: cadutil.NewLocker(),
	}
}

func (sm *storeMux) open(set cadata.Set) cadata.Store {
	return &virtualStore{
		sm:     sm.sm,
		set:    set,
		store:  sm.store,
		locker: sm.locker,
	}
}

type virtualStore struct {
	sm     *setManager
	set    cadata.Set
	store  cadata.Store
	locker *cadutil.Locker
}

func (vs *virtualStore) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	if uf, err := vs.locker.LockAdd(ctx, id); err != nil {
		return 0, err
	} else {
		defer uf()
	}
	if exists, err := vs.set.Exists(ctx, id); err != nil {
		return 0, err
	} else if !exists {
		return 0, cadata.ErrNotFound
	}
	return vs.store.Get(ctx, id, buf)
}

func (vs *virtualStore) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	id := vs.Hash(data)
	if uf, err := vs.locker.LockAdd(ctx, id); err != nil {
		return cadata.ID{}, err
	} else {
		defer uf()
	}
	if err := vs.set.Add(ctx, id); err != nil {
		return cadata.ID{}, nil
	}
	return vs.store.Post(ctx, data)
}

func (vs *virtualStore) Delete(ctx context.Context, id cadata.ID) error {
	if uf, err := vs.locker.LockDelete(ctx, id); err != nil {
		return err
	} else {
		defer uf()
	}
	if err := vs.set.Delete(ctx, id); err != nil {
		return err
	}
	count, err := vs.sm.getRefCount(ctx, id)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	return vs.store.Delete(ctx, id)
}

func (vs *virtualStore) List(ctx context.Context, first cadata.ID, ids []cadata.ID) (int, error) {
	return vs.set.List(ctx, first, ids)
}

func (vs *virtualStore) Hash(x []byte) cadata.ID {
	return vs.store.Hash(x)
}

func (vs *virtualStore) MaxSize() int {
	return vs.store.MaxSize()
}
