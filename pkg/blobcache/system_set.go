package blobcache

import (
	"context"
	"sync/atomic"

	"github.com/blobcache/blobcache/pkg/dirserv"
	"github.com/brendoncarroll/go-state/cadata"
)

type systemSet struct {
	ds  *dirserv.DirServer
	sm  *setManager
	p   []string
	oid dirserv.OID
}

func newSystemSet(ds *dirserv.DirServer, sm *setManager, p []string) *systemSet {
	return &systemSet{
		ds: ds,
		sm: sm,
		p:  append([]string{systemDirName}, p...),
	}
}

func (ss *systemSet) getOID(ctx context.Context) (dirserv.OID, error) {
	oid := dirserv.OID(atomic.LoadUint64((*uint64)(&ss.oid)))
	if oid != dirserv.NullOID {
		return oid, nil
	}
	oid, err := ss.ds.Ensure(ctx, ss.ds.Root(), ss.p, nil)
	if err != nil {
		return dirserv.NullOID, err
	}
	atomic.StoreUint64((*uint64)(&ss.oid), uint64(oid))
	return oid, nil
}

func (ss *systemSet) Add(ctx context.Context, id cadata.ID) error {
	oid, err := ss.getOID(ctx)
	if err != nil {
		return err
	}
	return ss.sm.open(oid).Add(ctx, id)
}

func (ss *systemSet) Delete(ctx context.Context, id cadata.ID) error {
	oid, err := ss.getOID(ctx)
	if err != nil {
		return err
	}
	return ss.sm.open(oid).Delete(ctx, id)
}

func (ss *systemSet) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	oid, err := ss.getOID(ctx)
	if err != nil {
		return false, err
	}
	return ss.sm.open(oid).Exists(ctx, id)
}

func (ss *systemSet) List(ctx context.Context, span cadata.Span, ids []cadata.ID) (int, error) {
	oid, err := ss.getOID(ctx)
	if err != nil {
		return 0, err
	}
	return ss.sm.open(oid).List(ctx, span, ids)
}
