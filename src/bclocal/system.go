package bclocal

import (
	"context"

	"blobcache.io/blobcache/src/bclocal/internal/localvol"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/internal/backend/memory"
)

var _ backend.System[localvol.Params, backend.Volume, blobcache.QueueBackend_Memory, *memory.Queue] = &system{}

type system struct {
	vols   *localvol.System
	queues *memory.System
}

func newSystem(vols *localvol.System, queues *memory.System) *system {
	return &system{
		vols:   vols,
		queues: queues,
	}
}

func (s *system) VolumeUp(ctx context.Context, spec localvol.Params) (backend.Volume, error) {
	vol, err := s.vols.VolumeUp(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &volume{
		inner: vol,
		sys:   s,
	}, nil
}

func (s *system) CreateQueue(ctx context.Context, spec blobcache.QueueBackend_Memory) (*memory.Queue, error) {
	return s.queues.CreateQueue(ctx, spec)
}

func (s *system) VolumeDestroy(ctx context.Context, vol backend.Volume) error {
	return s.vols.VolumeDestroy(ctx, vol.(*localvol.Volume))
}

func (s *system) normalizeVolume(vol backend.Volume) backend.Volume {
	if v, ok := vol.(*volume); ok {
		return v.inner
	}
	return vol
}

func unwrapLocalVolume(vol backend.Volume) (*localvol.Volume, bool) {
	switch v := vol.(type) {
	case *localvol.Volume:
		return v, true
	case interface{ Inner() *localvol.Volume }:
		return v.Inner(), true
	default:
		return nil, false
	}
}

type volume struct {
	inner *localvol.Volume
	sys   *system
}

func (v *volume) GetParams() blobcache.VolumeConfig {
	return v.inner.GetParams()
}

func (v *volume) GetBackend() blobcache.VolumeBackend[blobcache.OID] {
	return v.inner.GetBackend()
}

func (v *volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (backend.Tx, error) {
	tx, err := v.inner.BeginTx(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &txWrap{
		inner: tx,
		sys:   v.sys,
		vol:   v,
	}, nil
}

func (v *volume) AccessSubVolume(ctx context.Context, target blobcache.LinkToken) (blobcache.ActionSet, error) {
	return v.inner.AccessSubVolume(ctx, target)
}

func (v *volume) Down(ctx context.Context) error {
	return v.inner.Down(ctx)
}

func (v *volume) Inner() *localvol.Volume {
	return v.inner
}

type txWrap struct {
	inner backend.Tx
	sys   *system
	vol   *volume
}

func (t *txWrap) Params() blobcache.TxParams {
	return t.inner.Params()
}

func (t *txWrap) Commit(ctx context.Context) error {
	if err := t.inner.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (t *txWrap) Abort(ctx context.Context) error {
	return t.inner.Abort(ctx)
}

func (t *txWrap) Save(ctx context.Context, src []byte) error {
	return t.inner.Save(ctx, src)
}

func (t *txWrap) Load(ctx context.Context, dst *[]byte) error {
	return t.inner.Load(ctx, dst)
}

func (t *txWrap) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return t.inner.Post(ctx, data, opts)
}

func (t *txWrap) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return t.inner.Get(ctx, cid, buf, opts)
}

func (t *txWrap) Delete(ctx context.Context, cids []blobcache.CID) error {
	return t.inner.Delete(ctx, cids)
}

func (t *txWrap) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return t.inner.Exists(ctx, cids, dst)
}

func (t *txWrap) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return t.inner.IsVisited(ctx, cids, dst)
}

func (t *txWrap) Visit(ctx context.Context, cids []blobcache.CID) error {
	return t.inner.Visit(ctx, cids)
}

func (t *txWrap) MaxSize() int {
	return t.inner.MaxSize()
}

func (t *txWrap) HashAlgo() blobcache.HashAlgo {
	return t.inner.HashAlgo()
}

func (t *txWrap) Link(ctx context.Context, svoid blobcache.OID, rights blobcache.ActionSet, subvol backend.Volume) (*blobcache.LinkToken, error) {
	return t.inner.Link(ctx, svoid, rights, subvol)
}

func (t *txWrap) Unlink(ctx context.Context, targets []blobcache.LinkTokenID) error {
	return t.inner.Unlink(ctx, targets)
}

func (t *txWrap) VisitLinks(ctx context.Context, targets []blobcache.LinkTokenID) error {
	return t.inner.VisitLinks(ctx, targets)
}
