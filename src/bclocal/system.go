package bclocal

import (
	"context"
	"sync"
	"time"

	"blobcache.io/blobcache/src/bclocal/internal/localvol"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/internal/backend/memory"
)

var _ backend.System[localvol.Params, backend.Volume, blobcache.QueueBackend_Memory] = &system{}

type system struct {
	vols   *localvol.System
	queues *memory.System

	mu   sync.RWMutex
	subs map[backend.Volume]map[backend.Queue]blobcache.VolSubSpec
}

func newSystem(vols *localvol.System, queues *memory.System) *system {
	return &system{
		vols:   vols,
		queues: queues,
		subs:   make(map[backend.Volume]map[backend.Queue]blobcache.VolSubSpec),
	}
}

func (s *system) Up(ctx context.Context, spec localvol.Params) (backend.Volume, error) {
	vol, err := s.vols.Up(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &volume{
		inner: vol,
		sys:   s,
	}, nil
}

func (s *system) Drop(ctx context.Context, vol backend.Volume) error {
	s.removeVolume(vol)
	inner, ok := unwrapLocalVolume(vol)
	if !ok {
		return nil
	}
	return s.vols.Drop(ctx, inner)
}

func (s *system) CreateQueue(ctx context.Context, spec blobcache.QueueBackend_Memory) (backend.Queue, error) {
	return s.queues.CreateQueue(ctx, spec)
}

func (s *system) QueueDown(ctx context.Context, q backend.Queue) error {
	s.removeQueue(q)
	return s.queues.QueueDown(ctx, q)
}

func (s *system) SubToVol(ctx context.Context, vol backend.Volume, q backend.Queue, spec blobcache.VolSubSpec) error {
	volKey := s.normalizeVolume(vol)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subs == nil {
		s.subs = make(map[backend.Volume]map[backend.Queue]blobcache.VolSubSpec)
	}
	subs := s.subs[volKey]
	if subs == nil {
		subs = make(map[backend.Queue]blobcache.VolSubSpec)
		s.subs[volKey] = subs
	}
	subs[q] = spec
	return nil
}

func (s *system) notifyVol(ctx context.Context, vol backend.Volume) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	volKey := s.normalizeVolume(vol)
	s.mu.RLock()
	subs := s.subs[volKey]
	if len(subs) == 0 {
		s.mu.RUnlock()
		return nil
	}
	type item struct {
		q    backend.Queue
		spec blobcache.VolSubSpec
	}
	items := make([]item, 0, len(subs))
	for q, spec := range subs {
		items = append(items, item{q: q, spec: spec})
	}
	s.mu.RUnlock()

	var firstErr error
	for _, it := range items {
		_ = it.spec
		if err := it.q.Enqueue(ctx, []blobcache.Message{{}}); err != nil && firstErr == nil {
			firstErr = err
		}
		if ctx.Err() != nil {
			return firstErr
		}
	}
	return firstErr
}

func (s *system) removeVolume(vol backend.Volume) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, s.normalizeVolume(vol))
}

func (s *system) removeQueue(q backend.Queue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for vol, subs := range s.subs {
		delete(subs, q)
		if len(subs) == 0 {
			delete(s.subs, vol)
		}
	}
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
	return &transaction{
		inner: tx,
		sys:   v.sys,
		vol:   v,
	}, nil
}

func (v *volume) AccessSubVolume(ctx context.Context, target blobcache.LinkToken) (blobcache.ActionSet, error) {
	return v.inner.AccessSubVolume(ctx, target)
}

func (v *volume) Inner() *localvol.Volume {
	return v.inner
}

type transaction struct {
	inner backend.Tx
	sys   *system
	vol   *volume
}

func (t *transaction) Params() blobcache.TxParams {
	return t.inner.Params()
}

func (t *transaction) Commit(ctx context.Context) error {
	if err := t.inner.Commit(ctx); err != nil {
		return err
	}
	_ = t.sys.notifyVol(ctx, t.vol)
	return nil
}

func (t *transaction) Abort(ctx context.Context) error {
	return t.inner.Abort(ctx)
}

func (t *transaction) Save(ctx context.Context, src []byte) error {
	return t.inner.Save(ctx, src)
}

func (t *transaction) Load(ctx context.Context, dst *[]byte) error {
	return t.inner.Load(ctx, dst)
}

func (t *transaction) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return t.inner.Post(ctx, data, opts)
}

func (t *transaction) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return t.inner.Get(ctx, cid, buf, opts)
}

func (t *transaction) Delete(ctx context.Context, cids []blobcache.CID) error {
	return t.inner.Delete(ctx, cids)
}

func (t *transaction) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return t.inner.Exists(ctx, cids, dst)
}

func (t *transaction) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return t.inner.IsVisited(ctx, cids, dst)
}

func (t *transaction) Visit(ctx context.Context, cids []blobcache.CID) error {
	return t.inner.Visit(ctx, cids)
}

func (t *transaction) MaxSize() int {
	return t.inner.MaxSize()
}

func (t *transaction) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return t.inner.Hash(salt, data)
}

func (t *transaction) Link(ctx context.Context, svoid blobcache.OID, rights blobcache.ActionSet, subvol backend.Volume) (*blobcache.LinkToken, error) {
	return t.inner.Link(ctx, svoid, rights, subvol)
}

func (t *transaction) Unlink(ctx context.Context, targets []blobcache.LinkToken) error {
	return t.inner.Unlink(ctx, targets)
}

func (t *transaction) VisitLinks(ctx context.Context, targets []blobcache.LinkToken) error {
	return t.inner.VisitLinks(ctx, targets)
}
