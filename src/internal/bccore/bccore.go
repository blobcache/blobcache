// Package bccore implements handles and object lifecycles.
package bccore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"go.brendoncarroll.net/exp/singleflight"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
)

const (
	DefaultTxTTL    = 1 * time.Minute
	DefaultQueueTTL = 2 * time.Minute
)

type (
	Queue  backend.Queue
	Volume backend.Volume
	Tx     backend.Tx
)

type volume struct {
	info    blobcache.VolumeInfo
	backend backend.Volume
}

type queue struct {
	info    blobcache.QueueInfo
	backend backend.Queue
	subs    map[*sub]struct{}
}

type transaction struct {
	backend backend.Tx
	volume  *volume
}

// System manages objects and handles to those objects
type System struct {
	p    Params
	root volume

	// mu guards the volumes, queues, and txns.
	// pure handle operations like Drop, KeepAlive, Inspect, etc. do not require this lock.
	// mu should always be taken for a superset of the time that the handle system's lock is taken.
	mu      sync.RWMutex
	volumes map[blobcache.OID]volume
	queues  map[blobcache.OID]queue
	txns    map[blobcache.OID]transaction

	handles handleSystem
	setup   singleflight.Group[blobcache.OID, AnyObject]
	hub     hub
}

type AnyObject struct {
	Volume Volume
	Queue  Queue
}

type Params struct {
	// Root is the root Volume, which will be given the 0 ID
	Root Volume
	// Up is called to bring up a Volume or Queue
	Up func(ctx context.Context, oid blobcache.OID) (AnyObject, error)
	// OnLink is called before the object is linked to.
	// The object must be persisted so that it can be loaded later
	OnLink func(ctx context.Context, info blobcache.Info, ao AnyObject) error
	// OnSave, if not nil, is called on Save.
	// This is where schema checks can be performed.
	OnSave func(ctx context.Context, vol Volume, tx Tx, root []byte) error
}

func New(p Params) System {
	return System{
		p: p,
		root: volume{
			info: blobcache.VolumeInfo{
				VolumeConfig: p.Root.GetParams(),
				Backend:      p.Root.GetBackend(),
			},
			backend: p.Root,
		},
	}
}

// IsUp returns true if x has at least one active handle pointing at it, and it's state is loaded in memory.
func (sys *System) IsUp(x blobcache.OID) bool {
	if x == (blobcache.OID{}) {
		return true
	}
	return sys.handles.isAlive(x)
}

// Cleanup should be called periodically, so that unused handles can be expired.
func (s *System) Cleanup(ctx context.Context, now time.Time, onDown func(blobcache.OID)) error {
	// 1. Delete expired handles.
	logctx.Info(ctx, "cleaning up handles")
	s.handles.filter(func(h handle) bool {
		// return true to keep, false to delete.
		return h.expiresAt.After(now)
	})

	// 2. Release resources for transactions which do not have a handle.
	logctx.Info(ctx, "cleaning up transactions")
	s.mu.Lock()
	defer s.mu.Unlock()
	for oid := range s.txns {
		if !s.handles.isAlive(oid) {
			delete(s.txns, oid)
		}
	}

	// 3. Release resources for mounted volumes which do not have a handle.
	logctx.Info(ctx, "cleaning up volumes")
	var ret []blobcache.OID
	for oid := range s.volumes {
		if !s.handles.isAlive(oid) {
			vol := s.volumes[oid]
			delete(s.volumes, oid)
			ret = append(ret, oid)
			_ = vol.backend.VolumeDown(ctx)
		}
	}

	// 4. Release resources for queues which do not have a handle.
	logctx.Info(ctx, "cleaning up queues")
	for oid := range s.queues {
		if !s.handles.isAlive(oid) {
			q := s.queues[oid]
			delete(s.queues, oid)
			_ = q.backend.QueueDown(ctx)
		}
	}
	return nil
}

func (sys *System) Create(ctx context.Context, oid blobcache.OID, x AnyObject, rights blobcache.ActionSet, createdAt time.Time, ttl time.Duration) (blobcache.Handle, error) {
	if oid == (blobcache.OID{}) {
		return blobcache.Handle{}, fmt.Errorf("cannot create new object with root OID")
	}
	switch {
	case x.Volume != nil:
		if !sys.addVolume(oid, x.Volume) {
			return blobcache.Handle{}, fmt.Errorf("volume already exists")
		}
	case x.Queue != nil:
		if !sys.addQueue(oid, x.Queue) {
			return blobcache.Handle{}, fmt.Errorf("queue already exists")
		}
	default:
		return blobcache.Handle{}, fmt.Errorf("cannot create empty object")
	}

	expireAt := createdAt.Add(ttl)
	h := sys.handles.Create(oid, rights, createdAt, expireAt)
	if sys.p.OnLink != nil {
		info, err := sys.Inspect(ctx, h)
		if err != nil {
			sys.handles.Drop(h)
			return blobcache.Handle{}, err
		}
		if err := sys.p.OnLink(ctx, info, x); err != nil {
			sys.handles.Drop(h)
			return blobcache.Handle{}, err
		}
	}
	return h, nil
}

func (sys *System) Mint(x blobcache.OID, rights blobcache.ActionSet, createdAt time.Time, ttl time.Duration) blobcache.Handle {
	// TODO: need to check that the object exists
	return sys.handles.Create(x, rights, createdAt, createdAt.Add(ttl))
}

func (sys *System) ResolveVol(qh blobcache.Handle) (Volume, error) {
	q, _, err := sys.resolveVol(qh)
	if err != nil {
		return nil, err
	}
	return q.backend, nil
}

func (sys *System) ResolveQueue(qh blobcache.Handle) (Queue, error) {
	q, _, err := sys.resolveQueue(qh, 0)
	if err != nil {
		return nil, err
	}
	return q.backend, nil
}

func (sys *System) resolveVol(x blobcache.Handle) (volume, blobcache.ActionSet, error) {
	sys.mu.RLock()
	defer sys.mu.RUnlock()
	oid, rights := sys.handles.Resolve(x)
	if rights == 0 {
		return volume{}, 0, blobcache.ErrInvalidHandle{Handle: x}
	}
	if oid == (blobcache.OID{}) {
		return sys.root, rights, nil
	}
	vol, exists := sys.volumes[oid]
	if !exists {
		return volume{}, 0, fmt.Errorf("handle does not refer to volume, OID=%v ", x.OID)
	}
	return vol, rights, nil
}

// resolveTx looks up the transaction handle from memory.
func (sys *System) resolveTx(txh blobcache.Handle, touch bool, requires blobcache.ActionSet) (transaction, error) {
	sys.mu.RLock()
	defer sys.mu.RUnlock()
	oid, rights := sys.handles.Resolve(txh)
	if rights == 0 {
		return transaction{}, blobcache.ErrInvalidHandle{Handle: txh}
	}
	tx, exists := sys.txns[oid]
	if !exists {
		return transaction{}, blobcache.ErrInvalidHandle{Handle: txh}
	}
	if rights&requires < requires {
		return transaction{}, blobcache.ErrPermission{
			Handle:   txh,
			Rights:   rights,
			Requires: requires,
		}
	}
	if touch {
		sys.handles.KeepAlive(txh, time.Now().Add(DefaultTxTTL))
	}
	return tx, nil
}

func (sys *System) addVolume(oid blobcache.OID, vol backend.Volume) bool {
	if oid == (blobcache.OID{}) {
		return false
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	if _, exists := sys.volumes[oid]; exists {
		return false
	}
	if sys.volumes == nil {
		sys.volumes = make(map[blobcache.OID]volume)
	}
	sys.volumes[oid] = volume{
		info: blobcache.VolumeInfo{
			ID:           oid,
			VolumeConfig: vol.GetParams(),
			Backend:      vol.GetBackend(),
		},
		backend: vol,
	}
	return true
}

func (sys *System) addQueue(oid blobcache.OID, q backend.Queue) bool {
	if oid == (blobcache.OID{}) {
		return false
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	if _, exists := sys.queues[oid]; exists {
		return false
	}
	if sys.queues == nil {
		sys.queues = make(map[blobcache.OID]queue)
	}
	sys.queues[oid] = queue{
		info: blobcache.QueueInfo{
			ID:      oid,
			Config:  q.Config(),
			Backend: q.Backend(),
		},
		backend: q,
		subs:    make(map[*sub]struct{}),
	}
	return true
}

func (sys *System) resolveQueue(qh blobcache.Handle, requires blobcache.ActionSet) (queue, blobcache.ActionSet, error) {
	sys.mu.RLock()
	defer sys.mu.RUnlock()
	oid, rights := sys.handles.Resolve(qh)
	if rights == 0 {
		return queue{}, 0, blobcache.ErrInvalidHandle{Handle: qh}
	}
	if rights&requires < requires {
		return queue{}, 0, blobcache.ErrPermission{
			Handle:   qh,
			Rights:   rights,
			Requires: requires,
		}
	}
	q, exists := sys.queues[oid]
	if !exists {
		return queue{}, 0, blobcache.ErrInvalidHandle{Handle: qh}
	}
	return q, rights, nil
}

// Share creates a new handle from x and applies mask.
func (sys *System) Share(x blobcache.Handle, mask blobcache.ActionSet) (blobcache.Handle, error) {
	oid, originalRights := sys.handles.Resolve(x)
	if originalRights == 0 {
		return blobcache.Handle{}, blobcache.ErrInvalidHandle{Handle: x}
	}
	rights := originalRights.Share()
	if rights == 0 {
		return blobcache.Handle{}, blobcache.ErrPermission{
			Handle:   x,
			Rights:   originalRights,
			Requires: blobcache.Action_SHARE_ACK,
		}
	}
	rights &= mask
	now := time.Now()
	expiresAt := now.Add(DefaultQueueTTL)
	out := sys.handles.Create(oid, rights, now, expiresAt)
	return out, nil
}

// Drop implements blobcache.HandleAPI.Drop
func (sys *System) Drop(_ context.Context, h blobcache.Handle) error {
	sys.handles.Drop(h)
	return nil
}

// KeepAlive implements blobcache.HandleAPI.KeepAlive
func (sys *System) KeepAlive(_ context.Context, hs []blobcache.Handle) error {
	for _, h := range hs {
		if _, rights := sys.handles.Resolve(h); rights == 0 {
			return blobcache.ErrInvalidHandle{Handle: h}
		}
		panic("todo, need to check what kind of object it is to get timeout")
		sys.handles.KeepAlive(h, time.Now().Add(DefaultTxTTL))
	}
	return nil
}

// InspectHandle implements blobcache.HandleAPI.InspectHandle
func (sys *System) InspectHandle(_ context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	hstate, exists := sys.handles.Inspect(h)
	if !exists {
		return nil, blobcache.ErrInvalidHandle{Handle: h}
	}
	return &blobcache.HandleInfo{
		OID:       h.OID,
		Rights:    hstate.rights,
		CreatedAt: tai64.Now().TAI64(),
		ExpiresAt: tai64.FromGoTime(hstate.expiresAt).TAI64(),
	}, nil
}

func (sys *System) Inspect(ctx context.Context, h blobcache.Handle) (blobcache.Info, error) {
	hi, err := sys.InspectHandle(ctx, h)
	if err != nil {
		return blobcache.Info{}, err
	}
	ret := blobcache.Info{Handle: *hi}

	sys.mu.RLock()
	_, isTx := sys.txns[h.OID]
	_, isQueue := sys.queues[h.OID]
	_, isVol := sys.volumes[h.OID]
	sys.mu.RUnlock()

	if isTx {
		tx, err := sys.InspectTx(ctx, h)
		if err != nil {
			return blobcache.Info{}, err
		}
		ret.Tx = tx
		return ret, nil
	}
	if isQueue {
		q, err := sys.InspectQueue(ctx, h)
		if err != nil {
			return blobcache.Info{}, err
		}
		ret.Queue = &q
		return ret, nil
	}
	if h.OID == (blobcache.OID{}) || isVol {
		vi, err := sys.InspectVolume(ctx, h)
		if err != nil {
			return blobcache.Info{}, err
		}
		ret.Volume = vi
		return ret, nil
	}
	return blobcache.Info{}, fmt.Errorf("handle refers to unknown object type: %v", h.OID)
}
