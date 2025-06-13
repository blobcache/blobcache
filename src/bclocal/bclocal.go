// Package bclocal implements a local Blobcache service.
package bclocal

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/volumes"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
)

const (
	DefaultVolumeTTL = 5 * time.Minute
	DefaultTxTTL     = 1 * time.Minute
)

var _ blobcache.Service = &Service{}

type Env struct {
	DB *sqlx.DB
}

// Service implements a blobcache.Service.
type Service struct {
	env Env
	db  *sqlx.DB

	mu sync.RWMutex
	// handles stores ephemeral handles.
	// Anchored handles are stored in the database.
	handles map[[32]byte]handle
}

func New(env Env) *Service {
	return &Service{
		env:     env,
		db:      env.DB,
		handles: make(map[[32]byte]handle),
	}
}

func (s *Service) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		s.cleanupHandlesLoop(ctx)
		return nil
	})
	return eg.Wait()
}

func (s *Service) cleanupHandlesLoop(ctx context.Context) {
	tick := time.NewTicker(300 * time.Second)
	defer tick.Stop()
	for {
		s.mu.Lock()
		for k, h := range s.handles {
			if h.expiresAt.Before(time.Now()) {
				delete(s.handles, k)
			}
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-tick.C:
		}
	}
}

type handle struct {
	expiresAt time.Time // zero value means no expiration

	// vol will be set if the handle is for a volume.
	vol volumes.Volume[[]byte]
	// txn will be set if the handle is for a transaction.
	txn volumes.Tx[[]byte]
}

// createEphemeralHandle creates a handle that expires at the given time.
// it will be stored in memory and not in the database.
func (s *Service) createEphemeralHandle(target blobcache.OID, expiresAt time.Time, vol volumes.Volume[[]byte], txn volumes.Tx[[]byte]) blobcache.Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	secret := [16]byte{}
	rand.Read(secret[:])
	ret := blobcache.Handle{OID: target, Secret: secret}
	s.handles[handleKey(ret)] = handle{
		expiresAt: expiresAt,

		vol: vol,
		txn: txn,
	}
	return ret
}

func (s *Service) resolveVol(ctx context.Context, h blobcache.Handle) (volumes.Volume[[]byte], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	k := handleKey(h)
	if h, exists := s.handles[k]; exists {
		return h.vol, nil
	}
	// if the handle is anchored, then it will be in the database.
	volRow, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*volumeRow, error) {
		return getVolume(tx, h.OID)
	})
	if err != nil {
		return nil, err
	}
	var backend blobcache.VolumeBackend[blobcache.OID]
	if err := json.Unmarshal(volRow.Backend, &backend); err != nil {
		return nil, err
	}
	return s.makeVolume(volRow.ID, backend)
}

// resolveTx looks up the transaction handle from memory.
// If the handle is valid it will load a new transaction.
func (s *Service) resolveTx(txh blobcache.Handle, touch bool) (volumes.Tx[[]byte], error) {
	if touch {
		s.mu.Lock()
		defer s.mu.Unlock()
	} else {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}
	// transactions are not stored in the database, so we only have to check the handles map.
	h, exists := s.handles[handleKey(txh)]
	if !exists {
		return nil, blobcache.ErrInvalidHandle{Handle: txh}
	}
	if h.txn == nil {
		return nil, fmt.Errorf("handle is valid, but not for a transaction")
	}
	if touch {
		h := s.handles[handleKey(txh)]
		h.expiresAt = time.Now().Add(DefaultTxTTL)
		s.handles[handleKey(txh)] = h
	}
	return h.txn, nil
}

func (s *Service) CreateVolume(ctx context.Context, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if err := vspec.Validate(); err != nil {
		return nil, err
	}
	if vspec.Salted {
		return nil, fmt.Errorf("salted volumes are not yet supported")
	}
	info := blobcache.VolumeInfo{
		ID:       blobcache.NewOID(),
		HashAlgo: vspec.HashAlgo,
		MaxSize:  vspec.MaxSize,
		Backend:  blobcache.VolumeBackendToOID(vspec.Backend),
	}
	volid, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*blobcache.OID, error) {
		return createVolume(tx, info)
	})
	if err != nil {
		return nil, err
	}
	vol, err := s.makeVolume(*volid, info.Backend)
	if err != nil {
		return nil, err
	}
	handle := s.createEphemeralHandle(*volid, time.Now().Add(DefaultVolumeTTL), vol, nil)
	return &handle, nil
}

func (s *Service) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	vol, err := s.resolveVol(ctx, h)
	if err != nil {
		return nil, err
	}
	switch vol := vol.(type) {
	case *localVolume:
		volRow, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*volumeRow, error) {
			return getVolume(tx, vol.id)
		})
		if err != nil {
			return nil, err
		}
		var backend blobcache.VolumeBackend[blobcache.OID]
		if err := json.Unmarshal(volRow.Backend, &backend); err != nil {
			return nil, err
		}
		return &blobcache.VolumeInfo{
			ID:       volRow.ID,
			HashAlgo: blobcache.HashAlgo(volRow.HashAlgo),
			MaxSize:  volRow.MaxSize,
			Backend:  backend,
		}, nil
	default:
		return nil, fmt.Errorf("cannot inspect volume of type %T", vol)
	}
}

// Anchor causes a handle to persist indefinitely.
func (s *Service) Anchor(ctx context.Context, h blobcache.Handle) error {
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return insertHandle(tx, h, nil)
	}); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.handles, handleKey(h))
	s.mu.Unlock()
	return nil
}

func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, handleKey(h))
	return nil
}

func (s *Service) Await(ctx context.Context, cond blobcache.Conditions) error {
	panic("not implemented")
}

func (s *Service) StartSync(ctx context.Context, src blobcache.Handle, dst blobcache.Handle) error {
	panic("not implemented")
}

func (s *Service) CreateRule(ctx context.Context, rspec blobcache.RuleSpec) (*blobcache.Handle, error) {
	panic("not implemented")
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, mutate bool) (*blobcache.Handle, error) {
	vol, err := s.resolveVol(ctx, volh)
	if err != nil {
		return nil, err
	}
	txn, err := vol.BeginTx(ctx, mutate)
	if err != nil {
		return nil, err
	}

	txid := blobcache.NewOID()
	if ltxn, ok := txn.(*localVolumeTx); ok {
		txid = ltxn.txid
	}
	h := s.createEphemeralHandle(txid, time.Now().Add(DefaultTxTTL), nil, txn)
	return &h, nil
}

func (s *Service) Commit(ctx context.Context, txh blobcache.Handle, root []byte) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	if err := txn.Commit(ctx, root); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, handleKey(txh))
	return nil
}

func (s *Service) Abort(ctx context.Context, txh blobcache.Handle) error {
	txn, err := s.resolveTx(txh, false)
	if err != nil {
		return err
	}
	if err := txn.Abort(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, handleKey(txh))
	return nil
}

func (s *Service) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.Load(ctx, dst)
}

func (s *Service) Post(ctx context.Context, txh blobcache.Handle, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return blobcache.CID{}, err
	}
	return txn.Post(ctx, salt, data)
}

func (s *Service) Exists(ctx context.Context, txh blobcache.Handle, cid blobcache.CID) (bool, error) {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return false, err
	}
	return txn.Exists(ctx, cid)
}

func (s *Service) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return 0, err
	}
	return txn.Get(ctx, cid, salt, buf)
}

func (s *Service) Delete(ctx context.Context, txh blobcache.Handle, cid blobcache.CID) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.Delete(ctx, cid)
}

func (s *Service) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range hs {
		hstate := s.handles[handleKey(h)]
		s.handles[handleKey(h)] = handle{
			expiresAt: hstate.expiresAt.Add(300 * time.Second),
		}
	}
	return nil
}

func handleKey(h blobcache.Handle) [32]byte {
	return blake3.Sum256(slices.Concat(h.OID[:], h.Secret[:]))
}

// makeVolume constructs an in-memory volume object from a backend.
func (s *Service) makeVolume(oid blobcache.OID, backend blobcache.VolumeBackend[blobcache.OID]) (volumes.Volume[[]byte], error) {
	switch {
	case backend.Local != nil:
		return &localVolume{db: s.db, id: oid}, nil
	case backend.Remote != nil:
		fallthrough
	case backend.Git != nil:
		fallthrough

	// higher order volumes
	case backend.Vault != nil:
		panic("not implemented")
	default:
		return nil, fmt.Errorf("empty backend")
	}
}
