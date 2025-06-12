// Package bclocal implements a local Blobcache service.
package bclocal

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/state/cadata"
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
}

// createEphemeralHandle creates a handle that expires at the given time.
// it will be stored in memory and not in the database.
func (s *Service) createEphemeralHandle(target blobcache.OID, expiresAt time.Time) blobcache.Handle {
	secret := [16]byte{}
	rand.Read(secret[:])
	s.handles[handleKey(blobcache.Handle{OID: target, Secret: secret})] = handle{expiresAt: expiresAt}
	return blobcache.Handle{OID: target, Secret: secret}
}

func (s *Service) resolveVol(h blobcache.Handle) (blobcache.OID, error) {
	k := handleKey(h)
	if _, exists := s.handles[k]; exists {
		return h.OID, nil
	}
	// if the handle is anchored, then it will be in the database.
	var oid blobcache.OID
	if err := s.db.Get(&oid, `SELECT id FROM volumes
		JOIN handles ON volumes.id = handles.target
		WHERE handles.id = ?
	`, k); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blobcache.OID{}, blobcache.ErrNotFound{Type: "volume", ID: h.OID}
		}
		return blobcache.OID{}, err
	}
	return oid, nil
}

func (s *Service) resolveTx(txh blobcache.Handle, touch bool) (blobcache.OID, error) {
	// transactions are not stored in the database, so we only have to check the handles map.
	if _, exists := s.handles[handleKey(txh)]; !exists {
		return blobcache.OID{}, blobcache.ErrInvalidHandle{Handle: txh}
	} else {
		if touch {
			s.handles[handleKey(txh)] = handle{expiresAt: time.Now().Add(DefaultTxTTL)}
		}
		return txh.OID, nil
	}
}

func (s *Service) CreateVolume(ctx context.Context, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if err := vspec.Validate(); err != nil {
		return nil, err
	}
	info := blobcache.VolumeInfo{
		ID:       blobcache.NewOID(),
		HashAlgo: vspec.HashAlgo,
		Backend:  blobcache.VolumeBackendToOID(vspec.Backend),
	}
	volid, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*blobcache.OID, error) {
		return createVolume(tx, info)
	})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	handle := s.createEphemeralHandle(*volid, time.Now().Add(DefaultVolumeTTL))
	return &handle, nil
}

// Anchor causes a handle to persist indefinitely.
func (s *Service) Anchor(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, handleKey(h))
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return insertHandle(tx, h, nil)
	})
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
	s.mu.Lock()
	defer s.mu.Unlock()
	volID, err := s.resolveVol(volh)
	if err != nil {
		return nil, err
	}
	// loop until there is no active tx on the volume.
	var oid *blobcache.OID
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for oid == nil {
		var err error
		if oid, err = dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*blobcache.OID, error) {
			if mutate {
				if yes, err := volumeHasActiveTx(tx, volID); err != nil {
					return nil, err
				} else if yes {
					return nil, nil
				}
			}
			return createTx(tx, volID, mutate)
		}); err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
		}
	}
	h := s.createEphemeralHandle(*oid, time.Now().Add(DefaultTxTTL))
	return &h, nil
}

func (s *Service) Commit(ctx context.Context, txh blobcache.Handle, root []byte) error {
	txid, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		var err error
		txRow, err := getTx(tx, txid)
		if err != nil {
			return err
		}
		volRow, err := getVolume(tx, txRow.VolID)
		if err != nil {
			return err
		}
		if err := mergeStores(tx, volRow.StoreID, []StoreID{txRow.StoreID}); err != nil {
			return err
		}
		if err := setVolumeRoot(tx, volRow.ID, root); err != nil {
			return err
		}
		return dropTx(tx, txh.OID)
	}); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, handleKey(txh))
	return nil
}

func (s *Service) Abort(ctx context.Context, txh blobcache.Handle) error {
	txid, err := s.resolveTx(txh, false)
	if err != nil {
		return err
	}
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return dropTx(tx, txid)
	})
}

func (s *Service) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	txid, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		txRow, err := getTx(tx, txid)
		if err != nil {
			return err
		}
		return getVolumeRoot(tx, txRow.VolID, dst)
	})
}

func (s *Service) Post(ctx context.Context, txh blobcache.Handle, data []byte) (blobcache.CID, error) {
	txid, err := s.resolveTx(txh, true)
	if err != nil {
		return blobcache.CID{}, err
	}
	var cid blobcache.CID
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		// TODO: get hf from volume spec
		hf := func(data []byte) blobcache.CID {
			return blobcache.CID(blake3.Sum256(data))
		}
		cid = hf(data)
		txRow, err := getTx(tx, txid)
		if err != nil {
			return err
		}
		if err := ensureBlob(tx, cid, nil, data); err != nil {
			return err
		}
		if err := addBlob(tx, txRow.StoreID, cid); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return blobcache.CID{}, err
	}
	return cid, nil
}

func (s *Service) Exists(ctx context.Context, txh blobcache.Handle, cid blobcache.CID) (bool, error) {
	txid, err := s.resolveTx(txh, true)
	if err != nil {
		return true, nil
	}
	var exists bool
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		txRow, err := getTx(tx, txid)
		if err != nil {
			return err
		}
		volRow, err := getVolume(tx, txRow.VolID)
		if err != nil {
			return err
		}
		exists, err = storesContainsBlob(tx, []StoreID{txRow.StoreID, volRow.StoreID}, cid)
		if err != nil {
			return err
		}
		// if the blob exists, then we need to add it to the tx store.
		if exists {
			if err := addBlob(tx, txRow.StoreID, cid); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *Service) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, buf []byte) (int, error) {
	txid, err := s.resolveTx(txh, true)
	if err != nil {
		return 0, err
	}
	var n int
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		txRow, err := getTx(tx, txid)
		if err != nil {
			return err
		}
		volRow, err := getVolume(tx, txRow.VolID)
		if err != nil {
			return err
		}
		if n, err = readBlob(tx, []StoreID{txRow.StoreID, volRow.StoreID}, cid, buf); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *Service) Delete(ctx context.Context, txh blobcache.Handle, cid blobcache.CID) error {
	txid, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		txRow, err := getTx(tx, txid)
		if err != nil {
			return err
		}
		return deleteBlob(tx, txRow.StoreID, cid)
	})
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

func getHashFunc(hashFunc blobcache.HashAlgo) cadata.HashFunc {
	switch hashFunc {
	case blobcache.HashAlgo_BLAKE3_256:
		return func(data []byte) blobcache.CID {
			return blobcache.CID(blake3.Sum256(data))
		}
	default:
		panic(fmt.Sprintf("unknown hash algo: %v", hashFunc))
	}
}

func handleKey(h blobcache.Handle) [32]byte {
	return blake3.Sum256(slices.Concat(h.OID[:], h.Secret[:]))
}
