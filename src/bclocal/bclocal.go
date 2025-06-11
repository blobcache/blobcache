// Package bclocal implements a local Blobcache service.
package bclocal

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cells"
	"lukechampine.com/blake3"
)

const (
	DefaultVolumeTTL = 5 * time.Minute
	DefaultTxTTL     = 1 * time.Minute
)

var _ blobcache.Service = &Service{}

// Service implements a
type Service struct {
	env Env

	mu      sync.Mutex
	handles map[[32]byte]handle
	volumes map[blobcache.OID]*volState
	txns    map[blobcache.OID]*txn
}

type Env struct {
	DB         *sqlx.DB
	PacketConn net.PacketConn
}

func New(env Env) *Service {
	return &Service{
		env: env,

		handles: make(map[[32]byte]handle),
		volumes: make(map[blobcache.OID]*volState),
		txns:    make(map[blobcache.OID]*txn),
	}
}

func (s *Service) Run(ctx context.Context) error {
	return nil
}

type handle struct {
	expiresAt time.Time // zero value means no expiration
}

type volState struct {
	id blobcache.OID
	hf cadata.HashFunc

	sem   chan struct{}
	root  []byte
	blobs map[blobcache.CID][]byte
}

type txn struct {
	vol       *volState
	mutating  bool
	changeset map[blobcache.CID][]byte
}

func (s *Service) createHandle(target blobcache.OID, expiresAt time.Time) blobcache.Handle {
	secret := [16]byte{}
	rand.Read(secret[:])
	s.handles[handleKey(blobcache.Handle{OID: target, Secret: secret})] = handle{expiresAt: expiresAt}
	return blobcache.Handle{OID: target, Secret: secret}
}

func (s *Service) resolveVol(h blobcache.Handle) (*volState, error) {
	if _, exists := s.handles[handleKey(h)]; !exists {
		return nil, blobcache.ErrInvalidHandle{Handle: h}
	}
	volState, ok := s.volumes[h.OID]
	if !ok {
		return nil, blobcache.ErrNotFound{Type: "volume", ID: h.OID}
	}
	return volState, nil
}

func (s *Service) resolveTx(tx blobcache.Handle) (*txn, error) {
	if _, exists := s.handles[handleKey(tx)]; !exists {
		return nil, blobcache.ErrInvalidHandle{Handle: tx}
	}
	txn, ok := s.txns[tx.OID]
	if !ok {
		return nil, blobcache.ErrNotFound{Type: "tx", ID: tx.OID}
	}
	return txn, nil
}

func (s *Service) CreateVolume(ctx context.Context, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oid := blobcache.NewOID()
	volState := &volState{
		id: oid,
		hf: getHashFunc(vspec.HashAlgo),

		blobs: make(map[blobcache.CID][]byte),
		sem:   make(chan struct{}, 1),
	}
	volState.sem <- struct{}{}
	s.volumes[oid] = volState
	handle := s.createHandle(oid, time.Now().Add(DefaultVolumeTTL))
	return &handle, nil
}

func (s *Service) Anchor(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handles[handleKey(h)] = handle{expiresAt: time.Time{}}
	return nil
}

func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, handleKey(h))
	return nil
}

func (s *Service) Await(ctx context.Context, cond blobcache.Conditions) error {
	tick := time.NewTicker(time.Second / 10)
	defer tick.Stop()
	for {
		yes, err := s.conditionsMet(ctx, cond)
		if err != nil {
			return err
		}
		if yes {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (s *Service) conditionsMet(ctx context.Context, cond blobcache.Conditions) (bool, error) {
	for i := 0; i < len(cond.AllEqual)-1; i++ {
		vola, err := s.resolveVol(cond.AllEqual[i])
		if err != nil {
			return false, err
		}
		volb, err := s.resolveVol(cond.AllEqual[i+1])
		if err != nil {
			return false, err
		}
		if err := lockVolumes(ctx, vola, volb); err != nil {
			return false, err
		}
		if !bytes.Equal(vola.root, volb.root) {
			return false, nil
		}
	}
	return true, nil
}

func (s *Service) StartSync(ctx context.Context, src blobcache.Handle, dst blobcache.Handle) error {
	srcVol, err := s.resolveVol(src)
	if err != nil {
		return err
	}
	dstVol, err := s.resolveVol(dst)
	if err != nil {
		return err
	}
	go func() {
		if err := lockVolumes(ctx, srcVol, dstVol); err != nil {
			return
		}
		defer unlockVolumes(srcVol, dstVol)

	}()
	return nil
}

func (s *Service) CreateRule(ctx context.Context, rspec blobcache.RuleSpec) (*blobcache.Handle, error) {
	panic("not implemented")
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, mutate bool) (*blobcache.Handle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	volState, err := s.resolveVol(volh)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case _, isOpen := <-volState.sem:
		if !isOpen {
			// The volume was deleted.
			return nil, fmt.Errorf("volume not found")
		}
	}
	// TODO: we should probably check if there is already a writer on this volume.
	txid := blobcache.NewOID()
	s.txns[txid] = &txn{
		vol:       volState,
		mutating:  mutate,
		changeset: make(map[blobcache.CID][]byte),
	}
	txh := s.createHandle(txid, time.Now().Add(DefaultTxTTL))
	return &txh, nil
}

func (s *Service) Commit(ctx context.Context, tx blobcache.Handle, root []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	txn, err := s.resolveTx(tx)
	if err != nil {
		return err
	}
	delete(s.txns, tx.OID)
	// TODO: apply changeset.
	for cid, data := range txn.changeset {
		if data != nil && txn.vol.blobs[cid] == nil {
			txn.vol.blobs[cid] = data
		} else {
			delete(txn.vol.blobs, cid)
		}
	}
	txn.changeset = nil
	txn.vol.root = root
	txn.vol.sem <- struct{}{} // Release the semaphore
	return nil
}

func (s *Service) Abort(ctx context.Context, tx blobcache.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	txn, err := s.resolveTx(tx)
	if err != nil {
		return err
	}
	delete(s.txns, tx.OID)
	txn.vol.sem <- struct{}{} // Release the semaphore
	return nil
}

func (s *Service) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	txn, err := s.resolveTx(tx)
	if err != nil {
		return err
	}
	cells.CopyBytes(dst, txn.vol.root)
	return nil
}

func (s *Service) Post(ctx context.Context, tx blobcache.Handle, data []byte) (blobcache.CID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	txn, err := s.resolveTx(tx)
	if err != nil {
		return blobcache.CID{}, err
	}
	if !txn.mutating {
		return blobcache.CID{}, fmt.Errorf("read-only transaction")
	}
	cid := txn.vol.hf(data)
	txn.changeset[cid] = slices.Clone(data)
	return cid, nil
}

func (s *Service) Exists(ctx context.Context, tx blobcache.Handle, cid blobcache.CID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	txn, err := s.resolveTx(tx)
	if err != nil {
		return false, err
	}
	if data, exists := txn.changeset[cid]; exists && data == nil {
		// deleted in this tx
		return false, nil
	} else if exists {
		// modified in this tx
		return true, nil
	}
	// already in the volume
	return txn.vol.blobs[cid] != nil, nil
}

func (s *Service) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, buf []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	txn, err := s.resolveTx(tx)
	if err != nil {
		return 0, err
	}
	if data, exists := txn.changeset[cid]; exists && data == nil {
		// deleted in this tx
		return 0, cadata.ErrNotFound{Key: cid}
	} else if exists {
		// modified in this tx
		return copy(buf, data), nil
	}
	// already in the volume
	if data := txn.vol.blobs[cid]; data != nil {
		return copy(buf, data), nil
	}
	// not found
	return 0, cadata.ErrNotFound{Key: cid}
}

func (s *Service) Delete(ctx context.Context, tx blobcache.Handle, cid blobcache.CID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	txn, err := s.resolveTx(tx)
	if err != nil {
		return err
	}
	txn.changeset[cid] = nil
	return nil
}

func (s *Service) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range hs {
		s.handles[handleKey(h)] = handle{expiresAt: time.Time{}}
	}
	// current implementation of Txs does not need to be kept alive.
	return nil
}

// lockVolumes locks the volumes in a deterministic order.
// if lockVolumes suceeds then the caller must call unlockVolumes.
func lockVolumes(ctx context.Context, vols ...*volState) error {
	slices.SortFunc(vols, func(a, b *volState) int {
		return bytes.Compare(a.id[:], b.id[:])
	})

	for i, vol := range vols {
		select {
		case <-ctx.Done():
			unlockVolumes(vols[:i]...)
			return ctx.Err()
		case <-vol.sem:
		}
	}
	return nil
}

func unlockVolumes(vols ...*volState) {
	for _, vol := range vols {
		vol.sem <- struct{}{}
	}
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
