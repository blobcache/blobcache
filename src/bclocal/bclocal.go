// Package bclocal implements a local Blobcache service.
package bclocal

import (
	"context"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/volumes"
	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/tai64"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
)

const (
	DefaultVolumeTTL = 5 * time.Minute
	DefaultTxTTL     = 1 * time.Minute
)

var _ blobcache.Service = &Service{}

type Env struct {
	DB         *sqlx.DB
	PrivateKey ed25519.PrivateKey
	PacketConn net.PacketConn
	ACL        ACL
}

// Service implements a blobcache.Service.
type Service struct {
	env  Env
	db   *sqlx.DB
	node *bcnet.Node

	mu sync.RWMutex
	// handles stores ephemeral handles.
	handles map[[32]byte]handle
}

func New(env Env) *Service {
	var node *bcnet.Node
	if env.PacketConn != nil {
		node = bcnet.New(env.PrivateKey, env.PacketConn)
	}
	return &Service{
		env:  env,
		db:   env.DB,
		node: node,

		handles: make(map[[32]byte]handle),
	}
}

func (s *Service) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	if s.node != nil {
		eg.Go(func() error {
			return s.node.Serve(ctx, bcnet.Server{
				Access: func(peer blobcache.PeerID) blobcache.Service {
					return &PeerView{Peer: peer, Service: s}
				},
			})
		})
	}
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
	target    blobcache.OID
	createdAt time.Time
	expiresAt time.Time // zero value means no expiration
	rights    blobcache.Rights

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
		target:    target,
		createdAt: time.Now(),
		expiresAt: expiresAt,
		rights:    0, // TODO: set rights.

		vol: vol,
		txn: txn,
	}
	return ret
}

func (s *Service) resolveNS(ctx context.Context, h blobcache.Handle) (volumes.Volume[[]byte], error) {
	if h.OID != (blobcache.OID{}) {
		// TODO: for now root is the only valid namespace.
		return nil, fmt.Errorf("volume is not a namespace")
	}
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return ensureRootVolume(tx)
	}); err != nil {
		return nil, err
	}
	vol, err := s.resolveVol(h)
	if err != nil {
		return nil, err
	}
	return vol, nil
}

func (s *Service) resolveVol(h blobcache.Handle) (volumes.Volume[[]byte], error) {
	if h.OID == (blobcache.OID{}) {
		// this is the root namespace, so we can just return the root volume.
		return &localVolume{db: s.db, id: h.OID}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	k := handleKey(h)
	if h, exists := s.handles[k]; exists {
		return h.vol, nil
	}
	return nil, blobcache.ErrInvalidHandle{Handle: h}
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

func (s *Service) Endpoint(_ context.Context) (blobcache.Endpoint, error) {
	if s.node != nil {
		return s.node.LocalEndpoint(), nil
	}
	return blobcache.Endpoint{}, nil
}

func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, handleKey(h))
	return nil
}

func (s *Service) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hstate, exists := s.handles[handleKey(h)]
	if !exists {
		return nil, blobcache.ErrInvalidHandle{Handle: h}
	}
	return &blobcache.HandleInfo{
		OID:       h.OID,
		CreatedAt: tai64.Now(), // TODO: store creation time.
		ExpiresAt: tai64.FromGoTime(hstate.expiresAt),
	}, nil
}

func (s *Service) Open(ctx context.Context, base blobcache.Handle, name string) (*blobcache.Handle, error) {
	vol, err := s.resolveNS(ctx, base)
	if err != nil {
		return nil, err
	}
	ents, err := nsLoad(ctx, vol)
	if err != nil {
		return nil, err
	}
	if idx, found := slices.BinarySearchFunc(ents, name, func(e blobcache.Entry, name string) int {
		return strings.Compare(e.Name, name)
	}); found {
		ent := ents[idx]
		if ent.Volume == nil {
			return nil, fmt.Errorf("cannot open non-volume at %s", name)
		}
		vol, err := s.makeVolume(ctx, ent.Target, ent.Volume.Backend)
		if err != nil {
			return nil, err
		}
		handle := s.createEphemeralHandle(ent.Target, time.Now().Add(DefaultVolumeTTL), vol, nil)
		return &handle, nil
	} else {
		return nil, blobcache.ErrNoEntry{Namespace: base.OID, Name: name}
	}
}

func (s *Service) GetEntry(ctx context.Context, ns blobcache.Handle, name string) (*blobcache.Entry, error) {
	nsVol, err := s.resolveNS(ctx, ns)
	if err != nil {
		return nil, err
	}
	ents, err := nsLoad(ctx, nsVol)
	if err != nil {
		return nil, err
	}
	if e := findEntry(ents, name); e != nil {
		return e, nil
	} else {
		return nil, blobcache.ErrNoEntry{Namespace: ns.OID, Name: name}
	}
}

func (s *Service) PutEntry(ctx context.Context, ns blobcache.Handle, name string, target blobcache.Handle) error {
	nsVol, err := s.resolveNS(ctx, ns)
	if err != nil {
		return err
	}
	volInfo, err := s.InspectVolume(ctx, target)
	if err != nil {
		return err
	}
	return nsModify(ctx, nsVol, func(ents []blobcache.Entry) ([]blobcache.Entry, error) {
		// remove the entry if it already exists
		ents = slices.DeleteFunc(ents, func(e blobcache.Entry) bool {
			return e.Name == name
		})
		// add the new entry
		ents = append(ents, blobcache.Entry{
			Name:   name,
			Target: target.OID,
			Volume: volInfo,
		})
		return ents, nil
	})
}

func (s *Service) DeleteEntry(ctx context.Context, ns blobcache.Handle, name string) error {
	nsVol, err := s.resolveNS(ctx, ns)
	if err != nil {
		return err
	}
	return nsModify(ctx, nsVol, func(ents []blobcache.Entry) ([]blobcache.Entry, error) {
		ents = slices.DeleteFunc(ents, func(e blobcache.Entry) bool {
			return e.Name == name
		})
		return ents, nil
	})
}

func (s *Service) ListNames(ctx context.Context, ns blobcache.Handle) ([]string, error) {
	nsVol, err := s.resolveNS(ctx, ns)
	if err != nil {
		return nil, err
	}
	ents, err := nsLoad(ctx, nsVol)
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0, len(ents))
	for _, e := range ents {
		ret = append(ret, e.Name)
	}
	return ret, nil
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
	vol, err := s.makeVolume(ctx, *volid, info.Backend)
	if err != nil {
		return nil, err
	}
	handle := s.createEphemeralHandle(*volid, time.Now().Add(DefaultVolumeTTL), vol, nil)
	return &handle, nil
}

func (s *Service) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	vol, err := s.resolveVol(h)
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

func (s *Service) Await(ctx context.Context, cond blobcache.Conditions) error {
	panic("not implemented")
}

func (s *Service) StartSync(ctx context.Context, src blobcache.Handle, dst blobcache.Handle) error {
	panic("not implemented")
}

func (s *Service) CreateRule(ctx context.Context, rspec blobcache.RuleSpec) (*blobcache.Handle, error) {
	panic("not implemented")
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, txspec blobcache.TxParams) (*blobcache.Handle, error) {
	vol, err := s.resolveVol(volh)
	if err != nil {
		return nil, err
	}
	txn, err := vol.BeginTx(ctx, txspec)
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
func (s *Service) makeVolume(ctx context.Context, oid blobcache.OID, backend blobcache.VolumeBackend[blobcache.OID]) (volumes.Volume[[]byte], error) {
	if err := backend.Validate(); err != nil {
		return nil, err
	}
	switch {
	case backend.Local != nil:
		return &localVolume{db: s.db, id: oid}, nil
	case backend.Remote != nil:
		return bcnet.OpenVolume(ctx, s.node, backend.Remote.Endpoint, backend.Remote.Name)
	case backend.Git != nil:
		return nil, fmt.Errorf("git volumes are not yet supported")
	case backend.RootAEAD != nil:
		return s.makeRootAEAD(ctx, *backend.RootAEAD)
	case backend.Vault != nil:
		return nil, fmt.Errorf("Vault volumes are not yet supported")
	default:
		return nil, fmt.Errorf("empty backend")
	}
}

func (s *Service) makeRootAEAD(ctx context.Context, backend blobcache.VolumeBackend_RootAEAD[blobcache.OID]) (*volumes.RootAEAD, error) {
	var inner volumes.Volume[[]byte] // TODO: need to look for volumes by OID.

	var cipher cipher.AEAD
	switch backend.Algo {
	case blobcache.AEAD_CHACHA20POLY1305:
		var err error
		cipher, err = chacha20poly1305.New(backend.Secret[:])
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown AEAD algorithm: %s", backend.Algo)
	}
	return &volumes.RootAEAD{
		AEAD:  cipher,
		Inner: inner,
	}, nil
}
