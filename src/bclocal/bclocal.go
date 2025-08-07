// Package bclocal implements a local Blobcache service.
package bclocal

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"net"
	"slices"
	"sort"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/simplens"
	"blobcache.io/blobcache/src/internal/volumes"
	"github.com/cloudflare/circl/sign/ed25519"
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

	mu      sync.RWMutex
	volumes map[blobcache.OID]volume
	txns    map[blobcache.OID]volumes.Tx
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
			return
		case <-tick.C:
		}
	}
}

func (s *Service) rootVolume() volumes.Volume {
	return &localVolume{db: s.db, oid: blobcache.OID{}}
}

// mountVolume ensures the volume is available.
func (s *Service) mountVolume(ctx context.Context, oid blobcache.OID, info blobcache.VolumeInfo) error {
	s.mu.RLock()
	_, exists := s.volumes[oid]
	s.mu.RUnlock()
	if exists {
		return nil
	}
	vol, err := s.makeVolume(ctx, oid, info.Backend)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.volumes == nil {
		s.volumes = make(map[blobcache.OID]volume)
	}
	s.volumes[oid] = volume{
		info:    info,
		backend: vol,
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) mountAllVolumes(ctx context.Context, ns volumes.Namespace) error {
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return ensureRootVolume(tx)
	}); err != nil {
		return err
	}
	if err := s.mountVolume(ctx, blobcache.OID{}, rootVolumeInfo()); err != nil {
		return err
	}
	ents, err := ns.ListEntries(ctx)
	if err != nil {
		return err
	}
	topsortEntries(ents)
	for _, ent := range ents {
		if err := s.mountVolume(ctx, ent.Target, *ent.Volume); err != nil {
			return err
		}
	}
	return nil
}

type volume struct {
	info    blobcache.VolumeInfo
	backend volumes.Volume
}

type handle struct {
	target    blobcache.OID
	createdAt time.Time
	expiresAt time.Time // zero value means no expiration
	rights    blobcache.ActionSet
}

// createEphemeralHandle creates a handle that expires at the given time.
// it will be stored in memory and not in the database.
func (s *Service) createEphemeralHandle(target blobcache.OID, expiresAt time.Time) blobcache.Handle {
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
	}
	return ret
}

func (s *Service) resolveNS(ctx context.Context, h blobcache.Handle) (volumes.Namespace, blobcache.ActionSet, error) {
	if h.OID != (blobcache.OID{}) {
		// TODO: for now root is the only valid namespace.
		return nil, 0, fmt.Errorf("volume is not a namespace")
	}
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return ensureRootVolume(tx)
	}); err != nil {
		return nil, 0, err
	}
	vol, _, err := s.resolveVol(h)
	if err != nil {
		return nil, 0, err
	}
	return &simplens.Namespace{Volume: vol}, blobcache.Action_ALL, nil
}

func (s *Service) resolveVol(x blobcache.Handle) (volumes.Volume, blobcache.ActionSet, error) {
	if x.OID == (blobcache.OID{}) {
		// this is the root namespace, so we can just return the root volume.
		return &localVolume{db: s.db, oid: x.OID}, 0, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	k := handleKey(x)
	if _, exists := s.handles[k]; !exists {
		return nil, 0, blobcache.ErrInvalidHandle{Handle: x}
	}
	vol, exists := s.volumes[x.OID]
	if !exists {
		return nil, 0, fmt.Errorf("handle does not refer to volume")
	}
	return vol.backend, blobcache.Action_ALL, nil
}

// resolveTx looks up the transaction handle from memory.
// If the handle is valid it will load a new transaction.
func (s *Service) resolveTx(txh blobcache.Handle, touch bool) (volumes.Tx, error) {
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
	txn, exists := s.txns[h.target]
	if !exists {
		return nil, fmt.Errorf("handle is valid, but not for a transaction")
	}
	if touch {
		h := s.handles[handleKey(txh)]
		h.expiresAt = time.Now().Add(DefaultTxTTL)
		s.handles[handleKey(txh)] = h
	}
	return txn, nil
}

func (s *Service) inspectVolume(x blobcache.Handle) (volume, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, exists := s.handles[handleKey(x)]; !exists {
		return volume{}, blobcache.ErrInvalidHandle{Handle: x}
	}
	vol, exists := s.volumes[x.OID]
	if !exists {
		return volume{}, fmt.Errorf("handle does not refer to volume")
	}
	return vol, nil
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

func (s *Service) Open(ctx context.Context, x blobcache.OID) (*blobcache.Handle, error) {
	if err := s.mountAllVolumes(ctx, &simplens.Namespace{Volume: s.rootVolume()}); err != nil {
		return nil, err
	}
	if x != (blobcache.OID{}) {
		s.mu.RLock()
		_, exists := s.volumes[x]
		s.mu.RUnlock()
		if !exists {
			return nil, blobcache.ErrNotFound{Type: "Volume", ID: x}
		}
	}
	handle := s.createEphemeralHandle(x, time.Now().Add(DefaultVolumeTTL))
	return &handle, nil
}

func (s *Service) OpenAt(ctx context.Context, base blobcache.Handle, name string) (*blobcache.Handle, error) {
	ns, _, err := s.resolveNS(ctx, base)
	if err != nil {
		return nil, err
	}
	ent, err := ns.GetEntry(ctx, name)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, blobcache.ErrNoEntry{Namespace: base.OID, Name: name}
	}
	if err := s.mountVolume(ctx, ent.Target, *ent.Volume); err != nil {
		return nil, err
	}
	handle := s.createEphemeralHandle(ent.Target, time.Now().Add(DefaultVolumeTTL))
	return &handle, nil
}

func (s *Service) GetEntry(ctx context.Context, ns blobcache.Handle, name string) (*blobcache.Entry, error) {
	nsVol, _, err := s.resolveNS(ctx, ns)
	if err != nil {
		return nil, err
	}
	ent, err := nsVol.GetEntry(ctx, name)
	if err != nil {
		return nil, err
	}
	if ent == nil {
		return nil, blobcache.ErrNoEntry{Namespace: ns.OID, Name: name}
	}
	return ent, nil
}

func (s *Service) PutEntry(ctx context.Context, ns blobcache.Handle, name string, target blobcache.Handle) error {
	nsVol, _, err := s.resolveNS(ctx, ns)
	if err != nil {
		return err
	}
	volInfo, err := s.InspectVolume(ctx, target)
	if err != nil {
		return err
	}
	// TODO: after calling PutEntry, we need to ensure add an edge to the volumes_volumes table.
	// from the namespace volume to the target volume.
	// We only need to do this for local namespaces.
	return nsVol.PutEntry(ctx, blobcache.Entry{
		Name:   name,
		Target: target.OID,
		Volume: volInfo,
	})
}

func (s *Service) DeleteEntry(ctx context.Context, ns blobcache.Handle, name string) error {
	nsVol, _, err := s.resolveNS(ctx, ns)
	if err != nil {
		return err
	}
	// TODO: after calling DeleteEntry, we need to ensure remove the edge from the volumes_volumes table
	// if it is no longer in any of the entries.
	return nsVol.DeleteEntry(ctx, name)
}

func (s *Service) ListNames(ctx context.Context, ns blobcache.Handle) ([]string, error) {
	nsVol, _, err := s.resolveNS(ctx, ns)
	if err != nil {
		return nil, err
	}
	ents, err := nsVol.ListEntries(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name)
	}
	return names, nil
}

func (s *Service) CreateVolumeAt(ctx context.Context, ns blobcache.Handle, name string, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	nsVol, _, err := s.resolveVol(ns)
	if err != nil {
		return nil, err
	}
	switch nsVol := nsVol.(type) {
	case *bcnet.Volume:
		remoteH, err := bcnet.CreateVolumeAt(ctx, s.node, nsVol.Endpoint(), nsVol.Handle(), name, vspec)
		if err != nil {
			return nil, err
		}
		localOID := blobcache.NewOID()
		localSpec := blobcache.VolumeInfo{
			ID:           localOID,
			VolumeParams: vspec.Local.VolumeParams,
			Backend: blobcache.VolumeBackend[blobcache.OID]{
				Remote: &blobcache.VolumeBackend_Remote{
					Endpoint: nsVol.Endpoint(),
					Volume:   remoteH.OID,
				},
			},
		}
		// now we have a remote node's handle to the new volume.
		if err := s.mountVolume(ctx, localOID, localSpec); err != nil {
			return nil, err
		}
		localH := s.createEphemeralHandle(localOID, time.Now().Add(DefaultVolumeTTL))
		return &localH, nil
	default:
		// TODO: this is good enough for remote callers to create a volume, but it should be atomic.
		volh, err := s.CreateVolume(ctx, vspec)
		if err != nil {
			return nil, err
		}
		if err := s.PutEntry(ctx, ns, name, *volh); err != nil {
			return nil, err
		}
		return volh, nil
	}
}

func (s *Service) CreateVolume(ctx context.Context, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if err := vspec.Validate(); err != nil {
		return nil, err
	}
	vp, err := s.findVolumeParams(ctx, vspec)
	if err != nil {
		return nil, err
	}
	info := blobcache.VolumeInfo{
		// ID is intentionally left blank, createVolume will set it.
		VolumeParams: vp,
		Backend:      blobcache.VolumeBackendToOID(vspec),
	}
	volid, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*blobcache.OID, error) {
		return createVolume(tx, info)
	})
	if err != nil {
		return nil, err
	}
	if err := s.mountVolume(ctx, *volid, info); err != nil {
		return nil, err
	}
	handle := s.createEphemeralHandle(*volid, time.Now().Add(DefaultVolumeTTL))
	return &handle, nil
}

func (s *Service) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	vol, err := s.inspectVolume(h)
	if err != nil {
		return nil, err
	}
	return &vol.info, nil
}

func (s *Service) Await(ctx context.Context, cond blobcache.Conditions) error {
	panic("not implemented")
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, txspec blobcache.TxParams) (*blobcache.Handle, error) {
	vol, _, err := s.resolveVol(volh)
	if err != nil {
		return nil, err
	}
	txn, err := vol.BeginTx(ctx, txspec)
	if err != nil {
		return nil, err
	}

	txoid := blobcache.NewOID()
	s.mu.Lock()
	if s.txns == nil {
		s.txns = make(map[blobcache.OID]volumes.Tx)
	}
	s.txns[txoid] = txn
	s.mu.Unlock()
	h := s.createEphemeralHandle(txoid, time.Now().Add(DefaultTxTTL))
	return &h, nil
}

func (s *Service) InspectTx(ctx context.Context, txh blobcache.Handle) (*blobcache.TxInfo, error) {
	txn, err := s.resolveTx(txh, false)
	if err != nil {
		return nil, err
	}
	vol := txn.Volume()
	switch vol := vol.(type) {
	case *localVolume:
		s.mu.RLock()
		volInfo, exists := s.volumes[vol.oid]
		s.mu.RUnlock()
		if !exists {
			return nil, fmt.Errorf("volume not found")
		}
		return &blobcache.TxInfo{
			ID:       txh.OID,
			Volume:   volInfo.info.ID,
			MaxSize:  volInfo.info.MaxSize,
			HashAlgo: volInfo.info.HashAlgo,
		}, nil
	default:
		return nil, fmt.Errorf("InspectTx not implemented for volume type:%T", vol)
	}
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

// handleKey computes a map key from a handle.
func handleKey(h blobcache.Handle) [32]byte {
	return blake3.Sum256(slices.Concat(h.OID[:], h.Secret[:]))
}

// makeVolume constructs an in-memory volume object from a backend.
func (s *Service) makeVolume(ctx context.Context, oid blobcache.OID, backend blobcache.VolumeBackend[blobcache.OID]) (volumes.Volume, error) {
	if err := backend.Validate(); err != nil {
		return nil, err
	}
	switch {
	case backend.Local != nil:
		return s.makeLocal(ctx, oid)
	case backend.Remote != nil:
		return bcnet.OpenVolume(ctx, s.node, backend.Remote.Endpoint, backend.Remote.Volume)
	case backend.Git != nil:
		return s.makeGit(ctx, *backend.Git)
	case backend.RootAEAD != nil:
		return s.makeRootAEAD(ctx, *backend.RootAEAD)
	case backend.Vault != nil:
		return s.makeVault(ctx, *backend.Vault)
	default:
		return nil, fmt.Errorf("empty backend")
	}
}

func (s *Service) makeLocal(_ context.Context, oid blobcache.OID) (volumes.Volume, error) {
	return &localVolume{db: s.db, oid: oid}, nil
}

func (s *Service) makeGit(ctx context.Context, backend blobcache.VolumeBackend_Git) (volumes.Volume, error) {
	return nil, fmt.Errorf("git volumes are not yet supported")
}

func (s *Service) makeRootAEAD(ctx context.Context, backend blobcache.VolumeBackend_RootAEAD[blobcache.OID]) (*volumes.RootAEAD, error) {
	s.mu.RLock()
	volstate, exists := s.volumes[backend.Inner]
	s.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("inner volume not found: %v", backend.Inner)
	}
	inner, err := s.makeVolume(ctx, backend.Inner, volstate.info.Backend)
	if err != nil {
		return nil, err
	}

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

func (s *Service) makeVault(ctx context.Context, backend blobcache.VolumeBackend_Vault[blobcache.OID]) (*volumes.Vault, error) {
	s.mu.RLock()
	volstate, exists := s.volumes[backend.Inner]
	s.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("inner volume not found: %v", backend.Inner)
	}
	inner, err := s.makeVolume(ctx, backend.Inner, volstate.info.Backend)
	if err != nil {
		return nil, err
	}
	return volumes.NewVault(inner, backend.Secret), nil
}

func (s *Service) findVolumeParams(ctx context.Context, vspec blobcache.VolumeSpec) (blobcache.VolumeParams, error) {
	switch {
	case vspec.Local != nil:
		return vspec.Local.VolumeParams, nil
	case vspec.Git != nil:
		return vspec.Git.VolumeParams, nil

	case vspec.RootAEAD != nil:
		innerVol, err := s.inspectVolume(vspec.RootAEAD.Inner)
		if err != nil {
			return blobcache.VolumeParams{}, err
		}
		return innerVol.info.VolumeParams, nil
	case vspec.Vault != nil:
		innerVol, err := s.inspectVolume(vspec.Vault.Inner)
		if err != nil {
			return blobcache.VolumeParams{}, err
		}
		return innerVol.info.VolumeParams, nil
	case vspec.Remote != nil:
		vol, err := bcnet.OpenVolume(ctx, s.node, vspec.Remote.Endpoint, vspec.Remote.Volume)
		if err != nil {
			return blobcache.VolumeParams{}, err
		}
		volInfo := vol.Info()
		return volInfo.VolumeParams, nil
	default:
		panic(vspec)
	}
}

func topsortEntries(ents []blobcache.Entry) {
	// Build dependency graph: map from target OID to list of entries that depend on it
	deps := make(map[blobcache.OID][]int)

	// For each entry, check if it's a RootAEAD or Vault volume and add dependencies
	for i, ent := range ents {
		if ent.Volume != nil && ent.Volume.Backend.RootAEAD != nil {
			// RootAEAD depends on its inner volume
			inner := ent.Volume.Backend.RootAEAD.Inner
			deps[inner] = append(deps[inner], i)
		}
		if ent.Volume != nil && ent.Volume.Backend.Vault != nil {
			// Vault depends on its inner volume
			inner := ent.Volume.Backend.Vault.Inner
			deps[inner] = append(deps[inner], i)
		}
	}

	// Build reverse mapping: map from entry index to list of entries it depends on
	reverseDeps := make(map[int][]int)
	for inner, dependents := range deps {
		// Find the index of the inner volume
		for j, ent := range ents {
			if ent.Target == inner {
				// Each dependent depends on the inner volume
				for _, depIdx := range dependents {
					reverseDeps[depIdx] = append(reverseDeps[depIdx], j)
				}
				break
			}
		}
	}

	// Count incoming edges for each entry
	inDegree := make([]int, len(ents))
	for i := range ents {
		inDegree[i] = len(reverseDeps[i])
	}

	// Kahn's algorithm for topological sort
	var queue []int
	var result []int

	// Add all entries with no dependencies to the queue
	for i := range ents {
		if inDegree[i] == 0 {
			queue = append(queue, i)
		}
	}

	// Process the queue
	for len(queue) > 0 {
		// Take the first entry from queue
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		// Reduce in-degree for all dependents
		for _, depIdx := range deps[ents[current].Target] {
			inDegree[depIdx]--
			if inDegree[depIdx] == 0 {
				queue = append(queue, depIdx)
			}
		}
	}

	// Check for cycles (shouldn't happen with valid volume configurations)
	if len(result) != len(ents) {
		// If there's a cycle, just sort by target OID as fallback
		sort.Slice(ents, func(i, j int) bool {
			return ents[i].Target.String() < ents[j].Target.String()
		})
		return
	}

	// Reorder the entries according to the topological sort result
	sorted := make([]blobcache.Entry, len(ents))
	for i, idx := range result {
		sorted[i] = ents[idx]
	}
	copy(ents, sorted)
}
