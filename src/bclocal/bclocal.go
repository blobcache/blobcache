// Package bclocal implements a local Blobcache service.
package bclocal

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"maps"
	"net"
	"slices"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
	"go.uber.org/zap"
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
	DB *sqlx.DB
	// PrivateKey determines the node's identity.
	// It must be provided if PacketConn is set.
	PrivateKey ed25519.PrivateKey
	// PacketConn is the connection to listen on.
	PacketConn net.PacketConn
	Schemas    map[blobcache.Schema]schema.Schema
	ACL        ACL
	// Root is the spec to use for the root volume.
	Root blobcache.VolumeSpec
}

// Service implements a blobcache.Service.
type Service struct {
	env  Env
	db   *sqlx.DB
	node *bcnet.Node

	mu      sync.RWMutex
	volumes map[blobcache.OID]volume
	txns    map[blobcache.OID]transaction
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
					if s.env.ACL.Mentions(peer) {
						return s
					} else {
						return nil
					}
				},
			})
		})
	}
	eg.Go(func() error {
		s.cleanupLoop(ctx)
		return nil
	})
	return eg.Wait()
}

// Cleanup runs the full cleanup process.
func (s *Service) Cleanup(ctx context.Context) error {
	now := time.Now()
	keep := map[blobcache.OID]struct{}{}
	s.mu.Lock()
	// 1. Delete expired handles.
	for k, h := range s.handles {
		if h.expiresAt.Before(now) {
			delete(s.handles, k)
		} else {
			keep[h.target] = struct{}{}
		}
	}
	// 2. Release resources for transactions which do not have a handle.
	for oid := range s.txns {
		if _, exists := keep[oid]; !exists {
			delete(s.txns, oid)
		}
	}
	// 3. Release resources for mounted volumes which do not have a handle.
	for oid := range s.volumes {
		if _, exists := keep[oid]; !exists {
			delete(s.volumes, oid)
		}
	}
	s.mu.Unlock()

	keepSlice := slices.Collect(maps.Keys(keep))
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		// TODO: fix cleanupVolumes
		return nil
		return cleanupVolumes(tx, keepSlice)
	})
}

func (s *Service) cleanupLoop(ctx context.Context) {
	tick := time.NewTicker(300 * time.Second)
	defer tick.Stop()
	for {
		if err := s.Cleanup(ctx); err != nil {
			logctx.Error(ctx, "during cleanup", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// getSchema looks up a schema by name.
func (s *Service) getSchema(name blobcache.Schema) (schema.Schema, error) {
	schema, exists := s.env.Schemas[name]
	if !exists {
		return nil, fmt.Errorf("unknown schema %s", name)
	}
	return schema, nil
}

// getContainer looks up a container schema by name.
func (s *Service) getContainer(name blobcache.Schema) (schema.Container, error) {
	sch, err := s.getSchema(name)
	if err != nil {
		return nil, err
	}
	container, ok := sch.(schema.Container)
	if !ok {
		return nil, fmt.Errorf("found schema %s, but it is not a container", name)
	}
	return container, nil
}

func (s *Service) rootVolume() volumes.Volume {
	return newLocalVolume(s, blobcache.OID{})
}

func (s *Service) handleToLink(x blobcache.Handle) (*schema.Link, error) {
	_, rights, err := s.resolveVol(x)
	if err != nil {
		return nil, err
	}
	return &schema.Link{Target: x.OID, Rights: rights}, nil
}

// mountVolume ensures the volume is available.
// if the volume is already in memory, it does nothing.
// otherwise it calls makeVolume and writes to the volumes map.
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

// mountAllInContainer reads the links from the container using the provided container schema.
// Next it mounts all of those volumes.
// allowedLinks is used to filter out illegitimate links produced by the container.
func (s *Service) mountAllInContainer(ctx context.Context, sch schema.Container, contVol volumes.Volume, allowedLinks map[blobcache.OID]blobcache.ActionSet) error {
	txn, err := contVol.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer txn.Abort(ctx)
	src, root, err := volumes.ViewUnsalted(ctx, txn)
	if err != nil {
		return err
	}
	links := make(map[blobcache.OID]blobcache.ActionSet)
	if err := sch.ReadLinks(ctx, src, root, links); err != nil {
		return err
	}
	// constrain according to allowedLinks
	for target, rights := range links {
		links[target] |= allowedLinks[target] & rights
		if links[target] == 0 {
			delete(links, target)
		}
	}
	for target := range links {
		volInfo, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*blobcache.VolumeInfo, error) {
			return inspectVolume(tx, target)
		})
		if err != nil {
			return err
		}
		if err := s.mountVolume(ctx, target, *volInfo); err != nil {
			return err
		}
	}
	return nil
}

// mountRoot ensures that the root volume is mounted.
// First it ensures that the root volume is in the database.
// Then is calls mount volume using the root volume info (which is constant).
// Then is calls mountAllInContainer on the root volume.
func (s *Service) mountRoot(ctx context.Context) error {
	s.mu.RLock()
	_, exists := s.volumes[blobcache.OID{}]
	s.mu.RUnlock()
	if exists {
		return nil
	}

	rootOID := blobcache.OID{}
	allowedLinks := make(map[blobcache.OID]blobcache.ActionSet)
	volInfo, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*blobcache.VolumeInfo, error) {
		clear(allowedLinks)
		if err := readVolumeLinks(tx, rootOID, allowedLinks); err != nil {
			return nil, err
		}
		return ensureRootVolume(tx, s.env.Root)
	})
	if err != nil {
		return err
	}

	if err := s.mountVolume(ctx, rootOID, *volInfo); err != nil {
		return err
	}
	container, err := s.getContainer(volInfo.Schema)
	if err != nil {
		return err
	}
	return s.mountAllInContainer(ctx, container, s.rootVolume(), allowedLinks)
}

type volume struct {
	info    blobcache.VolumeInfo
	backend volumes.Volume
}

type transaction struct {
	backend volumes.Tx
	volume  *volume
}

type handle struct {
	target    blobcache.OID
	createdAt time.Time
	expiresAt time.Time // zero value means no expiration
	rights    blobcache.ActionSet
}

func (h handle) isExpired(now time.Time) bool {
	return h.expiresAt.Before(now)
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
		rights:    blobcache.Action_ALL, // TODO: set rights.
	}
	return ret
}

func (s *Service) resolveVol(x blobcache.Handle) (volume, blobcache.ActionSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	k := handleKey(x)
	if _, exists := s.handles[k]; !exists {
		return volume{}, 0, blobcache.ErrInvalidHandle{Handle: x}
	}
	vol, exists := s.volumes[x.OID]
	if !exists {
		return volume{}, 0, fmt.Errorf("handle does not refer to volume")
	}
	return vol, blobcache.Action_ALL, nil
}

// resolveTx looks up the transaction handle from memory.
// If the handle is valid it will load a new transaction.
func (s *Service) resolveTx(txh blobcache.Handle, touch bool) (transaction, error) {
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
		return transaction{}, blobcache.ErrInvalidHandle{Handle: txh}
	}
	tx, exists := s.txns[h.target]
	if !exists {
		return transaction{}, fmt.Errorf("handle is valid, but not for a transaction")
	}
	if touch {
		h := s.handles[handleKey(txh)]
		h.expiresAt = time.Now().Add(DefaultTxTTL)
		s.handles[handleKey(txh)] = h
	}
	return tx, nil
}

func (s *Service) Endpoint(_ context.Context) (blobcache.Endpoint, error) {
	if s.node != nil {
		return s.node.LocalEndpoint(), nil
	}
	return blobcache.Endpoint{}, nil
}

func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	delete(s.handles, handleKey(h))
	s.mu.Unlock()
	return s.Cleanup(ctx)
}

func (s *Service) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range hs {
		if hstate, exists := s.handles[handleKey(h)]; !exists {
			continue
		} else {
			if hstate.isExpired(now) {
				delete(s.handles, handleKey(h))
				continue
			}
		}
		var ttl time.Duration
		if _, exists := s.volumes[h.OID]; exists {
			ttl = DefaultVolumeTTL
		}
		if _, exists := s.txns[h.OID]; exists {
			ttl = DefaultTxTTL
		}
		s.handles[handleKey(h)] = handle{
			expiresAt: now.Add(ttl),
		}
	}
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
		CreatedAt: tai64.Now().TAI64(), // TODO: store creation time.
		ExpiresAt: tai64.FromGoTime(hstate.expiresAt).TAI64(),
	}, nil
}

func (s *Service) OpenAs(ctx context.Context, caller *blobcache.PeerID, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	if caller != nil {
		if !slices.Contains(s.env.ACL.Owners, *caller) {
			return nil, ErrNotAllowed{
				Peer:   *caller,
				Action: "OpenAs",
				Target: x,
			}
		}
	}
	if err := s.mountRoot(ctx); err != nil {
		return nil, err
	}
	h := s.createEphemeralHandle(x, time.Now().Add(DefaultVolumeTTL))
	return &h, nil
}

func (s *Service) OpenFrom(ctx context.Context, base blobcache.Handle, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	if err := s.mountRoot(ctx); err != nil {
		return nil, err
	}
	baseVol, _, err := s.resolveVol(base)
	if err != nil {
		return nil, err
	}
	txn, err := baseVol.backend.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer txn.Abort(ctx)

	links := make(map[blobcache.OID]blobcache.ActionSet)
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return readVolumeLinks(tx, base.OID, links)
	}); err != nil {
		return nil, err
	}
	if links[x] == 0 {
		return nil, blobcache.ErrNoLink{Base: base.OID, Target: x}
	}

	for target := range links {
		volInfo, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*blobcache.VolumeInfo, error) {
			return inspectVolume(tx, target)
		})
		if err != nil {
			return nil, err
		}
		if err := s.mountVolume(ctx, target, *volInfo); err != nil {
			return nil, err
		}
	}
	_ = links // TODO: use links to check access to x
	h := s.createEphemeralHandle(x, time.Now().Add(DefaultVolumeTTL))
	return &h, nil
}

func (s *Service) CreateVolume(ctx context.Context, caller *blobcache.PeerID, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if caller != nil {
		if !slices.Contains(s.env.ACL.Owners, *caller) {
			return nil, ErrNotAllowed{
				Peer:   *caller,
				Action: "CreateVolume",
			}
		}
	}

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
	info.ID = *volid
	if err := s.mountVolume(ctx, *volid, info); err != nil {
		return nil, err
	}
	handle := s.createEphemeralHandle(*volid, time.Now().Add(DefaultVolumeTTL))
	return &handle, nil
}

func (s *Service) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	vol, _, err := s.resolveVol(h)
	if err != nil {
		return nil, err
	}
	return &vol.info, nil
}

func (s *Service) Await(ctx context.Context, cond blobcache.Conditions) error {
	return fmt.Errorf("Await not implemented")
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, txspec blobcache.TxParams) (*blobcache.Handle, error) {
	if err := s.mountRoot(ctx); err != nil {
		return nil, err
	}
	vol, _, err := s.resolveVol(volh)
	if err != nil {
		return nil, err
	}
	tx, err := vol.backend.BeginTx(ctx, txspec)
	if err != nil {
		return nil, err
	}

	txoid := blobcache.NewOID()
	s.mu.Lock()
	if s.txns == nil {
		s.txns = make(map[blobcache.OID]transaction)
	}
	s.txns[txoid] = transaction{
		backend: tx,
		volume:  &vol,
	}
	s.mu.Unlock()
	h := s.createEphemeralHandle(txoid, time.Now().Add(DefaultTxTTL))
	return &h, nil
}

func (s *Service) InspectTx(ctx context.Context, txh blobcache.Handle) (*blobcache.TxInfo, error) {
	txn, err := s.resolveTx(txh, false)
	if err != nil {
		return nil, err
	}
	vol := txn.volume.backend
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

func (s *Service) Save(ctx context.Context, txh blobcache.Handle, root []byte) error {
	tx, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	// validate against the schema.
	var prevRoot []byte
	if err := tx.backend.Load(ctx, &prevRoot); err != nil {
		return err
	}
	src := volumes.NewUnsaltedStore(tx.backend)
	sch, err := s.getSchema(tx.volume.info.Schema)
	if err != nil {
		return err
	}
	if err := sch.Validate(ctx, src, prevRoot, root); err != nil {
		return err
	}
	return tx.backend.Save(ctx, root)
}

func (s *Service) Commit(ctx context.Context, txh blobcache.Handle) error {
	tx, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	if err := tx.backend.Commit(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.handles, handleKey(txh))
	s.mu.Unlock()
	return nil
}

func (s *Service) Abort(ctx context.Context, txh blobcache.Handle) error {
	txn, err := s.resolveTx(txh, false)
	if err != nil {
		return err
	}
	if err := txn.backend.Abort(ctx); err != nil {
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
	return txn.backend.Load(ctx, dst)
}

func (s *Service) Post(ctx context.Context, txh blobcache.Handle, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return blobcache.CID{}, err
	}
	return txn.backend.Post(ctx, salt, data)
}

func (s *Service) Exists(ctx context.Context, txh blobcache.Handle, cid blobcache.CID) (bool, error) {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return false, err
	}
	return txn.backend.Exists(ctx, cid)
}

func (s *Service) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return 0, err
	}
	return txn.backend.Get(ctx, cid, salt, buf)
}

func (s *Service) Delete(ctx context.Context, txh blobcache.Handle, cid blobcache.CID) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.backend.Delete(ctx, cid)
}

func (s *Service) AllowLink(ctx context.Context, txh blobcache.Handle, subvolh blobcache.Handle) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.backend.AllowLink(ctx, subvolh)
}

// handleKey computes a map key from a handle.
func handleKey(h blobcache.Handle) [32]byte {
	return blake3.Sum256(slices.Concat(h.OID[:], h.Secret[:]))
}

// makeVolume constructs an in-memory volume object from a backend.
// it does not create volumes in the database.
func (s *Service) makeVolume(ctx context.Context, oid blobcache.OID, backend blobcache.VolumeBackend[blobcache.OID]) (volumes.Volume, error) {
	if err := backend.Validate(); err != nil {
		return nil, err
	}
	switch {
	case backend.Local != nil:
		return s.makeLocal(ctx, oid)
	case backend.Remote != nil:
		return bcnet.OpenVolumeAs(ctx, s.node, backend.Remote.Endpoint, backend.Remote.Volume, blobcache.Action_ALL)
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
	return newLocalVolume(s, oid), nil
}

func (s *Service) makeGit(ctx context.Context, backend blobcache.VolumeBackend_Git) (volumes.Volume, error) {
	return nil, fmt.Errorf("git volumes are not yet supported")
}

func (s *Service) makeRootAEAD(ctx context.Context, backend blobcache.VolumeBackend_RootAEAD[blobcache.OID]) (*volumes.RootAEADVolume, error) {
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
	return &volumes.RootAEADVolume{
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
		innerVol, _, err := s.resolveVol(vspec.RootAEAD.Inner)
		if err != nil {
			return blobcache.VolumeParams{}, err
		}
		return innerVol.info.VolumeParams, nil
	case vspec.Vault != nil:
		innerVol, _, err := s.resolveVol(vspec.Vault.Inner)
		if err != nil {
			return blobcache.VolumeParams{}, err
		}
		return innerVol.info.VolumeParams, nil
	case vspec.Remote != nil:
		vol, err := bcnet.OpenVolumeAs(ctx, s.node, vspec.Remote.Endpoint, vspec.Remote.Volume, blobcache.Action_ALL)
		if err != nil {
			return blobcache.VolumeParams{}, err
		}
		volInfo := vol.Info()
		return volInfo.VolumeParams, nil
	default:
		panic(vspec)
	}
}
