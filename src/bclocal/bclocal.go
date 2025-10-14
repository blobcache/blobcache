// Package bclocal implements a local Blobcache service.
package bclocal

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
	"lukechampine.com/blake3"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/svcgroup"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
)

const (
	// DefaultVolumeTTL is the default time to live for a volume handle.
	DefaultVolumeTTL = 5 * time.Minute
	// DefaultTxTTL is the default time to live for a transaction handle.
	DefaultTxTTL = 1 * time.Minute

	// MaxMaxBlobSize is the maximum value that a Volume's max size can be set to.
	MaxMaxBlobSize = 1 << 24
)

var _ blobcache.Service = &Service{}

type Env struct {
	Background context.Context
	// StateDir is the directory where all the state is stored.
	StateDir string
	// PrivateKey determines the node's identity.
	// It must be provided if PacketConn is set.
	PrivateKey ed25519.PrivateKey
	// Schemas is the supported schemas.
	Schemas map[blobcache.SchemaName]schema.Constructor
	// Root is the spec to use for the root volume.
	Root blobcache.VolumeSpec
	// Policy control network access to the service.
	Policy Policy
}

// Config contains configuration for the service.
// Config{} works fine.
// You don't need to worry about anything in here.
type Config struct {
	// When set to true, the service will not sync the database and blob directory.
	NoSync bool
}

// Service implements a blobcache.Service.
type Service struct {
	env Env
	cfg Config

	db      *pebble.DB
	blobDir *os.Root

	node     atomic.Pointer[bcnet.Node]
	svcs     svcgroup.Group
	handles  handleSystem
	localSys localSystem

	// mu guards the volumes and txns.
	// pure handle operations like Drop, KeepAlive, Inspect, etc. do not require this lock.
	// mu should always be taken for a superset of the time that the handle system's lock is taken.
	mu      sync.RWMutex
	volumes map[blobcache.OID]volume
	txns    map[blobcache.OID]transaction
}

func New(env Env, cfg Config) (*Service, error) {
	if env.Background == nil {
		return nil, fmt.Errorf("bclocal.New: Background cannot be nil")
	}
	if env.StateDir == "" {
		return nil, fmt.Errorf("bclocal.New: StateDir cannot be empty")
	}
	dbPath := filepath.Join(env.StateDir, "pebble")
	blobDirPath := filepath.Join(env.StateDir, "blob")
	for _, dir := range []string{dbPath, blobDirPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	blobDir, err := os.OpenRoot(blobDirPath)
	if err != nil {
		return nil, err
	}

	s := &Service{
		env: env,
		cfg: cfg,

		db:      db,
		blobDir: blobDir,
		svcs:    svcgroup.New(env.Background),
	}

	s.localSys = newLocalSystem(cfg, db, blobDir, &s.handles, func(spec blobcache.SchemaSpec) (schema.Schema, error) {
		factory := env.Schemas[spec.Name]
		if factory == nil {
			return nil, fmt.Errorf("unknown schema %s", spec.Name)
		}
		return factory(spec.Params, s.getSchema)
	})
	s.svcs.Always(func(ctx context.Context) error {
		s.cleanupLoop(ctx)
		return nil
	})

	return s, nil
}

func (s *Service) Close() error {
	ctx := context.TODO()
	// stop background services.
	s.svcs.Shutdown()
	return errors.Join(
		// abort all transactions.
		s.AbortAll(ctx),
		// flush the blobs to disk.
		func() error {
			if !s.cfg.NoSync {
				return s.localSys.blobs.Flush()
			}
			return nil
		}(),
		// close the blob directory.
		s.blobDir.Close(),
		// close the database.
		s.db.Close(),
	)
}

// Serve handles requests from the network.
// Serve blocks untilt the context is cancelled, or Close is called.
// Cancelling the context will cause Run to return without an error.
// If Serve is *not* running, then remote volumes will not work, hosted on this Node or other Nodes.
func (s *Service) Serve(ctx context.Context, pc net.PacketConn) error {
	node := bcnet.New(s.env.PrivateKey, pc)
	s.node.Store(node)

	err := node.Serve(ctx, bcnet.Server{
		Access: func(peer blobcache.PeerID) blobcache.Service {
			if s.env.Policy.CanConnect(peer) {
				return &peerView{Service: s, Caller: peer}
			} else {
				return nil
			}
		},
	})
	if errors.Is(err, net.ErrClosed) {
		err = nil
	} else if errors.Is(err, context.Canceled) {
		err = nil
	}
	return err
}

func (s *Service) LocalID() blobcache.PeerID {
	return inet256.NewID(s.env.PrivateKey.Public().(inet256.PublicKey))
}

// AbortAll aborts all transactions.
func (s *Service) AbortAll(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for oid, txn := range s.txns {
		if err := txn.backend.Abort(ctx); err != nil {
			logctx.Warn(ctx, "aborting transaction", zap.Error(err))
		}
		s.handles.DropAllForOID(oid)
		delete(s.txns, oid)
	}
	return nil
}

// Cleanup runs the full cleanup process.
// This method is called periodically by Run, but it can also be called manually.
func (s *Service) Cleanup(ctx context.Context) error {
	logctx.Info(ctx, "cleanup BEGIN")
	defer logctx.Info(ctx, "cleanup END")
	now := time.Now()

	// 1. Delete expired handles.
	logctx.Info(ctx, "cleaning up handles")
	s.handles.filter(func(h handle) bool {
		// return true to keep, false to delete.
		return h.expiresAt.After(now)
	})

	// 2. Release resources for transactions which do not have a handle.
	logctx.Info(ctx, "cleaning up transactions")
	s.mu.Lock()
	for oid := range s.txns {
		if !s.handles.isAlive(oid) {
			delete(s.txns, oid)
		}
	}

	// 3. Release resources for mounted volumes which do not have a handle.
	logctx.Info(ctx, "cleaning up volumes")
	for oid := range s.volumes {
		if !s.handles.isAlive(oid) {
			delete(s.volumes, oid)
		}
	}
	s.mu.Unlock()

	// cleanup volumes.
	if err := cleanupVolumes(ctx, s.db, func(oid blobcache.OID) bool {
		if oid == (blobcache.OID{}) {
			return true
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		_, exists := s.volumes[oid]
		return exists
	}); err != nil {
		return err
	}

	// TODO: cleanup database.
	return nil
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
func (s *Service) getSchema(spec blobcache.SchemaSpec) (schema.Schema, error) {
	schema, exists := s.env.Schemas[spec.Name]
	if !exists {
		return nil, fmt.Errorf("unknown schema %s", spec.Name)
	}
	return schema(spec.Params, s.getSchema)
}

// getContainer looks up a container schema by name.
func (s *Service) getContainer(spec blobcache.SchemaSpec) (schema.Container, error) {
	sch, err := s.getSchema(spec)
	if err != nil {
		return nil, err
	}
	container, ok := sch.(schema.Container)
	if !ok {
		return nil, fmt.Errorf("found schema %s, but it is not a container", spec.Name)
	}
	return container, nil
}

func (s *Service) rootVolume() volumes.Volume {
	return newLocalVolume(&s.localSys, 0)
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
		links[target] = allowedLinks[target] & rights
		if links[target] == 0 {
			delete(links, target)
		}
	}
	for target := range links {
		volInfo, err := inspectVolume(s.db, target)
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
	var volInfo *blobcache.VolumeInfo
	if err := doRWBatch(s.db, func(ba *pebble.Batch) error {
		clear(allowedLinks)
		if err := readVolumeLinks(ba, rootOID, allowedLinks); err != nil {
			return err
		}
		var err error
		volInfo, err = ensureRootVolume(ba, s.env.Root)
		return err
	}); err != nil {
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

func (s *Service) resolveVol(x blobcache.Handle) (volume, blobcache.ActionSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	oid, rights := s.handles.Resolve(x)
	if rights == 0 {
		return volume{}, 0, blobcache.ErrInvalidHandle{Handle: x}
	}
	vol, exists := s.volumes[oid]
	if !exists {
		return volume{}, 0, fmt.Errorf("handle does not refer to volume")
	}
	return vol, rights, nil
}

// resolveTx looks up the transaction handle from memory.
// If the handle is valid it will load a new transaction.
func (s *Service) resolveTx(txh blobcache.Handle, touch bool) (transaction, error) {
	oid, rights := s.handles.Resolve(txh)
	if rights == 0 {
		return transaction{}, blobcache.ErrInvalidHandle{Handle: txh}
	}
	// transactions are not stored in the database, so we only have to check the handles map.
	tx, exists := s.txns[oid]
	if !exists {
		return transaction{}, blobcache.ErrInvalidHandle{Handle: txh}
	}
	if touch {
		s.handles.KeepAlive(txh, time.Now().Add(DefaultTxTTL))
	}
	return tx, nil
}

// Endpoint blocks waiting for a node to be created (happens when Serve is running).
// And then returns the Endpoint for that Node.
func (s *Service) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	ctx, cf := context.WithTimeout(ctx, time.Second)
	defer cf()
	var node *bcnet.Node
	for ; node == nil; node = s.node.Load() {
		select {
		case <-ctx.Done():
			return blobcache.Endpoint{}, fmt.Errorf("waiting for node to come online: %w", ctx.Err())
		default:
		}
	}
	return node.LocalEndpoint(), nil
}

func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	s.handles.Drop(h)
	s.mu.Unlock()
	return s.Cleanup(ctx)
}

func (s *Service) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	now := time.Now()
	volExpire := now.Add(DefaultVolumeTTL)
	txExpire := now.Add(DefaultTxTTL)

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, h := range hs {
		_, exists := s.handles.Inspect(h)
		if !exists {
			continue
		}
		var expiresAt time.Time
		if _, exists := s.volumes[h.OID]; exists {
			expiresAt = volExpire
		}
		if _, exists := s.txns[h.OID]; exists {
			expiresAt = txExpire
		}
		s.handles.KeepAlive(h, expiresAt)
	}
	return nil
}

func (s *Service) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return nil, fmt.Errorf("Share not implemented")
}

func (s *Service) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	hstate, exists := s.handles.Inspect(h)
	if !exists {
		return nil, blobcache.ErrInvalidHandle{Handle: h}
	}
	return &blobcache.HandleInfo{
		OID:       h.OID,
		CreatedAt: tai64.Now().TAI64(), // TODO: store creation time.
		ExpiresAt: tai64.FromGoTime(hstate.expiresAt).TAI64(),
	}, nil
}

func (s *Service) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	if err := s.mountRoot(ctx); err != nil {
		return nil, err
	}
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultVolumeTTL)
	h := s.handles.Create(x, mask, createdAt, expiresAt)
	return &h, nil
}

func (s *Service) OpenFrom(ctx context.Context, base blobcache.Handle, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	if err := s.mountRoot(ctx); err != nil {
		return nil, err
	}
	_, _, err := s.resolveVol(base)
	if err != nil {
		return nil, err
	}

	links := make(map[blobcache.OID]blobcache.ActionSet)
	if err := doSnapshot(s.db, func(sp *pebble.Snapshot) error {
		return readVolumeLinks(sp, base.OID, links)
	}); err != nil {
		return nil, err
	}
	if links[x] == 0 {
		return nil, blobcache.ErrNoLink{Base: base.OID, Target: x}
	}

	sp := s.db.NewSnapshot()
	defer sp.Close()
	for target := range links {
		volInfo, err := inspectVolume(sp, target)
		if err != nil {
			return nil, err
		}
		if err := s.mountVolume(ctx, target, *volInfo); err != nil {
			return nil, err
		}
	}

	rights := links[x] & mask
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultVolumeTTL)
	h := s.handles.Create(x, rights, createdAt, expiresAt)
	return &h, nil
}

func (s *Service) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if err := vspec.Validate(); err != nil {
		return nil, err
	}

	node := s.node.Load()
	if host != nil && node != nil {
		return nil, fmt.Errorf("bclocal: node is not running. Cannot call CreateVolume with a non-nil host")
	} else if host != nil && host.Peer != node.LocalID() {
		return s.createRemoteVolume(ctx, *host, vspec)
	}

	vp, err := s.findVolumeParams(ctx, vspec)
	if err != nil {
		return nil, err
	}
	if vp.MaxSize > MaxMaxBlobSize {
		return nil, fmt.Errorf("bclocal: only supports blobs up to %d, requested %d", MaxMaxBlobSize, vp.MaxSize)
	}
	lvid, err := s.localSys.GenerateLocalID()
	if err != nil {
		return nil, err
	}
	oid := oidFromLocalID(lvid)
	info := blobcache.VolumeInfo{
		ID:           oid,
		VolumeParams: vp,
		Backend:      blobcache.VolumeBackendToOID(vspec),
	}

	if err := doRWBatch(s.db, func(ba *pebble.Batch) error {
		return putVolume(ba, info)
	}); err != nil {
		return nil, err
	}
	if err := s.mountVolume(ctx, info.ID, info); err != nil {
		return nil, err
	}

	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultVolumeTTL)
	handle := s.handles.Create(info.ID, blobcache.Action_ALL, createdAt, expiresAt)
	return &handle, nil
}

// createRemoteVolume calls CreateVolume on a remote node
func (s *Service) createRemoteVolume(ctx context.Context, host blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	node := s.node.Load()
	// the request is for a remote node.
	rvh, err := bcnet.CreateVolume(ctx, node, host, vspec)
	if err != nil {
		return nil, err
	}
	rvInfo, err := bcnet.InspectVolume(ctx, node, host, *rvh)
	if err != nil {
		return nil, err
	}
	vp, err := s.findVolumeParams(ctx, vspec)
	if err != nil {
		return nil, err
	}
	// now we have a handle to the remote volume.
	// The handle is only valid on the remote node.
	localInfo := blobcache.VolumeInfo{
		ID: blobcache.RandomOID(),
		Backend: blobcache.VolumeBackend[blobcache.OID]{
			Remote: &blobcache.VolumeBackend_Remote{
				Endpoint: host,
				Volume:   blobcache.RandomOID(),
				HashAlgo: vp.HashAlgo,
			},
		},
	}
	// insert into volumes
	s.mu.Lock()
	s.volumes[localInfo.ID] = volume{
		info:    localInfo,
		backend: bcnet.NewVolume(node, host, *rvh, rvInfo),
	}
	s.mu.Unlock()
	// create a local handle
	localHandle := s.handles.Create(localInfo.ID, blobcache.Action_ALL, time.Now(), time.Now().Add(DefaultVolumeTTL))
	return &localHandle, nil

}

func (s *Service) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	vol, _, err := s.resolveVol(volh)
	if err != nil {
		return nil, err
	}
	if vol.info.Backend.Local == nil {
		return nil, fmt.Errorf("only local volumes can be cloned")
	}

	ba := s.db.NewIndexedBatch()
	defer ba.Close()
	vinfo, err := inspectVolume(ba, vol.info.ID)
	if err != nil {
		return nil, err
	}
	if vinfo == nil {
		return nil, fmt.Errorf("volume not found")
	}
	vinfo.ID = blobcache.RandomOID()
	if err := putVolume(ba, *vinfo); err != nil {
		return nil, err
	}
	if err := ba.Commit(nil); err != nil {
		return nil, err
	}

	h := s.handles.Create(vol.info.ID, blobcache.Action_ALL, time.Now(), time.Now().Add(DefaultVolumeTTL))
	return &h, nil
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

	txoid := blobcache.RandomOID()
	s.mu.Lock()
	if s.txns == nil {
		s.txns = make(map[blobcache.OID]transaction)
	}
	s.txns[txoid] = transaction{
		backend: tx,
		volume:  &vol,
	}
	s.mu.Unlock()
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultTxTTL)
	h := s.handles.Create(txoid, blobcache.Action_ALL, createdAt, expiresAt)
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
		volInfo, exists := s.volumes[oidFromLocalID(vol.lvid)]
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
	case *volumes.Vault:
		// For RootAEAD, report the underlying volume's params
		innerVol := vol.Inner
		switch inner := innerVol.(type) {
		case *localVolume:
			s.mu.RLock()
			volInfo, exists := s.volumes[oidFromLocalID(inner.lvid)]
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
			return nil, fmt.Errorf("InspectTx not implemented for inner volume type:%T", inner)
		}
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
	if err := sch.ValidateChange(ctx, src, prevRoot, root); err != nil {
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
	s.handles.Drop(txh)
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
	s.handles.Drop(txh)
	return nil
}

func (s *Service) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.backend.Load(ctx, dst)
}

func (s *Service) Post(ctx context.Context, txh blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return blobcache.CID{}, err
	}
	return txn.backend.Post(ctx, data, opts)
}

func (s *Service) Exists(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.backend.Exists(ctx, cids, dst)
}

func (s *Service) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return 0, err
	}
	n, err := txn.backend.Get(ctx, cid, buf, opts)
	if err != nil {
		return 0, err
	}
	if !opts.SkipVerify {
		cid2 := txn.backend.Hash(opts.Salt, buf[:n])
		if cid2 != cid {
			return -1, blobcache.ErrBadData{
				Salt:     opts.Salt,
				Expected: cid,
				Actual:   cid2,
				Len:      n,
			}
		}
	}
	return n, nil
}

func (s *Service) Delete(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.backend.Delete(ctx, cids)
}

func (s *Service) Copy(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, srcTxns []blobcache.Handle, out []bool) error {
	if len(cids) != len(out) {
		return fmt.Errorf("cids and out must have the same length")
	}
	_, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	// for now, we just return false for all cids.
	// This is an allowed behavior, the caller can always fallback to Post.
	ret := make([]bool, len(cids))
	for i := range cids {
		ret[i] = false
	}
	return nil
}

func (s *Service) AllowLink(ctx context.Context, txh blobcache.Handle, subvolh blobcache.Handle) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.backend.AllowLink(ctx, subvolh)
}

func (s *Service) Visit(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.backend.Visit(ctx, cids)
}

func (s *Service) IsVisited(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and out must have the same length")
	}
	txn, err := s.resolveTx(txh, true)
	if err != nil {
		return err
	}
	return txn.backend.IsVisited(ctx, cids, dst)
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
		lvid, err := localVolumeIDFromOID(oid)
		if err != nil {
			return nil, err
		}
		return s.makeLocal(ctx, lvid)
	case backend.Remote != nil:
		node := s.node.Load()
		return bcnet.OpenVolumeAs(ctx, node, backend.Remote.Endpoint, backend.Remote.Volume, blobcache.Action_ALL)
	case backend.Git != nil:
		return s.makeGit(ctx, *backend.Git)
	case backend.Vault != nil:
		return s.makeVault(ctx, *backend.Vault)
	default:
		return nil, fmt.Errorf("empty backend")
	}
}

func (s *Service) makeLocal(_ context.Context, lvid LocalVolumeID) (volumes.Volume, error) {
	return s.localSys.Open(lvid)
}

func (s *Service) makeGit(ctx context.Context, backend blobcache.VolumeBackend_Git) (volumes.Volume, error) {
	return nil, fmt.Errorf("git volumes are not yet supported")
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
	case vspec.Remote != nil:
		node := s.node.Load()
		vol, err := bcnet.OpenVolumeAs(ctx, node, vspec.Remote.Endpoint, vspec.Remote.Volume, blobcache.Action_ALL)
		if err != nil {
			return blobcache.VolumeParams{}, err
		}
		volInfo := vol.Info()
		return volInfo.VolumeParams, nil

	case vspec.Vault != nil:
		innerVol, _, err := s.resolveVol(vspec.Vault.Inner)
		if err != nil {
			return blobcache.VolumeParams{}, err
		}
		return innerVol.info.VolumeParams, nil
	default:
		panic(vspec)
	}
}
