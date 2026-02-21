package bcsys

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	consensusvol "blobcache.io/blobcache/src/internal/backend/consensusbe"
	"blobcache.io/blobcache/src/internal/backend/memory"
	"blobcache.io/blobcache/src/internal/backend/remotebe"
	"blobcache.io/blobcache/src/internal/backend/vaultvol"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/schema"
	"github.com/cloudflare/circl/sign/ed25519"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.brendoncarroll.net/tai64"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
)

const (
	// DefaultVolumeTTL is the default time to live for a volume handle.
	DefaultVolumeTTL = 5 * time.Minute
	// DefaultTxTTL is the default time to live for a transaction handle.
	DefaultTxTTL = 1 * time.Minute
	// DefaultQueueTTL is the default time to live for a queue handle.
	DefaultQueueTTL = 2 * time.Minute
)

type LocalVolume[K any] interface {
	backend.Volume
}

type LocalQueue interface {
	backend.Queue
}

// LVParams are the parameters for a local volume
type LVParams[K any] struct {
	Key    K
	Params blobcache.VolumeConfig
}

// PeerLocator finds the address of peers
type PeerLocator interface {
	WhereIs(blobcache.PeerID) []netip.AddrPort
}

type Env[LK any, LV LocalVolume[LK], LQ LocalQueue] struct {
	Background context.Context
	PrivateKey ed25519.PrivateKey
	// Root is the root volume.
	// It will have the all-zero OID.
	Root backend.Volume
	// MDS is where Volume metadata is stored.
	MDS         MetadataStore
	Policy      Policy
	PeerLocator PeerLocator
	MkSchema    schema.Factory

	// Local is the local backend system.
	Local      backend.System[LVParams[LK], LV, blobcache.QueueBackend_Memory, LQ]
	GenerateLK func() (LK, error)
	LKToOID    func(LK) blobcache.OID
	OIDToLK    func(blobcache.OID) (LK, error)
}

type Config struct {
	// MaxMaxBlobSize is the maximum size of the MaxSize volume parameter, for any local volume.
	MaxMaxBlobSize int64
}

func New[LK any, LV LocalVolume[LK], LQ LocalQueue](env Env[LK, LV, LQ], cfg Config) *Service[LK, LV, LQ] {
	var tmpSecret [32]byte
	if _, err := rand.Read(tmpSecret[:]); err != nil {
		panic(err)
	}
	s := &Service[LK, LV, LQ]{
		env: env,
		cfg: cfg,

		node:      atomic.Pointer[bcnet.Node]{},
		tmpSecret: &tmpSecret,
	}
	s.backends.local = env.Local
	s.backends.remote = remotebe.New(&s.node)
	s.backends.global = consensusvol.New(consensusvol.Env{
		Background: s.env.Background,
	})
	s.queueSys.memory = &memory.System{}
	return s
}

type Service[LK any, LV LocalVolume[LK], LQ LocalQueue] struct {
	env Env[LK, LV, LQ]
	cfg Config

	node     atomic.Pointer[bcnet.Node]
	backends struct {
		local  backend.VolumeSystem[LVParams[LK], LV]
		remote remotebe.System
		global consensusvol.System
		memory memory.System
	}
	queueSys struct {
		memory *memory.System
	}
	tmpSecret *[32]byte

	handles handleSystem
	// mu guards the volumes, queues, and txns.
	// pure handle operations like Drop, KeepAlive, Inspect, etc. do not require this lock.
	// mu should always be taken for a superset of the time that the handle system's lock is taken.
	mu      sync.RWMutex
	volumes map[blobcache.OID]volume
	queues  map[blobcache.OID]queue
	txns    map[blobcache.OID]transaction
}

// Serve handles requests from the network.
// Serve blocks untilt the context is cancelled, or Close is called.
// Cancelling the context will cause Run to return without an error.
// If Serve is *not* running, then remote volumes will not work, hosted on this Node or other Nodes.
func (s *Service[LK, LV, LQ]) Serve(ctx context.Context, pc net.PacketConn) error {
	node := bcnet.New(s.env.PrivateKey, pc)
	s.node.Store(node)

	err := node.Serve(ctx, &bcp.Server{
		Access: func(peer blobcache.PeerID) blobcache.Service {
			if s.env.Policy.CanConnect(peer) {
				return &peerView[LK, LV, LQ]{
					svc:       s,
					peer:      peer,
					tmpSecret: s.tmpSecret,
				}
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

func (s *Service[LK, LV, LQ]) Ping(ctx context.Context, ep blobcache.Endpoint) error {
	node, err := s.grabNode(ctx)
	if err != nil {
		return err
	}
	return bcp.Ping(ctx, node, ep)
}

func (s *Service[LK, LV, LQ]) LocalID() blobcache.PeerID {
	return inet256.NewID(s.env.PrivateKey.Public().(inet256.PublicKey))
}

// AbortAll aborts all transactions.
func (s *Service[LK, LV, LQ]) AbortAll(ctx context.Context) error {
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
// Cleanup removes expired handles, and cleans up any transactions, which are no longer referenced.
// It also removes volumes which are no longer referenced, and cleans up any in-memory resources for them.
// The dropped volumes are returned.
func (s *Service[LK, LV, LQ]) Cleanup(ctx context.Context) ([]blobcache.OID, error) {
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
	s.mu.Unlock()
	return ret, nil
}

// addVolume adds a volume to the volumes map.
// It acquires mu exclusively, and returns false if the volume already exists.
func (s *Service[LK, LV, LQ]) addVolume(oid blobcache.OID, vol backend.Volume) bool {
	if oid == (blobcache.OID{}) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.volumes[oid]; exists {
		return false
	}
	if s.volumes == nil {
		s.volumes = make(map[blobcache.OID]volume)
	}
	s.volumes[oid] = volume{
		info: blobcache.VolumeInfo{
			ID:           oid,
			VolumeConfig: vol.GetParams(),
			Backend:      vol.GetBackend(),
		},
		backend: vol,
	}
	return true
}

// addQueue adds a queue to the queues map.
// It acquires mu exclusively, and returns false if the queue already exists.
func (s *Service[LK, LV, LQ]) addQueue(oid blobcache.OID, q backend.Queue, cfg blobcache.QueueConfig, spec blobcache.QueueBackend[blobcache.OID]) bool {
	if oid == (blobcache.OID{}) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.queues[oid]; exists {
		return false
	}
	if s.queues == nil {
		s.queues = make(map[blobcache.OID]queue)
	}
	s.queues[oid] = queue{
		info: blobcache.QueueInfo{
			ID:      oid,
			Config:  cfg,
			Backend: spec,
		},
		backend: q,
	}
	return true
}

func (s *Service[LK, LV, LQ]) queueByOID(x blobcache.OID) (queue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, exists := s.queues[x]
	return q, exists
}

func (s *Service[LK, LV, LQ]) volByOID(x blobcache.OID) (volume, bool) {
	if x == (blobcache.OID{}) {
		rootVol := s.env.Root
		return volume{
			info: blobcache.VolumeInfo{
				VolumeConfig: rootVol.GetParams(),
				Backend:      rootVol.GetBackend(),
			},
			backend: rootVol,
		}, true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	vol, exists := s.volumes[x]
	return vol, exists
}

// mountVolume ensures the volume is available.
// if the volume is already in memory, it does nothing.
// otherwise it calls makeVolume and writes to the volumes map.
func (s *Service[LK, LV, LQ]) mountVolume(ctx context.Context, oid blobcache.OID) error {
	s.mu.RLock()
	_, exists := s.volumes[oid]
	s.mu.RUnlock()
	if exists {
		return nil
	}
	if oid == (blobcache.OID{}) {
		return nil
	}
	info, err := s.inspectVolume(ctx, oid)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("volume %s not found", oid)
	}
	vol, err := s.makeVolume(ctx, oid, info.Backend)
	if err != nil {
		return err
	}
	s.addVolume(oid, vol)
	return nil
}

func (s *Service[LK, LV, LQ]) inspectVolume(ctx context.Context, volID blobcache.OID) (*blobcache.VolumeInfo, error) {
	var ve VolumeEntry
	found, err := s.env.MDS.Get(ctx, volID, &ve)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return ve.Info(), nil
}

type volume struct {
	info    blobcache.VolumeInfo
	backend backend.Volume
}

type queue struct {
	info    blobcache.QueueInfo
	backend backend.Queue
}

type transaction struct {
	backend backend.Tx
	volume  *volume
}

// resolveVol first checks that the handle is valid.
// then if it is it will check the volumes map with a read lock.
// If there is no entry for the volume then the read-lock is temporarily
// released so that the volume can be mounted if it exists.
func (s *Service[LK, LV, LQ]) resolveVol(ctx context.Context, x blobcache.Handle) (volume, blobcache.ActionSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	oid, rights := s.handles.Resolve(x)
	if rights == 0 {
		return volume{}, 0, blobcache.ErrInvalidHandle{Handle: x}
	}
	// If it is the root volume, then make up the volume struct on the spot.
	if oid == (blobcache.OID{}) {
		rootVol, _ := s.volByOID(oid)
		return rootVol, rights, nil
	}
	vol, exists := s.volumes[oid]
	if !exists {
		s.mu.RUnlock()
		err := s.mountVolume(ctx, x.OID)
		s.mu.RLock()
		if err != nil {
			return volume{}, 0, err
		}
		return volume{}, 0, fmt.Errorf("handle does not refer to volume, OID=%v ", x.OID)
	}
	return vol, rights, nil
}

// resolveTx looks up the transaction handle from memory.
// If the handle is valid it will load a new transaction.
func (s *Service[LK, LV, LQ]) resolveTx(txh blobcache.Handle, touch bool, requires blobcache.ActionSet) (transaction, error) {
	oid, rights := s.handles.Resolve(txh)
	if rights == 0 {
		return transaction{}, blobcache.ErrInvalidHandle{Handle: txh}
	}
	// transactions are not stored in the database, so we only have to check the handles map.
	tx, exists := s.txns[oid]
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
		s.handles.KeepAlive(txh, time.Now().Add(DefaultTxTTL))
	}
	return tx, nil
}

// Endpoint blocks waiting for a node to be created (happens when Serve is running).
// And then returns the Endpoint for that Node.
func (s *Service[LK, LV, LQ]) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
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

func (s *Service[LK, LV, LQ]) Drop(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	s.handles.Drop(h)
	s.mu.Unlock()
	return nil
}

func (s *Service[LK, LV, LQ]) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
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

func (s *Service[LK, LV, LQ]) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return nil, fmt.Errorf("Share not implemented")
}

func (s *Service[LK, LV, LQ]) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	hstate, exists := s.handles.Inspect(h)
	if !exists {
		return nil, blobcache.ErrInvalidHandle{Handle: h}
	}
	return &blobcache.HandleInfo{
		OID:       h.OID,
		Rights:    hstate.rights,
		CreatedAt: tai64.Now().TAI64(), // TODO: store creation time.
		ExpiresAt: tai64.FromGoTime(hstate.expiresAt).TAI64(),
	}, nil
}

func (s *Service[LK, LV, LQ]) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	if x != (blobcache.OID{}) {
		if err := s.mountVolume(ctx, x); err != nil {
			return nil, err
		}
	}
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultVolumeTTL)
	h := s.handles.Create(x, mask, createdAt, expiresAt)
	return &h, nil
}

func (s *Service[LK, LV, LQ]) OpenFrom(ctx context.Context, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	baseVol, _, err := s.resolveVol(ctx, base)
	if err != nil {
		return nil, err
	}

	switch baseVol := baseVol.backend.(type) {
	case *remotebe.Volume:
		rights, subvol, err := s.backends.remote.OpenFrom(ctx, baseVol, ltok, mask)
		if err != nil {
			return nil, err
		}
		// create a handle first to prevent expiration
		localOID := blobcache.RandomOID()
		createdAt := time.Now()
		expiresAt := createdAt.Add(DefaultVolumeTTL)
		h := s.handles.Create(localOID, rights, createdAt, expiresAt)
		// add the volume
		s.addVolume(localOID, subvol)
		return &h, nil

	default:
		rights, err := baseVol.AccessSubVolume(ctx, ltok)
		if err != nil {
			return nil, err
		}
		if rights == 0 {
			return nil, blobcache.ErrNoLink{Base: base.OID, Target: ltok.Target}
		}

		volInfo, err := s.inspectVolume(ctx, ltok.Target)
		if err != nil {
			return nil, err
		}
		if volInfo == nil {
			return nil, fmt.Errorf("volume=%v has link to subvolume=%v, but that subvolume was not found", base.OID, ltok.Target)
		}

		// create a handle first to prevent expiration
		rights = rights & mask
		localOID := ltok.Target
		createdAt := time.Now()
		expiresAt := createdAt.Add(DefaultVolumeTTL)
		h := s.handles.Create(localOID, rights, createdAt, expiresAt)
		if err := s.mountVolume(ctx, ltok.Target); err != nil {
			return nil, err
		}
		return &h, nil
	}
}

func (s *Service[LK, LV, LQ]) grabNode(ctx context.Context) (*bcnet.Node, error) {
	var node *bcnet.Node
	// we will need the node to handle this.
	for i := 0; i < 10 && node == nil; i++ {
		node = s.node.Load()
		if node != nil {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, fmt.Errorf("bclocal: node is not running. Cannot call CreateVolume with a non-nil host")
		}
	}
	if node == nil {
		return nil, fmt.Errorf("bclocal: node is not running. Cannot call CreateVolume with a non-nil host")
	}
	return node, nil
}

func (s *Service[LK, LV, LQ]) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if err := vspec.Validate(); err != nil {
		return nil, err
	}

	if host != nil && host.Peer != s.LocalID() {
		return s.createRemoteVolume(ctx, *host, vspec)
	}

	vp, err := s.findVolumeParams(ctx, vspec)
	if err != nil {
		return nil, err
	}
	// generate an OID for the volume
	var oid blobcache.OID
	switch {
	case vspec.Local != nil:
		if vp.MaxSize > s.cfg.MaxMaxBlobSize {
			return nil, fmt.Errorf("local volume backend only supports blobs up to %d, requested %d", s.cfg.MaxMaxBlobSize, vp.MaxSize)
		}
		if _, err := s.env.MkSchema(vp.Schema); err != nil {
			return nil, err
		}
		lvid, err := s.env.GenerateLK()
		if err != nil {
			return nil, err
		}
		oid = s.env.LKToOID(lvid)
	default:
		oid = blobcache.RandomOID()
	}
	info := blobcache.VolumeInfo{
		ID:           oid,
		VolumeConfig: vp,
		Backend:      blobcache.VolumeBackendToOID(vspec),
	}

	ve := VolumeEntry{
		OID: oid,

		MaxSize:  vp.MaxSize,
		HashAlgo: vp.HashAlgo,
		Salted:   vp.Salted,
		Schema:   vp.Schema,
		Backend:  info.Backend,

		Deps: nil,
	}
	if err := s.env.MDS.Put(ctx, oid, ve); err != nil {
		return nil, err
	}
	if err := s.mountVolume(ctx, info.ID); err != nil {
		return nil, err
	}
	// create a handle
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultVolumeTTL)
	handle := s.handles.Create(info.ID, blobcache.Action_ALL, createdAt, expiresAt)
	return &handle, nil
}

// createRemoteVolume calls CreateVolume on a remote node
// it then creates a new local volume with a new random OID
func (s *Service[LK, LV, LQ]) createRemoteVolume(ctx context.Context, host blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	node, err := s.grabNode(ctx)
	if err != nil {
		return nil, err
	}
	// the request is for a remote node.
	rvh, err := bcp.CreateVolume(ctx, node, host, vspec)
	if err != nil {
		return nil, err
	}
	rvInfo, err := bcp.InspectVolume(ctx, node, host, *rvh)
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
				Volume:   rvInfo.ID,
				HashAlgo: vp.HashAlgo,
			},
		},
	}
	vol, err := s.backends.remote.VolumeUp(ctx, remotebe.Params{
		Endpoint: host,
		Volume:   rvInfo.ID,
		HashAlgo: vp.HashAlgo,
	})
	if err != nil {
		return nil, err
	}
	s.addVolume(localInfo.ID, vol)
	// create a local handle
	localHandle := s.handles.Create(localInfo.ID, blobcache.Action_ALL, time.Now(), time.Now().Add(DefaultVolumeTTL))
	return &localHandle, nil
}

func (s *Service[LK, LV, LQ]) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	vol, _, err := s.resolveVol(ctx, volh)
	if err != nil {
		return nil, err
	}
	if vol.info.Backend.Local == nil {
		return nil, fmt.Errorf("only local volumes can be cloned")
	}

	vinfo, err := s.inspectVolume(ctx, vol.info.ID)
	if err != nil {
		return nil, err
	}
	if vinfo == nil {
		return nil, fmt.Errorf("CloneVolume: volume not found")
	}
	vinfo.ID = blobcache.RandomOID()
	ve := VolumeEntry{}
	if err := s.env.MDS.Put(ctx, vinfo.ID, ve); err != nil {
		return nil, err
	}

	h := s.handles.Create(vol.info.ID, blobcache.Action_ALL, time.Now(), time.Now().Add(DefaultVolumeTTL))
	return &h, nil
}

func (s *Service[LK, LV, LQ]) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	vol, _, err := s.resolveVol(ctx, h)
	if err != nil {
		return nil, err
	}
	return &vol.info, nil
}

func (s *Service[LK, LV, LQ]) BeginTx(ctx context.Context, volh blobcache.Handle, txspec blobcache.TxParams) (*blobcache.Handle, error) {
	vol, _, err := s.resolveVol(ctx, volh)
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

func (s *Service[LK, LV, LQ]) InspectTx(ctx context.Context, txh blobcache.Handle) (*blobcache.TxInfo, error) {
	txn, err := s.resolveTx(txh, false, blobcache.Action_TX_INSPECT)
	if err != nil {
		return nil, err
	}
	vol := txn.volume
	vp := vol.backend.GetParams()
	return &blobcache.TxInfo{
		ID:     txh.OID,
		Volume: vol.info.ID,

		MaxSize:  vp.MaxSize,
		HashAlgo: vp.HashAlgo,
		Params:   txn.backend.Params(),
	}, nil
}

func (s *Service[LK, LV, LQ]) Save(ctx context.Context, txh blobcache.Handle, root []byte) error {
	tx, err := s.resolveTx(txh, true, blobcache.Action_TX_SAVE)
	if err != nil {
		return err
	}
	if p := tx.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "SAVE"}
	}
	// validate against the schema.
	var prevRoot []byte
	if err := tx.backend.Load(ctx, &prevRoot); err != nil {
		return err
	}
	src := backend.NewUnsaltedStore(tx.backend)
	sch, err := s.env.MkSchema(tx.volume.info.Schema)
	if err != nil {
		return err
	}
	change := schema.Change{
		Prev: schema.Value{
			Cell:  prevRoot,
			Store: src, // TODO: Need to open read-only transaction for the previous version of the volume.
		},
		Next: schema.Value{
			Cell:  root,
			Store: src,
		},
	}
	if err := sch.ValidateChange(ctx, change); err != nil {
		return err
	}
	return setErrTxOID(tx.backend.Save(ctx, root), txh.OID)
}

func (s *Service[LK, LV, LQ]) Commit(ctx context.Context, txh blobcache.Handle) error {
	tx, err := s.resolveTx(txh, true, 0) // anyone can commit the transaction if they opened it.
	if err != nil {
		return err
	}
	if p := tx.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "COMMIT"}
	}
	if err := tx.backend.Commit(ctx); err != nil {
		return setErrTxOID(err, txh.OID)
	}
	s.mu.Lock()
	s.handles.Drop(txh)
	s.mu.Unlock()

	return nil
}

func (s *Service[LK, LV, LQ]) Abort(ctx context.Context, txh blobcache.Handle) error {
	txn, err := s.resolveTx(txh, false, 0) // anyone can abort the transaction if they opened it.
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

func (s *Service[LK, LV, LQ]) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_LOAD)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Load(ctx, dst), txh.OID)
}

func (s *Service[LK, LV, LQ]) Post(ctx context.Context, txh blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_POST)
	if err != nil {
		return blobcache.CID{}, err
	}

	if p := txn.backend.Params(); !p.Modify {
		return blobcache.CID{}, blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "POST"}
	}
	cid, err := txn.backend.Post(ctx, data, opts)
	if err != nil {
		return blobcache.CID{}, setErrTxOID(err, txh.OID)
	}
	return cid, nil
}

func (s *Service[LV, LK, LQ]) Exists(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_EXISTS)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Exists(ctx, cids, dst), txh.OID)
}

func (s *Service[LV, LK, LQ]) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_GET)
	if err != nil {
		return 0, err
	}
	n, err := txn.backend.Get(ctx, cid, buf, opts)
	if err != nil {
		return 0, setErrTxOID(err, txh.OID)
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

func (s *Service[LV, LK, LQ]) Delete(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_DELETE)
	if err != nil {
		return err
	}
	if p := txn.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "DELETE"}
	}
	return setErrTxOID(txn.backend.Delete(ctx, cids), txh.OID)
}

func (s *Service[LK, LV, LQ]) Copy(ctx context.Context, txh blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, out []bool) error {
	if len(cids) != len(out) {
		return fmt.Errorf("cids and out must have the same length")
	}
	_, err := s.resolveTx(txh, true, blobcache.Action_TX_COPY_FROM)
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

func (s *Service[LK, LV, LQ]) Visit(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	txn, err := s.resolveTx(txh, true, 0) // if a GC transaction was opened, then Visit it allowed.
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Visit(ctx, cids), txh.OID)
}

func (s *Service[LK, LV, LQ]) IsVisited(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and out must have the same length")
	}
	txn, err := s.resolveTx(txh, true, 0) // if a GC transaction was opened, then IsVisited is allowed.
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.IsVisited(ctx, cids, dst), txh.OID)
}

func (s *Service[LK, LV, LQ]) Link(ctx context.Context, txh blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_LINK_FROM)
	if err != nil {
		return nil, err
	}
	volTo, rights, err := s.resolveVol(ctx, target)
	if err != nil {
		return nil, err
	}
	ltok, err := txn.backend.Link(ctx, volTo.info.ID, rights&mask, volTo.backend)
	if err != nil {
		return nil, setErrTxOID(err, txh.OID)
	}
	return ltok, nil
}

func (s *Service[LK, LV, LQ]) Unlink(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkToken) error {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_UNLINK_FROM)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Unlink(ctx, targets), txh.OID)
}

func (s *Service[LK, LV, LQ]) VisitLinks(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkToken) error {
	txn, err := s.resolveTx(txh, true, 0) // if a GC transaction was opened, then VisitLinks is allowed.
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.VisitLinks(ctx, targets), txh.OID)
}

func (s *Service[LK, LV, LQ]) CreateQueue(ctx context.Context, host *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	if host != nil && host.Peer != s.LocalID() {
		return s.createRemoteQueue(ctx, *host, qspec)
	}
	if err := qspec.Validate(); err != nil {
		return nil, err
	}

	// create a new Queue on the local Node.
	var queue backend.Queue
	var err error
	switch {
	case qspec.Memory != nil:
		queue, err = s.env.Local.CreateQueue(ctx, *qspec.Memory)
		if err != nil {
			return nil, err
		}
	case qspec.Remote != nil:
		queue, err = s.backends.remote.QueueUp(ctx, qspec.Remote)
		if err != nil {
			return nil, err
		}
	default:
		panic(qspec)
	}
	oid := blobcache.RandomOID()
	qcfg := queue.Config()
	qspec2 := blobcache.QueueBackendToOID(qspec)
	if !s.addQueue(oid, queue, qcfg, qspec2) {
		panic("random OID happened twice")
	}
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultQueueTTL)
	handle := s.handles.Create(oid, blobcache.Action_ALL, createdAt, expiresAt)
	return &handle, nil
}

// createRemoteQueue calls CreateQueue on a remote node
// it then creates a local proxy queue with a new random OID
func (s *Service[LK, LV, LQ]) createRemoteQueue(ctx context.Context, host blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	q, info, err := s.backends.remote.CreateQueue(ctx, host, qspec)
	if err != nil {
		return nil, err
	}
	oid := blobcache.RandomOID()
	if !s.addQueue(oid, q, info.Config, blobcache.QueueBackend[blobcache.OID]{
		Remote: &blobcache.QueueBackend_Remote{
			Endpoint: host,
			OID:      info.ID,
		},
	}) {
		return nil, fmt.Errorf("queue already exists")
	}
	createdAt := time.Now()
	expiresAt := createdAt.Add(DefaultQueueTTL)
	handle := s.handles.Create(oid, blobcache.Action_ALL, createdAt, expiresAt)
	return &handle, nil
}

func (s *Service[LK, LV, LQ]) InspectQueue(ctx context.Context, qh blobcache.Handle) (blobcache.QueueInfo, error) {
	q, err := s.resolveQueue(ctx, qh, blobcache.Action_QUEUE_INSPECT)
	if err != nil {
		return blobcache.QueueInfo{}, err
	}
	return q.info, nil
}

func (s *Service[LK, LV, LQ]) Dequeue(ctx context.Context, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	if err := opts.Validate(); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, fmt.Errorf("dequeue buffer must be non-empty")
	}
	q, err := s.resolveQueue(ctx, qh, blobcache.Action_QUEUE_NEXT)
	if err != nil {
		return 0, err
	}
	return q.backend.Dequeue(ctx, buf, opts)
}

func (s *Service[LK, LV, LQ]) Enqueue(ctx context.Context, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	q, err := s.resolveQueue(ctx, qh, blobcache.Action_QUEUE_INSERT)
	if err != nil {
		return nil, err
	}
	maxBytes := q.info.Config.MaxBytesPerMessage
	maxHandles := q.info.Config.MaxHandlesPerMessage
	for i, msg := range msgs {
		if uint32(len(msg.Bytes)) > maxBytes {
			return nil, fmt.Errorf("message %d exceeds max bytes per message: %d", i, maxBytes)
		}
		if uint32(len(msg.Handles)) > maxHandles {
			return nil, fmt.Errorf("message %d exceeds max handles per message: %d", i, maxHandles)
		}
	}
	n, err := q.backend.Enqueue(ctx, msgs)
	if err != nil {
		return nil, err
	}
	return &blobcache.InsertResp{Success: uint32(n)}, nil
}

func (s *Service[LK, LV, LQ]) SubToVolume(ctx context.Context, qh blobcache.Handle, volh blobcache.Handle, spec blobcache.VolSubSpec) error {
	q, err := s.resolveQueue(ctx, qh, blobcache.Action_QUEUE_SUB_VOLUME)
	if err != nil {
		return err
	}
	vol, rights, err := s.resolveVol(ctx, volh)
	if err != nil {
		return err
	}
	if rights&blobcache.Action_VOLUME_SUB_TO < blobcache.Action_VOLUME_SUB_TO {
		return blobcache.ErrPermission{
			Handle:   volh,
			Rights:   rights,
			Requires: blobcache.Action_VOLUME_SUB_TO,
		}
	}
	switch rv := vol.backend.(type) {
	case *remotebe.Volume:
		return s.backends.remote.SubToVol(ctx, rv, q.backend, spec)
	default:
		lv, ok := vol.backend.(LV)
		if !ok {
			return fmt.Errorf("SubToVolume not supported for volume type:%T", vol.backend)
		}
		return s.env.Local.SubToVol(ctx, lv, q.backend, spec)
	}
}

func (s *Service[LK, LV, LQ]) resolveQueue(ctx context.Context, qh blobcache.Handle, requires blobcache.ActionSet) (queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	oid, rights := s.handles.Resolve(qh)
	if rights == 0 {
		return queue{}, blobcache.ErrInvalidHandle{Handle: qh}
	}
	if rights&requires < requires {
		return queue{}, blobcache.ErrPermission{
			Handle:   qh,
			Rights:   rights,
			Requires: requires,
		}
	}
	q, exists := s.queues[oid]
	if !exists {
		return queue{}, blobcache.ErrInvalidHandle{Handle: qh}
	}
	return q, nil
}

// makeVolume constructs an in-memory volume object from a backend.
// it does not create volumes in the database.
func (s *Service[LK, LV, LQ]) makeVolume(ctx context.Context, oid blobcache.OID, volBackend blobcache.VolumeBackend[blobcache.OID]) (backend.Volume, error) {
	if err := volBackend.Validate(); err != nil {
		return nil, err
	}
	if oid == (blobcache.OID{}) {
		return s.env.Root, nil
	}
	switch {
	case volBackend.Local != nil:
		lvid, err := s.env.OIDToLK(oid)
		if err != nil {
			return nil, err
		}
		return s.makeLocal(ctx, oid, lvid)
	case volBackend.Remote != nil:
		return s.backends.remote.VolumeUp(ctx, *volBackend.Remote)
	case volBackend.Git != nil:
		return s.makeGit(ctx, *volBackend.Git)
	case volBackend.Vault != nil:
		return s.makeVault(ctx, *volBackend.Vault)
	default:
		return nil, fmt.Errorf("empty backend")
	}
}

func (s *Service[LK, LV, LQ]) makeLocal(ctx context.Context, oid blobcache.OID, lk LK) (backend.Volume, error) {
	var ve VolumeEntry
	found, err := s.env.MDS.Get(ctx, oid, &ve)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("makeLocal: volume entry not found")
	}
	return s.backends.local.VolumeUp(ctx, LVParams[LK]{
		Key:    lk,
		Params: blobcache.VolumeConfig(*ve.Info().Backend.Local),
	})
}

func (s *Service[LK, LV, LQ]) makeGit(ctx context.Context, backend blobcache.VolumeBackend_Git) (backend.Volume, error) {
	return nil, fmt.Errorf("git volumes are not yet supported")
}

func (s *Service[LK, LV, LQ]) makeVault(ctx context.Context, backend blobcache.VolumeBackend_Vault[blobcache.OID]) (*vaultvol.Vault, error) {
	volstate, exists := s.volByOID(backend.X)
	if !exists {
		return nil, fmt.Errorf("inner volume not found: %v", backend.X)
	}
	inner, err := s.makeVolume(ctx, backend.X, volstate.info.Backend)
	if err != nil {
		return nil, err
	}
	return vaultvol.New(inner, backend.Secret, backend.HashAlgo.HashFunc()), nil
}

func (s *Service[LK, LV, LQ]) findVolumeParams(ctx context.Context, vspec blobcache.VolumeSpec) (blobcache.VolumeConfig, error) {
	switch {
	case vspec.Local != nil:
		return vspec.Config(), nil
	case vspec.Git != nil:
		return vspec.Config(), nil
	case vspec.Remote != nil:
		vol, err := s.backends.remote.VolumeUp(ctx, *vspec.Remote)
		if err != nil {
			return blobcache.VolumeConfig{}, err
		}
		return vol.GetParams(), nil

	case vspec.Vault != nil:
		innerVol, _, err := s.resolveVol(ctx, vspec.Vault.X)
		if err != nil {
			return blobcache.VolumeConfig{}, err
		}
		return innerVol.info.VolumeConfig, nil
	default:
		panic(vspec)
	}
}

func setErrTxOID(err error, oid blobcache.OID) error {
	switch e := err.(type) {
	case blobcache.ErrTxDone:
		e.ID = oid
		return e
	case blobcache.ErrTxReadOnly:
		e.Tx = oid
		return e
	default:
		return err
	}
}
