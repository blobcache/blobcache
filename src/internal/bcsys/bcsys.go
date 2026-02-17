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
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/pubsub"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/internal/volumes/consensusvol"
	"blobcache.io/blobcache/src/internal/volumes/remotevol"
	"blobcache.io/blobcache/src/internal/volumes/vaultvol"
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
)

type LocalVolume[K any] interface {
	volumes.Volume
	Key() K
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

type Env[LK any, LV LocalVolume[LK]] struct {
	Background context.Context
	PrivateKey ed25519.PrivateKey
	// Root is the root volume.
	// It will have the all-zero OID.
	Root volumes.Volume
	// MDS is where Volume metadata is stored.
	MDS         MetadataStore
	Policy      Policy
	PeerLocator PeerLocator
	MkSchema    schema.Factory

	// Local is the local volume system.
	Local      volumes.System[LVParams[LK], LV]
	GenerateLK func() (LK, error)
	LKToOID    func(LK) blobcache.OID
	OIDToLK    func(blobcache.OID) (LK, error)
}

type Config struct {
	// MaxMaxBlobSize is the maximum size of the MaxSize volume parameter, for any local volume.
	MaxMaxBlobSize int64
}

func New[LK any, LV LocalVolume[LK]](env Env[LK, LV], cfg Config) *Service[LK, LV] {
	var tmpSecret [32]byte
	if _, err := rand.Read(tmpSecret[:]); err != nil {
		panic(err)
	}
	s := &Service[LK, LV]{
		env: env,
		cfg: cfg,

		node:      atomic.Pointer[bcnet.Node]{},
		tmpSecret: &tmpSecret,
	}
	s.volSys.local = env.Local
	s.volSys.remote = remotevol.New(&s.node)
	s.volSys.global = consensusvol.New(consensusvol.Env{
		Background: s.env.Background,
		Hub:        &s.hub,
	})
	return s
}

type Service[LK any, LV LocalVolume[LK]] struct {
	env Env[LK, LV]
	cfg Config

	node   atomic.Pointer[bcnet.Node]
	volSys struct {
		local  volumes.System[LVParams[LK], LV]
		remote remotevol.System
		global consensusvol.System
	}
	hub       pubsub.Hub
	tmpSecret *[32]byte

	handles handleSystem
	// mu guards the volumes and txns.
	// pure handle operations like Drop, KeepAlive, Inspect, etc. do not require this lock.
	// mu should always be taken for a superset of the time that the handle system's lock is taken.
	mu      sync.RWMutex
	volumes map[blobcache.OID]volume
	txns    map[blobcache.OID]transaction
}

// Serve handles requests from the network.
// Serve blocks untilt the context is cancelled, or Close is called.
// Cancelling the context will cause Run to return without an error.
// If Serve is *not* running, then remote volumes will not work, hosted on this Node or other Nodes.
func (s *Service[LK, LV]) Serve(ctx context.Context, pc net.PacketConn) error {
	node := bcnet.New(s.env.PrivateKey, pc)
	s.node.Store(node)

	err := node.Serve(ctx, &bcp.Server{
		Access: func(peer blobcache.PeerID) blobcache.Service {
			if s.env.Policy.CanConnect(peer) {
				return &peerView[LK, LV]{
					svc:       s,
					peer:      peer,
					tmpSecret: s.tmpSecret,
				}
			} else {
				return nil
			}
		},
		Deliver: func(ctx context.Context, from blobcache.Endpoint, ttm bcp.TopicTellMsg) error {
			topicID := s.hub.Lookup(ttm.TopicHash)
			if topicID.IsZero() {
				logctx.Warn(ctx, "dropping tell message", zap.Any("from", from), zap.Int("ctext_len", len(ttm.Ciphertext)))
				return nil
			}
			tmsg := s.hub.Acquire()
			if err := ttm.Decrypt(topicID, tmsg); err != nil {
				s.hub.Release(tmsg)
				return err
			}
			return nil
		},
	})
	if errors.Is(err, net.ErrClosed) {
		err = nil
	} else if errors.Is(err, context.Canceled) {
		err = nil
	}
	return err
}

func (s *Service[LK, LV]) Ping(ctx context.Context, ep blobcache.Endpoint) error {
	node, err := s.grabNode(ctx)
	if err != nil {
		return err
	}
	return bcp.Ping(ctx, node, ep)
}

func (s *Service[LK, LV]) LocalID() blobcache.PeerID {
	return inet256.NewID(s.env.PrivateKey.Public().(inet256.PublicKey))
}

// AbortAll aborts all transactions.
func (s *Service[LK, LV]) AbortAll(ctx context.Context) error {
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
func (s *Service[LK, LV]) Cleanup(ctx context.Context) ([]blobcache.OID, error) {
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
			delete(s.volumes, oid)
			ret = append(ret, oid)
		}
	}
	s.mu.Unlock()
	return ret, nil
}

// addVolume adds a volume to the volumes map.
// It acquires mu exclusively, and returns false if the volume already exists.
func (s *Service[LK, LV]) addVolume(oid blobcache.OID, vol volumes.Volume) bool {
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

func (s *Service[LK, LV]) volByOID(x blobcache.OID) (volume, bool) {
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
func (s *Service[LK, LV]) mountVolume(ctx context.Context, oid blobcache.OID) error {
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

func (s *Service[LK, LV]) inspectVolume(ctx context.Context, volID blobcache.OID) (*blobcache.VolumeInfo, error) {
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
	backend volumes.Volume
}

type transaction struct {
	backend volumes.Tx
	volume  *volume
}

// resolveVol first checks that the handle is valid.
// then if it is it will check the volumes map with a read lock.
// If there is no entry for the volume then the read-lock is temporarily
// released so that the volume can be mounted if it exists.
func (s *Service[LK, LV]) resolveVol(ctx context.Context, x blobcache.Handle) (volume, blobcache.ActionSet, error) {
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
func (s *Service[LK, LV]) resolveTx(txh blobcache.Handle, touch bool, requires blobcache.ActionSet) (transaction, error) {
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
func (s *Service[LK, LV]) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
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

func (s *Service[LK, LV]) Drop(ctx context.Context, h blobcache.Handle) error {
	s.mu.Lock()
	s.handles.Drop(h)
	s.mu.Unlock()
	return nil
}

func (s *Service[LK, LV]) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
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

func (s *Service[LK, LV]) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return nil, fmt.Errorf("Share not implemented")
}

func (s *Service[LK, LV]) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
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

func (s *Service[LK, LV]) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
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

func (s *Service[LK, LV]) OpenFrom(ctx context.Context, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	baseVol, _, err := s.resolveVol(ctx, base)
	if err != nil {
		return nil, err
	}

	switch baseVol := baseVol.backend.(type) {
	case *remotevol.Volume:
		rights, subvol, err := s.volSys.remote.OpenFrom(ctx, baseVol, ltok, mask)
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

func (s *Service[LK, LV]) grabNode(ctx context.Context) (*bcnet.Node, error) {
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

func (s *Service[LK, LV]) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
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
func (s *Service[LK, LV]) createRemoteVolume(ctx context.Context, host blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
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
	vol, err := s.volSys.remote.Up(ctx, remotevol.Params{
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

func (s *Service[LK, LV]) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
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

func (s *Service[LV, LK]) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	vol, _, err := s.resolveVol(ctx, h)
	if err != nil {
		return nil, err
	}
	return &vol.info, nil
}

func (s *Service[LV, LK]) BeginTx(ctx context.Context, volh blobcache.Handle, txspec blobcache.TxParams) (*blobcache.Handle, error) {
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

func (s *Service[LK, LV]) InspectTx(ctx context.Context, txh blobcache.Handle) (*blobcache.TxInfo, error) {
	txn, err := s.resolveTx(txh, false, blobcache.Action_TX_INSPECT)
	if err != nil {
		return nil, err
	}
	vol := txn.volume.backend
	switch vol := vol.(type) {
	case LV:
		lk := vol.Key()
		vp := vol.GetParams()
		return &blobcache.TxInfo{
			ID:       txh.OID,
			Volume:   s.env.LKToOID(lk),
			MaxSize:  vp.MaxSize,
			HashAlgo: vp.HashAlgo,
		}, nil
	case *vaultvol.Vault:
		innerVol := vol.Inner()
		switch inner := innerVol.(type) {
		case LV:
			vp := innerVol.GetParams()
			lk := inner.Key()
			return &blobcache.TxInfo{
				ID:       txh.OID,
				Volume:   s.env.LKToOID(lk),
				MaxSize:  vp.MaxSize,
				HashAlgo: vp.HashAlgo,
			}, nil
		default:
			return nil, fmt.Errorf("InspectTx not implemented for inner volume type:%T", inner)
		}
	default:
		return nil, fmt.Errorf("InspectTx not implemented for volume type:%T", vol)
	}
}

func (s *Service[LV, LK]) Save(ctx context.Context, txh blobcache.Handle, root []byte) error {
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
	src := volumes.NewUnsaltedStore(tx.backend)
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

func (s *Service[LV, LK]) Commit(ctx context.Context, txh blobcache.Handle) error {
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

func (s *Service[LV, LK]) Abort(ctx context.Context, txh blobcache.Handle) error {
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

func (s *Service[LV, LK]) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_LOAD)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Load(ctx, dst), txh.OID)
}

func (s *Service[LV, LK]) Post(ctx context.Context, txh blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
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

func (s *Service[LV, LK]) Exists(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_EXISTS)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Exists(ctx, cids, dst), txh.OID)
}

func (s *Service[LV, LK]) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
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

func (s *Service[LV, LK]) Delete(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_DELETE)
	if err != nil {
		return err
	}
	if p := txn.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "DELETE"}
	}
	return setErrTxOID(txn.backend.Delete(ctx, cids), txh.OID)
}

func (s *Service[LK, LV]) Copy(ctx context.Context, txh blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, out []bool) error {
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

func (s *Service[LK, LV]) Visit(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	txn, err := s.resolveTx(txh, true, 0) // if a GC transaction was opened, then Visit it allowed.
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Visit(ctx, cids), txh.OID)
}

func (s *Service[LK, LV]) IsVisited(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and out must have the same length")
	}
	txn, err := s.resolveTx(txh, true, 0) // if a GC transaction was opened, then IsVisited is allowed.
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.IsVisited(ctx, cids, dst), txh.OID)
}

func (s *Service[LK, LV]) Link(ctx context.Context, txh blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
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

func (s *Service[LK, LV]) Unlink(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkToken) error {
	txn, err := s.resolveTx(txh, true, blobcache.Action_TX_UNLINK_FROM)
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.Unlink(ctx, targets), txh.OID)
}

func (s *Service[LK, LV]) VisitLinks(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkToken) error {
	txn, err := s.resolveTx(txh, true, 0) // if a GC transaction was opened, then VisitLinks is allowed.
	if err != nil {
		return err
	}
	return setErrTxOID(txn.backend.VisitLinks(ctx, targets), txh.OID)
}

func (s *Service[LK, LV]) CreateQueue(ctx context.Context, host *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	return nil, fmt.Errorf("CreateQueue not implemented")
}

func (s *Service[LK, LV]) Dequeue(ctx context.Context, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.NextOpts) (int, error) {
	panic("Dequeue not implemented")
}

func (s *Service[LK, LV]) Enqueue(ctx context.Context, from *blobcache.Endpoint, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	panic("Enqueue not implemented")
}

func (s *Service[LK, LV]) SubToVolume(ctx context.Context, qh blobcache.Handle, volh blobcache.Handle) error {
	return fmt.Errorf("SubToVolume not implemented")
}

// makeVolume constructs an in-memory volume object from a backend.
// it does not create volumes in the database.
func (s *Service[LK, LV]) makeVolume(ctx context.Context, oid blobcache.OID, backend blobcache.VolumeBackend[blobcache.OID]) (volumes.Volume, error) {
	if err := backend.Validate(); err != nil {
		return nil, err
	}
	if oid == (blobcache.OID{}) {
		return s.env.Root, nil
	}
	switch {
	case backend.Local != nil:
		lvid, err := s.env.OIDToLK(oid)
		if err != nil {
			return nil, err
		}
		return s.makeLocal(ctx, oid, lvid)
	case backend.Remote != nil:
		return s.volSys.remote.Up(ctx, *backend.Remote)
	case backend.Git != nil:
		return s.makeGit(ctx, *backend.Git)
	case backend.Vault != nil:
		return s.makeVault(ctx, *backend.Vault)
	default:
		return nil, fmt.Errorf("empty backend")
	}
}

func (s *Service[LK, LV]) makeLocal(ctx context.Context, oid blobcache.OID, lk LK) (volumes.Volume, error) {
	var ve VolumeEntry
	found, err := s.env.MDS.Get(ctx, oid, &ve)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("makeLocal: volume entry not found")
	}
	return s.volSys.local.Up(ctx, LVParams[LK]{
		Key:    lk,
		Params: blobcache.VolumeConfig(*ve.Info().Backend.Local),
	})
}

func (s *Service[LK, LV]) makeGit(ctx context.Context, backend blobcache.VolumeBackend_Git) (volumes.Volume, error) {
	return nil, fmt.Errorf("git volumes are not yet supported")
}

func (s *Service[LK, LV]) makeVault(ctx context.Context, backend blobcache.VolumeBackend_Vault[blobcache.OID]) (*vaultvol.Vault, error) {
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

func (s *Service[LK, LV]) findVolumeParams(ctx context.Context, vspec blobcache.VolumeSpec) (blobcache.VolumeConfig, error) {
	switch {
	case vspec.Local != nil:
		return vspec.Config(), nil
	case vspec.Git != nil:
		return vspec.Config(), nil
	case vspec.Remote != nil:
		vol, err := s.volSys.remote.Up(ctx, *vspec.Remote)
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
