package bcsys

import (
	"context"
	"crypto/aes"
	"crypto/rand"
	"errors"
	"fmt"
	"iter"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"blobcache.io/blobcache/src/bccore"
	"blobcache.io/blobcache/src/bcp"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	consensusvol "blobcache.io/blobcache/src/internal/backend/consensusbe"
	"blobcache.io/blobcache/src/internal/backend/memory"
	"blobcache.io/blobcache/src/internal/backend/remotebe"
	"blobcache.io/blobcache/src/internal/backend/vaultvol"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/schema"
	"github.com/cloudflare/circl/sign/ed25519"
	"go.brendoncarroll.net/stdctx/logctx"
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
	WhereIs(ctx context.Context, peer blobcache.NodeID) iter.Seq[netip.AddrPort]
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
	s.backends.peer = remotebe.NewPeerSystem(&s.backends.remote, env.PeerLocator)
	s.backends.global = consensusvol.New(consensusvol.Env{
		Background: s.env.Background,
	})
	s.queueSys.memory = &memory.System{}
	s.core = bccore.New(bccore.Params{
		Root:   env.Root,
		Up:     s.up,
		OnLink: s.onLink,
		OnSave: func(ctx context.Context, vol bccore.Volume, tx bccore.Tx, root []byte) error {
			// validate against the schema.
			var prevRoot []byte
			if err := tx.Load(ctx, &prevRoot); err != nil {
				return err
			}
			src := backend.NewUnsaltedStore(tx)
			schSpec := vol.GetParams().Schema
			sch, err := env.MkSchema(schSpec)
			if err != nil {
				return err
			}
			change := schema.Change{
				Prev: schema.Value{
					Cell:  prevRoot,
					Store: src,
				},
				Next: schema.Value{
					Cell:  root,
					Store: src,
				},
			}
			return sch.ValidateChange(ctx, change)
		},
	})
	return s
}

type Service[LK any, LV LocalVolume[LK], LQ LocalQueue] struct {
	env Env[LK, LV, LQ]
	cfg Config

	node     atomic.Pointer[bcnet.Node]
	backends struct {
		local  backend.VolumeSystem[LVParams[LK], LV]
		remote remotebe.System
		peer   remotebe.PeerSystem
		global consensusvol.System
		memory memory.System
	}
	queueSys struct {
		memory *memory.System
	}
	tmpSecret *[32]byte
	core      bccore.System
}

// Serve handles requests from the network.
// Serve blocks untilt the context is cancelled, or Close is called.
// Cancelling the context will cause Run to return without an error.
// If Serve is *not* running, then remote volumes will not work, hosted on this Node or other Nodes.
func (s *Service[LK, LV, LQ]) Serve(ctx context.Context, pc net.PacketConn) error {
	node := bcnet.New(s.env.PrivateKey, pc)
	s.node.Store(node)

	err := node.Serve(ctx, &bcp.Server{
		Access: func(peer blobcache.NodeID) blobcache.Service {
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

func (s *Service[LK, LV, LQ]) LocalID() blobcache.NodeID {
	return inet256.NewID(s.env.PrivateKey.Public().(inet256.PublicKey))
}

// AbortAll aborts all transactions.
func (s *Service[LK, LV, LQ]) AbortAll(ctx context.Context) error {
	return s.core.AbortAll(ctx)
}

// Cleanup runs the full cleanup process.
// Cleanup removes expired handles, and cleans up any transactions, which are no longer referenced.
// It also removes volumes which are no longer referenced, and cleans up any in-memory resources for them.
// The dropped volumes are returned.
func (s *Service[LK, LV, LQ]) Cleanup(ctx context.Context) ([]blobcache.OID, error) {
	logctx.Info(ctx, "cleanup BEGIN")
	defer logctx.Info(ctx, "cleanup END")
	now := time.Now()
	var downed []blobcache.OID
	err := s.core.Cleanup(ctx, now, func(o blobcache.OID) {
		downed = append(downed, o)
	})
	return downed, err
}

func (s *Service[LK, LV, LQ]) up(ctx context.Context, oid blobcache.OID) (bccore.AnyObject, error) {
	if oid == (blobcache.OID{}) {
		panic("bcsys.up: root OID must not be loaded through up")
	}
	vinfo, err := s.inspectVolume(ctx, oid)
	if err != nil {
		return nil, err
	}
	if vinfo == nil {
		return nil, fmt.Errorf("non-volume object %v cannot be loaded", oid)
	}
	vol, err := s.makeVolume(ctx, oid, vinfo.Backend)
	if err != nil {
		return nil, err
	}
	return vol, nil
}

func (s *Service[LK, LV, LQ]) onLink(ctx context.Context, info blobcache.Info, ao bccore.AnyObject) error {
	switch ao := ao.(type) {
	case bccore.Volume:
		return s.env.MDS.Put(ctx, info.Volume.ID, AnyInfo{
			Volume: info.Volume,
		})
	case bccore.Queue:
		return s.env.MDS.Put(ctx, info.Queue.ID, AnyInfo{
			Queue: info.Queue,
		})
	default:
		return fmt.Errorf("cannot persist %v", ao)
	}
}

func (s *Service[LK, LV, LQ]) inspectVolume(ctx context.Context, volID blobcache.OID) (*blobcache.VolumeInfo, error) {
	var ainfo AnyInfo
	found, err := s.env.MDS.Get(ctx, volID, &ainfo)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return ainfo.Volume, nil
}

// Endpoint blocks waiting for a node to be created (happens when Serve is running).
// And then returns the Endpoint
func (s *Service[LK, LV, LQ]) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Endpoint"))
	defer logctx.Debug(ctx, "done", zap.String("method", "Endpoint"))
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
	return s.core.Drop(ctx, h)
}

func (s *Service[LK, LV, LQ]) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	return s.core.KeepAlive(ctx, hs)
}

func (s *Service[LK, LV, LQ]) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	return s.core.InspectHandle(ctx, h)
}

func (s *Service[LK, LV, LQ]) ShareOut(ctx context.Context, h blobcache.Handle, to blobcache.NodeID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	logctx.Info(ctx, "begin", zap.String("method", "Share"), zap.Stringer("oid", h.OID))
	defer logctx.Info(ctx, "done", zap.String("method", "Share"), zap.Stringer("oid", h.OID))
	out, err := s.core.Share(h, mask)
	if err != nil {
		return nil, err
	}
	key := derivePeerSecret(s.tmpSecret, to)
	ciph, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	ciph.Encrypt(out.Secret[:], out.Secret[:])
	return &out, nil
}

func (s *Service[LK, LV, LQ]) ShareIn(ctx context.Context, host blobcache.NodeID, h blobcache.Handle) (blobcache.Handle, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "ShareIn"), zap.Stringer("oid", h.OID), zap.Stringer("host", host))
	defer logctx.Debug(ctx, "done", zap.String("method", "ShareIn"), zap.Stringer("oid", h.OID), zap.Stringer("host", host))
	ep, info, err := s.inspectRemoteForShareIn(ctx, host, h)
	if err != nil {
		return blobcache.Handle{}, err
	}
	adopted, err := s.backends.remote.ShareIn(ctx, ep, h, info)
	if err != nil {
		return blobcache.Handle{}, err
	}

	var ao bccore.AnyObject
	var ttl time.Duration
	switch {
	case adopted.Volume != nil:
		ao = adopted.Volume
		ttl = DefaultVolumeTTL
	case adopted.Queue != nil:
		ao = adopted.Queue
		ttl = DefaultQueueTTL
	default:
		return blobcache.Handle{}, fmt.Errorf("cannot adopt unsupported object type")
	}
	createdAt := time.Now()
	oid := blobcache.RandomOID()
	return s.core.Create(ctx, oid, ao, blobcache.Action_ALL, createdAt, ttl)
}

func (s *Service[LK, LV, LQ]) Inspect(ctx context.Context, h blobcache.Handle) (blobcache.Info, error) {
	return s.core.Inspect(ctx, h)
}

func (s *Service[LK, LV, LQ]) inspectRemoteForShareIn(ctx context.Context, host blobcache.NodeID, h blobcache.Handle) (blobcache.Endpoint, blobcache.Info, error) {
	node, err := s.grabNode(ctx)
	if err != nil {
		return blobcache.Endpoint{}, blobcache.Info{}, err
	}
	if s.env.PeerLocator == nil {
		return blobcache.Endpoint{}, blobcache.Info{}, fmt.Errorf("cannot share-in remote object, no PeerLocator configured")
	}
	var lastErr error
	for addr := range s.env.PeerLocator.WhereIs(ctx, host) {
		ep := blobcache.Endpoint{Node: host, IPPort: addr}
		askCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		info, err := bcp.Inspect(askCtx, node, ep, h)
		cancel()
		if err == nil {
			if info.Tx != nil {
				return blobcache.Endpoint{}, blobcache.Info{}, fmt.Errorf("cannot share-in transaction handles")
			}
			return ep, info, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return blobcache.Endpoint{}, blobcache.Info{}, lastErr
	}
	return blobcache.Endpoint{}, blobcache.Info{}, fmt.Errorf("could not locate peer %s", host)
}

func (s *Service[LK, LV, LQ]) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	logctx.Info(ctx, "begin", zap.String("method", "OpenFiat"), zap.Stringer("oid", x))
	defer logctx.Info(ctx, "done", zap.String("method", "OpenFiat"), zap.Stringer("oid", x))
	if x == (blobcache.OID{}) {
		createdAt := time.Now()
		h := s.core.Mint(x, mask, createdAt, DefaultVolumeTTL)
		return &h, nil
	}
	vinfo, err := s.inspectVolume(ctx, x)
	if err != nil {
		return nil, err
	}
	if vinfo != nil {
		createdAt := time.Now()
		h := s.core.Mint(x, mask, createdAt, DefaultVolumeTTL)
		return &h, nil
	}
	createdAt := time.Now()
	h := s.core.Mint(x, mask, createdAt, DefaultQueueTTL)
	return &h, nil
}

func (s *Service[LK, LV, LQ]) OpenFrom(ctx context.Context, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	info, err := s.core.Inspect(ctx, base)
	if err != nil {
		return nil, err
	}
	if info.Volume != nil {
		var baseVol *remotebe.Volume
		switch {
		case info.Volume.Backend.Remote != nil:
			baseVol, err = s.backends.remote.VolumeUp(ctx, *info.Volume.Backend.Remote)
		case info.Volume.Backend.Peer != nil:
			baseVol, err = s.backends.peer.VolumeUp(ctx, *info.Volume.Backend.Peer)
		}
		if err != nil {
			return nil, err
		}
		if baseVol != nil {
			rights, subvol, err := s.backends.remote.OpenFrom(ctx, baseVol, ltok, mask)
			if err != nil {
				return nil, err
			}
			createdAt := time.Now()
			h, err := s.core.Create(ctx, blobcache.RandomOID(), subvol, rights, createdAt, DefaultVolumeTTL)
			if err != nil {
				return nil, err
			}
			return &h, nil
		}
	}
	return s.core.OpenFrom(ctx, base, ltok, mask)
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
	logctx.Info(ctx, "begin", zap.String("method", "CreateVolume"))
	defer logctx.Info(ctx, "done", zap.String("method", "CreateVolume"))
	if err := vspec.Validate(); err != nil {
		return nil, err
	}

	if host != nil && host.Node != s.LocalID() {
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

	if err := s.env.MDS.Put(ctx, oid, AnyInfo{Volume: &info}); err != nil {
		return nil, err
	}
	createdAt := time.Now()
	vol, err := s.makeVolume(ctx, info.ID, info.Backend)
	if err != nil {
		return nil, err
	}
	handle, err := s.core.Create(ctx, info.ID, vol, blobcache.Action_ALL, createdAt, DefaultVolumeTTL)
	if err != nil {
		return nil, err
	}
	return &handle, nil
}

// createRemoteVolume calls CreateVolume on a remote node
// it then creates a new local volume with a new random OID
func (s *Service[LK, LV, LQ]) createRemoteVolume(ctx context.Context, host blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	var vol *remotebe.Volume
	var err error
	if host.IPPort.IsValid() && host.IPPort.Port() != 0 {
		vol, err = s.backends.remote.CreateVolume(ctx, host, vspec)
		if err != nil {
			return nil, err
		}
	} else {
		vol, err = s.backends.peer.CreateVolume(ctx, host.Node, vspec)
		if err != nil {
			return nil, err
		}
	}
	// now we have a handle to the remote volume.
	// The handle is only valid on the remote node.
	id := blobcache.RandomOID()
	localHandle, err := s.core.Create(ctx, id, vol, blobcache.Action_ALL, time.Now(), DefaultVolumeTTL)
	if err != nil {
		return nil, err
	}
	return &localHandle, nil
}

func (s *Service[LK, LV, LQ]) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return s.core.InspectVolume(ctx, h)
}

func (s *Service[LK, LV, LQ]) BeginTx(ctx context.Context, volh blobcache.Handle, txspec blobcache.TxParams) (*blobcache.Handle, error) {
	return s.core.BeginTx(ctx, volh, txspec)
}

func (s *Service[LK, LV, LQ]) InspectTx(ctx context.Context, txh blobcache.Handle) (*blobcache.TxInfo, error) {
	return s.core.InspectTx(ctx, txh)
}

func (s *Service[LK, LV, LQ]) Save(ctx context.Context, txh blobcache.Handle, root []byte) error {
	return s.core.Save(ctx, txh, root)
}

func (s *Service[LK, LV, LQ]) Commit(ctx context.Context, txh blobcache.Handle) error {
	return s.core.Commit(ctx, txh)
}

func (s *Service[LK, LV, LQ]) Abort(ctx context.Context, txh blobcache.Handle) error {
	return s.core.Abort(ctx, txh)
}

func (s *Service[LK, LV, LQ]) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	return s.core.Load(ctx, txh, dst)
}

func (s *Service[LK, LV, LQ]) Post(ctx context.Context, txh blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return s.core.Post(ctx, txh, data, opts)
}

func (s *Service[LV, LK, LQ]) Exists(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return s.core.Exists(ctx, txh, cids, dst)
}

func (s *Service[LV, LK, LQ]) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return s.core.Get(ctx, txh, cid, buf, opts)
}

func (s *Service[LV, LK, LQ]) Delete(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	return s.core.Delete(ctx, txh, cids)
}

func (s *Service[LK, LV, LQ]) Copy(ctx context.Context, txh blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, out []bool) error {
	return s.core.Copy(ctx, txh, srcTxns, cids, out)
}

func (s *Service[LK, LV, LQ]) Visit(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	return s.core.Visit(ctx, txh, cids)
}

func (s *Service[LK, LV, LQ]) IsVisited(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return s.core.IsVisited(ctx, txh, cids, dst)
}

func (s *Service[LK, LV, LQ]) Link(ctx context.Context, txh blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	return s.core.Link(ctx, txh, target, mask)
}

func (s *Service[LK, LV, LQ]) Unlink(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkTokenID) error {
	return s.core.Unlink(ctx, txh, targets)
}

func (s *Service[LK, LV, LQ]) VisitLinks(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkTokenID) error {
	return s.core.VisitLinks(ctx, txh, targets)
}

func (s *Service[LK, LV, LQ]) CreateQueue(ctx context.Context, host *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	logctx.Info(ctx, "begin", zap.String("method", "CreateQueue"))
	defer logctx.Info(ctx, "done", zap.String("method", "CreateQueue"))
	if host != nil && host.Node != s.LocalID() {
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
	createdAt := time.Now()
	oid := blobcache.RandomOID()
	handle, err := s.core.Create(ctx, oid, queue, blobcache.Action_ALL, createdAt, DefaultQueueTTL)
	if err != nil {
		return nil, err
	}
	return &handle, nil
}

// createRemoteQueue calls CreateQueue on a remote node
// it then creates a local proxy queue with a new random OID
func (s *Service[LK, LV, LQ]) createRemoteQueue(ctx context.Context, host blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	q, _, err := s.backends.remote.CreateQueue(ctx, host, qspec)
	if err != nil {
		return nil, err
	}
	oid := q.Backend().Remote.OID
	handle, err := s.core.Create(ctx, oid, q, blobcache.Action_ALL, time.Now(), DefaultQueueTTL)
	if err != nil {
		return nil, err
	}
	return &handle, nil
}

func (s *Service[LK, LV, LQ]) InspectQueue(ctx context.Context, qh blobcache.Handle) (blobcache.QueueInfo, error) {
	return s.core.InspectQueue(ctx, qh)
}

func (s *Service[LK, LV, LQ]) Dequeue(ctx context.Context, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	return s.core.Dequeue(ctx, qh, buf, opts)
}

func (s *Service[LK, LV, LQ]) Enqueue(ctx context.Context, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	return s.core.Enqueue(ctx, qh, msgs)
}

func (s *Service[LK, LV, LQ]) SubToVolume(ctx context.Context, qh blobcache.Handle, volh blobcache.Handle, spec blobcache.VolSubSpec) error {
	logctx.Debug(ctx, "begin", zap.String("method", "SubToVolume"), zap.Stringer("oid", qh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "SubToVolume"), zap.Stringer("oid", qh.OID))
	q, err := s.core.ResolveQueue(qh)
	if err != nil {
		return err
	}
	vol, err := s.core.ResolveVol(volh)
	if err != nil {
		return err
	}
	switch vol := vol.(type) {
	case *remotebe.Volume:
		return s.backends.remote.SubToVol(ctx, vol, q, spec)
	default:
		_, ok := vol.(LV)
		if !ok {
			return fmt.Errorf("subscribe to volume not supported on volume=%T and queue=%T", vol, q)
		}
		return s.core.SubToVolume(ctx, qh, volh, spec)
	}
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
	case volBackend.Peer != nil:
		return s.backends.peer.VolumeUp(ctx, *volBackend.Peer)
	case volBackend.Git != nil:
		return s.makeGit(ctx, *volBackend.Git)
	case volBackend.Vault != nil:
		return s.makeVault(ctx, *volBackend.Vault)
	default:
		return nil, fmt.Errorf("empty backend")
	}
}

func (s *Service[LK, LV, LQ]) makeLocal(ctx context.Context, oid blobcache.OID, lk LK) (backend.Volume, error) {
	var ainfo AnyInfo
	found, err := s.env.MDS.Get(ctx, oid, &ainfo)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("makeLocal: volume entry not found")
	}
	return s.backends.local.VolumeUp(ctx, LVParams[LK]{
		Key:    lk,
		Params: ainfo.Volume.VolumeConfig,
	})
}

func (s *Service[LK, LV, LQ]) makeGit(ctx context.Context, backend blobcache.VolumeBackend_Git) (backend.Volume, error) {
	return nil, fmt.Errorf("git volumes are not yet supported")
}

func (s *Service[LK, LV, LQ]) makeVault(ctx context.Context, backend blobcache.VolumeBackend_Vault[blobcache.OID]) (*vaultvol.Vault, error) {
	volInfo, err := s.inspectVolume(ctx, backend.X)
	if err != nil {
		return nil, err
	}
	if volInfo == nil {
		return nil, fmt.Errorf("inner volume not found: %v", backend.X)
	}
	inner, err := s.makeVolume(ctx, backend.X, volInfo.Backend)
	if err != nil {
		return nil, err
	}
	return vaultvol.New(inner, backend.Secret, backend.HashAlgo.KeyedHash), nil
}

func (s *Service[LK, LV, LQ]) findVolumeParams(ctx context.Context, vspec blobcache.VolumeSpec) (blobcache.VolumeConfig, error) {
	switch {
	case vspec.Local != nil:
		cfg := vspec.Config()
		if cfg.HashAlgo == "" {
			cfg.HashAlgo = blobcache.HashAlgo_BLAKE3_256
		}
		return cfg, nil
	case vspec.Git != nil:
		return vspec.Config(), nil
	case vspec.Remote != nil:
		vol, err := s.backends.remote.VolumeUp(ctx, *vspec.Remote)
		if err != nil {
			return blobcache.VolumeConfig{}, err
		}
		return vol.GetParams(), nil
	case vspec.Peer != nil:
		vol, err := s.backends.peer.VolumeUp(ctx, *vspec.Peer)
		if err != nil {
			return blobcache.VolumeConfig{}, err
		}
		return vol.GetParams(), nil

	case vspec.Vault != nil:
		innerVol, err := s.core.InspectVolume(ctx, vspec.Vault.X)
		if err != nil {
			return blobcache.VolumeConfig{}, err
		}
		return innerVol.VolumeConfig, nil
	default:
		panic(vspec)
	}
}
