// Package bcremote implements a blobcache.Service over Blobcache Network Protocol (BCNP).
// This is useful for applications written in Go, or that have a QUIC implementation.
package bcremote

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/cloudflare/circl/sign/ed25519"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/bcp"
)

// Dial starts listening via UDP on any available port, then
// calls New to create a Service.
func Dial(privateKey ed25519.PrivateKey, ep blobcache.Endpoint) (*Service, error) {
	pc, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	return New(privateKey, pc, ep), nil
}

// New creates a Service, which is implemented remotely.
// Endpoint describes the service to connect to.
// Connections will be established out of pc.
// privateKey is used to authenticate to the service.
func New(privateKey ed25519.PrivateKey, pc net.PacketConn, ep blobcache.Endpoint) *Service {
	cache, _ := lru.New[blobcache.OID, *blobcache.TxInfo](128)
	bgCtx, bgCtxCancel := context.WithCancel(context.Background())
	s := &Service{
		ep:   ep,
		node: bcnet.New(privateKey, pc),
		pc:   pc,

		cache: cache,
		cf:    bgCtxCancel,
	}
	s.start(bgCtx)
	return s
}

var _ blobcache.Service = &Service{}

type Service struct {
	ep   blobcache.Endpoint
	node *bcnet.Node
	pc   net.PacketConn

	cache *lru.Cache[blobcache.OID, *blobcache.TxInfo]
	eg    errgroup.Group
	cf    context.CancelFunc
}

// Close closes the Service.
func (s *Service) Close() error {
	s.cf()
	if err := s.pc.Close(); err != nil {
		return err
	}
	return s.eg.Wait()
}

// AwaitReady pings the server in a loop and awaits a response.
// Any error returned will be from ctx.Err()
func (s *Service) AwaitReady(ctx context.Context) error {
	tick := time.NewTicker(time.Second / 8)
	defer tick.Stop()
	for {
		if err := bcp.Ping(ctx, s.node, s.ep); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (s *Service) start(bgCtx context.Context) {
	s.eg.Go(func() error {
		return s.node.Serve(bgCtx, &bcp.Server{
			Access: func(peer blobcache.PeerID) blobcache.Service { return nil },
		})
	})
}

// Endpoint returns the endpoint of the remote service.
func (s *Service) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	return s.ep, nil
}

func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	return bcp.Drop(ctx, s.node, s.ep, h)
}

func (s *Service) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	return bcp.KeepAlive(ctx, s.node, s.ep, hs)
}

func (s *Service) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return bcp.Share(ctx, s.node, s.ep, h, to, mask)
}

func (s *Service) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	return bcp.InspectHandle(ctx, s.node, s.ep, h)
}

func (s *Service) OpenFiat(ctx context.Context, target blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	h, _, err := bcp.OpenFiat(ctx, s.node, s.ep, target, mask)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (s *Service) OpenFrom(ctx context.Context, base blobcache.Handle, token blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	h, _, err := bcp.OpenFrom(ctx, s.node, s.ep, base, token, mask)
	return h, err
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	h, info, err := bcp.BeginTx(ctx, s.node, s.ep, volh, txp)
	if err != nil {
		return nil, err
	}
	s.cache.Add(h.OID, info)
	return h, nil
}

// CreateVolume creates a new volume.
func (s *Service) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if host != nil && *host != s.ep {
		return nil, fmt.Errorf("bcremote: caller cannot be different from the node ID")
	}
	return bcp.CreateVolume(ctx, s.node, s.ep, vspec)
}

// InspectVolume returns info about a Volume.
func (s *Service) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return bcp.InspectVolume(ctx, s.node, s.ep, h)
}

func (s *Service) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	return bcp.CloneVolume(ctx, s.node, s.ep, caller, volh)
}

// InspectTx returns info about a transaction.
func (s *Service) InspectTx(ctx context.Context, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	return bcp.InspectTx(ctx, s.node, s.ep, tx)
}

// Commit commits a transaction.
func (s *Service) Commit(ctx context.Context, tx blobcache.Handle) error {
	return bcp.Commit(ctx, s.node, s.ep, tx, nil)
}

// Abort aborts a transaction.
func (s *Service) Abort(ctx context.Context, tx blobcache.Handle) error {
	return bcp.Abort(ctx, s.node, s.ep, tx)
}

// Load loads the volume root into dst
func (s *Service) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	return bcp.Load(ctx, s.node, s.ep, tx, dst)
}

// Save writes to the volume root.
func (s *Service) Save(ctx context.Context, tx blobcache.Handle, src []byte) error {
	return bcp.Save(ctx, s.node, s.ep, tx, src)
}

// Post posts data to the volume
func (s *Service) Post(ctx context.Context, tx blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return bcp.Post(ctx, s.node, s.ep, tx, opts.Salt, data)
}

// Exists checks if a CID exists in the volume
func (s *Service) Exists(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	return bcp.Exists(ctx, s.node, s.ep, tx, cids, dst)
}

// Delete deletes a CID from the volume
func (s *Service) Delete(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return bcp.Delete(ctx, s.node, s.ep, tx, cids)
}

func (s *Service) Copy(ctx context.Context, tx blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, success []bool) error {
	if len(cids) != len(success) {
		return fmt.Errorf("cids and success must have the same length")
	}
	return bcp.AddFrom(ctx, s.node, s.ep, tx, cids, srcTxns, success)
}

// Get returns the data for a CID.
func (s *Service) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	hf, err := s.getHashFunc(ctx, tx)
	if err != nil {
		return 0, err
	}
	return bcp.Get(ctx, s.node, s.ep, tx, hf, cid, opts.Salt, buf)
}

func (s *Service) Visit(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return bcp.Visit(ctx, s.node, s.ep, tx, cids)
}

func (s *Service) IsVisited(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return bcp.IsVisited(ctx, s.node, s.ep, tx, cids, dst)
}

// Link allows the Volume to reference another volume.
func (s *Service) Link(ctx context.Context, tx blobcache.Handle, subvol blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	return bcp.Link(ctx, s.node, s.ep, tx, subvol, mask)
}

func (s *Service) Unlink(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	return bcp.Unlink(ctx, s.node, s.ep, tx, targets)
}

func (s *Service) VisitLinks(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	return bcp.VisitLinks(ctx, s.node, s.ep, tx, targets)
}

func (s *Service) CreateQueue(ctx context.Context, _ *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	return bcp.CreateQueue(ctx, s.node, s.ep, qspec)
}

func (s *Service) InspectQueue(ctx context.Context, qh blobcache.Handle) (blobcache.QueueInfo, error) {
	return bcp.InspectQueue(ctx, s.node, s.ep, qh)
}

func (s *Service) Dequeue(ctx context.Context, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	return bcp.Dequeue(ctx, s.node, s.ep, qh, buf, opts)
}

func (s *Service) Enqueue(ctx context.Context, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	return bcp.Enqueue(ctx, s.node, s.ep, qh, msgs)
}

func (s *Service) SubToVolume(ctx context.Context, qh blobcache.Handle, volh blobcache.Handle) error {
	return bcp.SubToVolume(ctx, s.node, s.ep, qh, volh)
}

// getHashFunc finds the hash function for a transaction.
func (s *Service) getHashFunc(ctx context.Context, txh blobcache.Handle) (blobcache.HashFunc, error) {
	txinfo, ok := s.cache.Get(txh.OID)
	if ok {
		return txinfo.HashAlgo.HashFunc(), nil
	}
	txinfo, err := bcp.InspectTx(ctx, s.node, s.ep, txh)
	if err != nil {
		return nil, err
	}
	s.cache.Add(txh.OID, txinfo)
	return txinfo.HashAlgo.HashFunc(), nil
}
