// Package bcremote implements a blobcache.Service over Blobcache Network Protocol (BCNP).
// This is useful for applications written in Go, or that have a QUIC implementation.
package bcremote

import (
	"context"
	"fmt"
	"net"

	"github.com/cloudflare/circl/sign/ed25519"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
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
		ep:    ep,
		node:  bcnet.New(privateKey, pc),
		pc:    pc,
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

func (s *Service) start(bgCtx context.Context) {
	s.eg.Go(func() error {
		return s.node.Serve(bgCtx, bcnet.Server{
			Access: func(peer blobcache.PeerID) blobcache.Service { return nil },
		})
	})
}

// Endpoint returns the endpoint of the remote service.
func (s *Service) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	return s.ep, nil
}

func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	return bcnet.Drop(ctx, s.node, s.ep, h)
}

func (s *Service) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	return bcnet.KeepAlive(ctx, s.node, s.ep, hs)
}

func (s *Service) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	return bcnet.InspectHandle(ctx, s.node, s.ep, h)
}

func (s *Service) OpenAs(ctx context.Context, caller *blobcache.PeerID, target blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return bcnet.OpenAs(ctx, s.node, s.ep, caller, target, mask)
}

func (s *Service) OpenFrom(ctx context.Context, base blobcache.Handle, target blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return bcnet.OpenFrom(ctx, s.node, s.ep, base, target, mask)
}

func (s *Service) Await(ctx context.Context, cond blobcache.Conditions) error {
	return bcnet.Await(ctx, s.node, s.ep, cond)
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	return bcnet.BeginTx(ctx, s.node, s.ep, volh, txp)
}

// CreateVolume creates a new volume.
func (s *Service) CreateVolume(ctx context.Context, caller *blobcache.PeerID, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	if caller != nil && *caller != s.node.LocalID() {
		return nil, fmt.Errorf("bcremote: caller cannot be different from the node ID")
	}
	return bcnet.CreateVolume(ctx, s.node, s.ep, caller, vspec)
}

// InspectVolume returns info about a Volume.
func (s *Service) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return bcnet.InspectVolume(ctx, s.node, s.ep, h)
}

func (s *Service) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	return bcnet.CloneVolume(ctx, s.node, s.ep, caller, volh)
}

// InspectTx returns info about a transaction.
func (s *Service) InspectTx(ctx context.Context, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	return bcnet.InspectTx(ctx, s.node, s.ep, tx)
}

// Commit commits a transaction.
func (s *Service) Commit(ctx context.Context, tx blobcache.Handle) error {
	return bcnet.Commit(ctx, s.node, s.ep, tx, nil)
}

// Abort aborts a transaction.
func (s *Service) Abort(ctx context.Context, tx blobcache.Handle) error {
	return bcnet.Abort(ctx, s.node, s.ep, tx)
}

// Load loads the volume root into dst
func (s *Service) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	return bcnet.Load(ctx, s.node, s.ep, tx, dst)
}

// Save writes to the volume root.
func (s *Service) Save(ctx context.Context, tx blobcache.Handle, src []byte) error {
	return bcnet.Save(ctx, s.node, s.ep, tx, src)
}

// Post posts data to the volume
func (s *Service) Post(ctx context.Context, tx blobcache.Handle, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	return bcnet.Post(ctx, s.node, s.ep, tx, salt, data)
}

// Exists checks if a CID exists in the volume
func (s *Service) Exists(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	return bcnet.Exists(ctx, s.node, s.ep, tx, cids, dst)
}

// Delete deletes a CID from the volume
func (s *Service) Delete(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return bcnet.Delete(ctx, s.node, s.ep, tx, cids)
}

func (s *Service) AddFrom(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, srcTxns []blobcache.Handle, success []bool) error {
	return bcnet.AddFrom(ctx, s.node, s.ep, tx, cids, srcTxns, success)
}

// Get returns the data for a CID.
func (s *Service) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	hf, err := s.getHashFunc(ctx, tx)
	if err != nil {
		return 0, err
	}
	return bcnet.Get(ctx, s.node, s.ep, tx, hf, cid, salt, buf)
}

// AllowLink allows the Volume to reference another volume.
func (s *Service) AllowLink(ctx context.Context, tx blobcache.Handle, subvol blobcache.Handle) error {
	return bcnet.AllowLink(ctx, s.node, s.ep, tx, subvol)
}

func (s *Service) Visit(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return bcnet.Visit(ctx, s.node, s.ep, tx, cids)
}

func (s *Service) IsVisited(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return bcnet.IsVisited(ctx, s.node, s.ep, tx, cids, dst)
}

// getHashFunc finds the hash function for a transaction.
func (s *Service) getHashFunc(ctx context.Context, txh blobcache.Handle) (blobcache.HashFunc, error) {
	txinfo, ok := s.cache.Get(txh.OID)
	if ok {
		return txinfo.HashAlgo.HashFunc(), nil
	}
	txinfo, err := bcnet.InspectTx(ctx, s.node, s.ep, txh)
	if err != nil {
		return nil, err
	}
	s.cache.Add(txh.OID, txinfo)
	return txinfo.HashAlgo.HashFunc(), nil
}
