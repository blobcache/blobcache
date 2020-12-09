package blobcachecmd

import (
	"context"
	"sync"

	"github.com/blobcache/blobcache/pkg/bchttp"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/brendoncarroll/go-p2p"
	"golang.org/x/sync/errgroup"
)

type Daemon struct {
	localID   p2p.PeerID
	node      *blobcache.Node
	peerStore *peerStore
	apiServer *bchttp.Server
}

type DaemonParams struct {
	BlobcacheParams blobcache.Params
	Swarm           p2p.SecureAskSwarm
	APIAddr         string
	PeerStore       *peerStore
}

func NewDaemon(params DaemonParams) *Daemon {
	node := blobcache.NewNode(params.BlobcacheParams)
	return &Daemon{
		peerStore: params.PeerStore,

		node:      node,
		localID:   p2p.NewPeerID(params.BlobcacheParams.PrivateKey.Public()),
		apiServer: bchttp.NewServer(node, params.APIAddr),
	}
}

func (d *Daemon) Run(ctx context.Context) error {
	group := errgroup.Group{}
	group.Go(func() error {
		return d.runAPI(ctx)
	})
	return group.Wait()
}

func (d *Daemon) runAPI(ctx context.Context) error {
	return d.apiServer.Run(ctx)
}

func (d *Daemon) Close() error {
	return nil
}

var _ blobcache.PeerStore = &peerStore{}

type peerStore struct {
	mu                        sync.Mutex
	staticAddrs, dynamicAddrs map[p2p.PeerID][]p2p.Addr
	trustFor                  map[p2p.PeerID]int64
}

func newPeerStore(swarm p2p.Swarm, specs []peers.PeerSpec) (*peerStore, error) {
	staticAddrs := make(map[p2p.PeerID][]p2p.Addr)
	trustFor := make(map[p2p.PeerID]int64)
	for _, spec := range specs {
		staticAddrs[spec.ID] = []p2p.Addr{}
		trustFor[spec.ID] = spec.Trust
		for _, addrStr := range spec.Addrs {
			addr, err := swarm.ParseAddr([]byte(addrStr))
			if err != nil {
				return nil, err
			}
			staticAddrs[spec.ID] = append(staticAddrs[spec.ID], addr)
		}
	}
	return &peerStore{
		staticAddrs:  staticAddrs,
		dynamicAddrs: make(map[p2p.PeerID][]p2p.Addr),
		trustFor:     trustFor,
	}, nil
}

func (s *peerStore) AddAddrs(id p2p.PeerID, addrs []p2p.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dynamicAddrs[id] = addrs
}

func (s *peerStore) GetAddrs(id p2p.PeerID) []p2p.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(s.staticAddrs[id], s.dynamicAddrs[id]...)
}

func (s *peerStore) ListPeers() []p2p.PeerID {
	s.mu.Lock()
	defer s.mu.Unlock()
	peerIDs := []p2p.PeerID{}
	for peerID := range s.trustFor {
		peerIDs = append(peerIDs, peerID)
	}
	return peerIDs
}

func (s *peerStore) TrustFor(id p2p.PeerID) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trustFor[id], nil
}
