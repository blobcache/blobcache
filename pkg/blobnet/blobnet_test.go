package blobnet

import (
	"context"
	"testing"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/brendoncarroll/go-p2p/p2ptest"
	"github.com/brendoncarroll/go-p2p/s/memswarm"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestBlobnet(t *testing.T) {
	const N = 10
	realm := memswarm.NewRealm()
	swarms := make([]p2p.SecureAskSwarm, N)
	for i := range swarms {
		swarms[i] = realm.NewSwarmWithKey(p2ptest.NewTestKey(t, i))
	}

	adjList := p2ptest.MakeChain(len(swarms))

	bns := make([]*Blobnet, N)
	for i := range swarms {
		peerStore := make(peers.MemPeerStore)
		for _, j := range adjList[i] {
			addr := swarms[j].LocalAddrs()[0]
			pubKey, err := swarms[i].LookupPublicKey(context.TODO(), addr)
			require.NoError(t, err)
			id := p2p.NewPeerID(pubKey)
			peerStore.AddAddr(id, addr)
		}
		bns[i] = makeBlobnet(swarms[i], peerStore)
	}
}

func makeBlobnet(s p2p.SecureAskSwarm, ps peers.PeerStore) *Blobnet {
	mux := p2pmux.NewStringSecureAskMux(s)
	bn := NewBlobNet(Params{
		PeerStore: ps,
		Mux:       mux,
		DB:        &bcdb.MemDB{},
		Local:     bcdb.BlobAdapter(&bcdb.MemKV{Capacity: 100}, 1<<22, cadata.DefaultHash),
		Clock:     clockwork.NewRealClock(),
	})
	return bn
}
