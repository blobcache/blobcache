package blobnet

import (
	"testing"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	"github.com/brendoncarroll/go-p2p/p2ptest"
	"github.com/brendoncarroll/go-p2p/s/memswarm"
)

func TestBlobnet(t *testing.T) {
	const N = 10
	realm := memswarm.NewRealm()
	swarms := make([]p2p.SecureAskSwarm, N)
	for i := range swarms {
		swarms[i] = realm.NewSwarmWithKey(p2ptest.GetTestKey(t, i))
	}

	adjList := p2ptest.Chain(p2ptest.CastSlice(swarms))

	bns := make([]*Blobnet, N)
	for i := range swarms {
		peerStore := make(MemPeerStore)
		for _, addr := range adjList[i] {
			id := p2p.NewPeerID(swarms[i].LookupPublicKey(addr))
			peerStore.AddAddr(id, addr)
		}
		bns[i] = makeBlobnet(swarms[i], peerStore)
	}

	// for i := range bns {
	// 	bns[i].bootstrap(context.TODO())
	// }
}

func makeBlobnet(s p2p.SecureAskSwarm, ps PeerStore) *Blobnet {
	mux := simplemux.MultiplexSwarm(s)
	bn := NewBlobNet(Params{
		PeerStore: ps,
		Mux:       mux,
		KV:        &bckv.MemKV{Capacity: 100},
		Local:     bckv.BlobAdapter(&bckv.MemKV{Capacity: 100}),
	})
	return bn
}
