package peerrouting

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p2ptest"
	"github.com/brendoncarroll/go-p2p/s/memswarm"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/blobcache/blobcache/pkg/blobnet/peers"
)

func init() {
	logrus.SetFormatter(&logrus.TextFormatter{
		ForceColors: true,
	})
	logrus.SetLevel(logrus.DebugLevel)
}

func TestRouter(t *testing.T) {
	const N = 10
	realm := memswarm.NewRealm()
	swarms := make([]p2p.SecureAskSwarm, N)
	for i := range swarms {
		swarms[i] = realm.NewSwarmWithKey(p2ptest.GetTestKey(t, i))
	}

	adjList := p2ptest.Chain(p2ptest.CastSlice(swarms))

	routers := make([]*Router, N)
	for i := range swarms {
		peerStore := make(peers.MemPeerStore)
		for _, addr := range adjList[i] {
			id := p2p.NewPeerID(swarms[i].LookupPublicKey(addr))
			peerStore.AddAddr(id, addr)
		}
		routers[i] = NewRouter(RouterParams{
			PeerSwarm: peers.NewPeerSwarm(swarms[i], peerStore),
			CacheSize: N,
			Clock:     clockwork.NewRealClock(),
		})
	}

	for i := 0; i < N; i++ {
		for _, r := range routers {
			r.queryPeers(context.TODO())
		}
	}

	for i, r := range routers {
		t.Log(r.peerSwarm.LocalID(), r.OneHop(), r.MultiHop())

		if i == 0 || i == len(routers)-1 {
			assert.Len(t, r.OneHop(), 1)
			assert.Len(t, r.MultiHop(), N-2)
		} else {
			assert.Len(t, r.OneHop(), 2)
			assert.Len(t, r.MultiHop(), N-3)
		}
	}
}
