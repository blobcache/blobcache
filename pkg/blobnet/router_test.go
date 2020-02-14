package blobnet

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p2ptest"
	"github.com/brendoncarroll/go-p2p/s/memswarm"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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
		peerStore := make(MemPeerStore)
		for _, addr := range adjList[i] {
			id := p2p.NewPeerID(swarms[i].LookupPublicKey(addr))
			peerStore.AddAddr(id, addr)
		}
		routers[i] = NewRouter(RouterParams{
			Swarm:     swarms[i],
			PeerStore: peerStore,
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
			assert.Len(t, r.MultiHop(), N-2)
		} else {
			assert.Len(t, r.MultiHop(), N-3)
		}
	}
}

type MemPeerStore map[p2p.PeerID][]p2p.Addr

func (ps MemPeerStore) AddAddr(id p2p.PeerID, addr p2p.Addr) {
	addrs := ps[id]
	addrs = append(addrs, addr)
	ps[id] = addrs
}

func (ps MemPeerStore) GetAddrs(id p2p.PeerID) []p2p.Addr {
	return ps[id]
}

func (ps MemPeerStore) ListPeers() []p2p.PeerID {
	ids := []p2p.PeerID{}
	for id := range ps {
		ids = append(ids, id)
	}
	return ids
}

func (ps MemPeerStore) TrustFor(id p2p.PeerID) (int64, error) {
	return 0, nil
}
