package p2pservices

import (
	"context"
	"testing"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/p2p/p2ptest"
	"github.com/stretchr/testify/assert"
)

func TestHeartbeat(t *testing.T) {
	ctx, cf := context.WithCancel(context.TODO())
	defer cf()

	r := &p2ptest.Realm{}
	nodes := p2ptest.Cluster(r, 2)
	hbs := make([]*HeartbeatService, len(nodes))
	for i, n := range nodes {
		hbs[i] = NewHeartbeat(n.Swarm(), time.Second, 2*time.Second)
		go n.Run(ctx)
		go hbs[i].Run(ctx)
	}

	// check ask
	assert.True(t, hbs[0].Ping(ctx, nodes[1].LocalID()))

	// check tell
	time.Sleep(time.Second * 2)
	assert.True(t, hbs[0].IsAlive(nodes[1].LocalID()))
}
