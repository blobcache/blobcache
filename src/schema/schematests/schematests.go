package schematests

import (
	"context"
	"fmt"
	"iter"
	"net/netip"
	"sync"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/schema"
	"github.com/stretchr/testify/require"
)

func Factory(schs Schemas) schema.Factory {
	var mkSchema func(spec blobcache.SchemaSpec) (schema.Schema, error)
	mkSchema = func(spec blobcache.SchemaSpec) (schema.Schema, error) {
		cons, exists := schs[spec.Name]
		if !exists {
			return nil, fmt.Errorf("schema %s not found", spec.Name)
		}
		return cons(spec.Params, mkSchema)
	}
	return mkSchema
}

type Schemas = map[blobcache.SchemaName]schema.Constructor

// InitNodes initializes svcs
func InitNodes(t testing.TB, schs Schemas, svcs []blobcache.Service) {
	t.Helper()
	ctx := context.Background()
	loc := &testPeerLocator{}
	for i := range svcs {
		env := bclocal.NewTestEnv(t)
		env.MkSchema = Factory(schs)
		env.PeerLocator = loc
		svc := bclocal.NewTestServiceFromEnv(t, env)
		for j := range svcs[:i] {
			ep, err := svcs[j].Endpoint(ctx)
			require.NoError(t, err)
			require.NoError(t, svc.Ping(ctx, ep))
		}
		ep, err := svc.Endpoint(ctx)
		require.NoError(t, err)
		loc.Add(ep)
		svcs[i] = svc
	}
}

type testPeerLocator struct {
	mu    sync.RWMutex
	peers map[blobcache.NodeID]netip.AddrPort
}

func (l *testPeerLocator) Add(ep blobcache.Endpoint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.peers == nil {
		l.peers = make(map[blobcache.NodeID]netip.AddrPort)
	}
	l.peers[ep.Node] = ep.IPPort
}

func (l *testPeerLocator) WhereIs(_ context.Context, peer blobcache.NodeID) iter.Seq[netip.AddrPort] {
	return func(yield func(netip.AddrPort) bool) {
		l.mu.RLock()
		addr, ok := l.peers[peer]
		l.mu.RUnlock()
		if ok {
			yield(addr)
		}
	}
}

// Setup performs generic setup to prepare a Volume with the desired schema.
func Setup(t testing.TB, schs Schemas, vspec blobcache.VolumeBackend_Local) (blobcache.Service, blobcache.Handle) {
	t.Helper()
	env := bclocal.NewTestEnv(t)
	env.MkSchema = Factory(schs)
	svc := bclocal.NewTestServiceFromEnv(t, env)
	volh := blobcachetests.CreateVolume(t, svc, nil, blobcache.VolumeSpec{Local: &vspec})
	return svc, volh
}
