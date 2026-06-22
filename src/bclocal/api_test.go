package bclocal_test

import (
	"context"
	"iter"
	"net/netip"
	"sync"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestAPI(t *testing.T) {
	t.Parallel()
	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		svc := bclocal.NewTestService(t)
		return svc
	})
}

func TestMultiNode(t *testing.T) {
	t.Parallel()
	blobcachetests.TestMultiNode(t, func(t testing.TB, n int) []blobcache.Service {
		ctx := testutil.Context(t)
		loc := &testPeerLocator{}
		svcs := make([]blobcache.Service, n)
		for i := range svcs {
			env := bclocal.NewTestEnv(t)
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
		return svcs
	})
}

// testPeerLocator is a PeerLocator that maps PeerIDs to addresses
// based on endpoints registered via Add.
type testPeerLocator struct {
	mu                 sync.RWMutex
	peers              map[blobcache.NodeID]netip.AddrPort
	injectBadAddrFirst bool
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
		if l.injectBadAddrFirst {
			// Return a bad address first to exercise retry logic.
			badAddr := netip.MustParseAddrPort("127.0.0.1:1")
			if !yield(badAddr) {
				return
			}
		}
		l.mu.RLock()
		addr, ok := l.peers[peer]
		l.mu.RUnlock()
		if ok {
			yield(addr)
		}
	}
}

func TestManyBlobs(t *testing.T) {
	t.Parallel()
	blobcachetests.TestManyBlobs(t, false, func(t testing.TB) blobcache.Service {
		return bclocal.NewTestService(t)
	})
}
