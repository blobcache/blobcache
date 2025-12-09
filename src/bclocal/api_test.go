package bclocal_test

import (
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/jsonns"
	"github.com/stretchr/testify/require"
)

// TestDefaultNoAccess tests that a remote peer cannot perform any
// actions on the local service by default.
func TestDefaultNoAccess(t *testing.T) {
	ctx := testutil.Context(t)
	svc1 := bclocal.NewTestService(t)
	svc2 := bclocal.NewTestService(t)

	// create a volume on svc1 so there is something to try to access.
	volh, err := svc1.CreateVolume(ctx, nil, blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			HashAlgo: blobcache.HashAlgo_BLAKE3_256,
			MaxSize:  1 << 20,
		},
	})
	require.NoError(t, err)
	nsc1 := schema.NSClient{Service: svc1, Schema: jsonns.Schema{}}
	require.NoError(t, err)
	require.NoError(t, nsc1.Put(ctx, blobcache.Handle{}, "name1", *volh, blobcache.Action_ALL))

	nsc2 := schema.NSClient{Service: svc2, Schema: jsonns.Schema{}}
	var entry schema.NSEntry
	found, err := nsc2.Get(ctx, blobcache.Handle{}, "name1", &entry)
	require.NoError(t, err)
	require.False(t, found)

	err = nsc2.Put(ctx, *volh, "any name", blobcache.Handle{}, blobcache.Action_ALL)
	require.Error(t, err)

	names, err := nsc2.ListNames(ctx, *volh)
	require.Error(t, err)
	require.Empty(t, names)
}

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
		svcs := make([]blobcache.Service, n)
		for i := range svcs {
			// NewTestService will use an allowAllPolicy
			svc := bclocal.NewTestService(t)
			for j := range svcs[:i] {
				ep, err := svcs[j].Endpoint(ctx)
				require.NoError(t, err)
				require.NoError(t, svc.Ping(ctx, ep))
			}
			svcs[i] = svc
		}
		return svcs
	})
}

func TestManyBlobs(t *testing.T) {
	t.Parallel()
	blobcachetests.TestManyBlobs(t, false, func(t testing.TB) blobcache.Service {
		return bclocal.NewTestService(t)
	})
}
