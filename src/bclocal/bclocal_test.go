package bclocal

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/simplens"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSetupDB(t *testing.T) {
	ctx := testutil.Context(t)
	db := dbutil.OpenMemory()
	if err := SetupDB(ctx, db); err != nil {
		t.Fatal(err)
	}
}

func TestNewService(t *testing.T) {
	NewTestService(t)
}

func TestAPI(t *testing.T) {
	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		svc := NewTestService(t)
		return svc
	})
}

func TestMultiNode(t *testing.T) {
	blobcachetests.TestMultiNode(t, func(t testing.TB, n int) []blobcache.Service {
		svcs := make([]blobcache.Service, n)
		for i := range svcs {
			svcs[i] = NewTestService(t)
		}
		for i := range svcs {
			for j := range svcs {
				if i == j {
					continue
				}
				peerID := svcs[j].(*Service).node.LocalID()
				svcs[i].(*Service).env.ACL.Owners = append(svcs[i].(*Service).env.ACL.Owners, peerID)
			}
		}
		return svcs
	})
}

// TestDefaultNoAccess tests that a remote peer cannot perform any
// actions on the local service by default.
func TestDefaultNoAccess(t *testing.T) {
	ctx := testutil.Context(t)
	svc1 := NewTestService(t)
	svc2 := NewTestService(t)

	// create a volume on svc1 so there is something to try to access.
	volh, err := svc1.CreateVolume(ctx, nil, blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			VolumeParams: blobcache.VolumeParams{
				HashAlgo: blobcache.HashAlgo_BLAKE3_256,
				MaxSize:  1 << 20,
			},
		},
	})
	require.NoError(t, err)
	nsc1 := simplens.Client{Service: svc1}
	require.NoError(t, err)
	require.NoError(t, nsc1.PutEntry(ctx, blobcache.RootHandle(), "name1", *volh))

	nsc2 := simplens.Client{Service: svc2}
	entry, err := nsc2.GetEntry(ctx, blobcache.RootHandle(), "name1")
	require.NoError(t, err)
	require.Nil(t, entry)

	err = nsc2.PutEntry(ctx, *volh, "any name", blobcache.Handle{})
	require.Error(t, err)

	names, err := nsc2.ListNames(ctx, *volh)
	require.Error(t, err)
	require.Empty(t, names)
}
