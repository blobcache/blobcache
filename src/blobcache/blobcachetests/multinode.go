package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestMultiNode(t *testing.T, mk func(t testing.TB, n int) []blobcache.Service) {
	t.Run("CreateVolumeRemote", func(t *testing.T) {
		ctx := testutil.Context(t)
		svcs := mk(t, 2)
		s1, s2 := svcs[0], svcs[1]
		// create a volume on the first node
		volh, err := s1.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		s1Ep, err := s1.Endpoint(ctx)
		require.NoError(t, err)
		require.NoError(t, s1.PutEntry(ctx, blobcache.RootHandle(), "name1", *volh))

		// creating a remote volume from the second node should turn into a call to Open on the first node
		volh2, err := s2.CreateVolume(ctx, remoteVolumeSpec(s1Ep, "name1"))
		require.NoError(t, err)

		tx, err := s2.BeginTx(ctx, *volh2, blobcache.TxParams{})
		require.NoError(t, err)
		require.NoError(t, s2.Abort(ctx, *tx))
	})
}

func remoteVolumeSpec(ep blobcache.Endpoint, name string) blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		HashAlgo: blobcache.HashAlgo_BLAKE3_256,
		MaxSize:  1 << 20,
		Backend: blobcache.VolumeBackend[blobcache.Handle]{
			Remote: &blobcache.VolumeBackend_Remote{
				Endpoint: ep,
				Name:     name,
			},
		},
	}
}
