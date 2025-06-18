package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestMultiNode(t *testing.T, mk func(t testing.TB, n int) []blobcache.Service) {
	t.Run("CreateVolume", func(t *testing.T) {
		ctx := testutil.Context(t)
		svcs := mk(t, 2)
		s1, s2 := svcs[0], svcs[1]
		_, err := s1.CreateVolume(ctx, defaultVolumeSpec())
		require.NoError(t, err)
		s1Ep, err := s1.Endpoint(ctx)
		require.NoError(t, err)

		volh, err := s2.CreateVolume(ctx, remoteVolumeSpec(s1Ep))
		require.NoError(t, err)

		buf := make([]byte, 3)
		err = s2.Load(ctx, *volh, &buf)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2, 3}, buf)
	})
}

func remoteVolumeSpec(ep blobcache.Endpoint) blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		HashAlgo: blobcache.HashAlgo_BLAKE3_256,
		MaxSize:  1 << 20,
		Backend: blobcache.VolumeBackend[blobcache.Handle]{
			Remote: &blobcache.VolumeBackend_Remote{
				Endpoint: ep,
			},
		},
	}
}
