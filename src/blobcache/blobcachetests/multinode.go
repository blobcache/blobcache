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
		volh, err := s1.CreateVolume(ctx, defaultLocalSpec())
		require.NoError(t, err)
		s1Ep, err := s1.Endpoint(ctx)
		require.NoError(t, err)
		require.NoError(t, s1.PutEntry(ctx, blobcache.RootHandle(), "name1", *volh))

		// creating a remote volume from the second node should turn into a call to Open on the first node
		volh2, err := s2.CreateVolume(ctx, remoteVolumeSpec(s1Ep, volh.OID))
		require.NoError(t, err)

		tx, err := s2.BeginTx(ctx, *volh2, blobcache.TxParams{})
		require.NoError(t, err)
		require.NoError(t, s2.Abort(ctx, *tx))
	})
	t.Run("CreateVolumeAt", func(t *testing.T) {
		ctx := testutil.Context(t)
		svcs := mk(t, 2)
		s1, s2 := svcs[0], svcs[1]
		s1Ep, err := s1.Endpoint(ctx)
		require.NoError(t, err)

		nsh, err := s2.CreateVolume(ctx, remoteVolumeSpec(s1Ep, blobcache.OID{}))
		require.NoError(t, err)
		volh, err := s2.CreateVolumeAt(ctx, *nsh, "vol1", defaultLocalSpec())
		require.NoError(t, err)
		Modify(t, s2, *volh, func(tx *blobcache.Tx) ([]byte, error) {
			return []byte("hello"), nil
		})
	})
	t.Run("Remote/Tx", func(t *testing.T) {
		ctx := testutil.Context(t)
		TxAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			svcs := mk(t, 2)
			s1, s2 := svcs[0], svcs[1]
			vol1 := CreateVolume(t, s1, defaultLocalSpec())
			ep, err := s1.Endpoint(ctx)
			require.NoError(t, err)
			t.Log("creating remote volume", ep, vol1.OID)
			vol2 := CreateVolume(t, s2, remoteVolumeSpec(ep, vol1.OID))
			t.Log("setup remote volume, handing over to TxAPI test")
			return s2, vol2
		})
	})
}

func remoteVolumeSpec(ep blobcache.Endpoint, volid blobcache.OID) blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		HashAlgo: blobcache.HashAlgo_BLAKE3_256,
		MaxSize:  1 << 20,
		Salted:   false,
		Backend: blobcache.VolumeBackend[blobcache.Handle]{
			Remote: &blobcache.VolumeBackend_Remote{
				Endpoint: ep,
				Volume:   volid,
			},
		},
	}
}
