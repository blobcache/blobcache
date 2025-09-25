package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/basicns"
	"github.com/stretchr/testify/require"
)

func TestMultiNode(t *testing.T, mk func(t testing.TB, n int) []blobcache.Service) {
	t.Run("CreateVolumeRemote", func(t *testing.T) {
		ctx := testutil.Context(t)
		svcs := mk(t, 2)
		s1, s2 := svcs[0], svcs[1]
		s1ep := Endpoint(t, s1)

		// create a volume on the first node
		volh, err := s1.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)
		nsh, err := s1.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)
		nsc := basicns.Client{Service: s1}
		require.NoError(t, nsc.PutEntry(ctx, *nsh, "name1", *volh))

		// creating a remote volume on the second node should turn into a call to OpenAs on the first node
		volh2, err := s2.CreateVolume(ctx, nil, remoteVolumeSpec(s1ep, volh.OID))
		require.NoError(t, err)

		tx, err := s2.BeginTx(ctx, *volh2, blobcache.TxParams{})
		require.NoError(t, err)
		require.NoError(t, s2.Abort(ctx, *tx))
	})
	t.Run("Remote/Tx", func(t *testing.T) {
		ctx := testutil.Context(t)
		TxAPI(t, func(t testing.TB) (blobcache.Service, blobcache.Handle) {
			svcs := mk(t, 2)
			s1, s2 := svcs[0], svcs[1]
			vol1 := CreateVolume(t, s1, nil, defaultLocalSpec())
			ep, err := s1.Endpoint(ctx)
			require.NoError(t, err)
			t.Log("creating remote volume", ep, vol1.OID)
			vol2 := CreateVolume(t, s2, nil, remoteVolumeSpec(ep, vol1.OID))
			t.Log("setup remote volume, handing over to TxAPI test")
			return s2, vol2
		})
	})
}

func remoteVolumeSpec(ep blobcache.Endpoint, volid blobcache.OID) blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Remote: &blobcache.VolumeBackend_Remote{
			Endpoint: ep,
			Volume:   volid,
		},
	}
}
