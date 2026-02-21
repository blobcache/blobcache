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
		s1ep := Endpoint(t, s1)

		// create a volume on the first node
		volh, err := s1.CreateVolume(ctx, nil, defaultLocalSpec())
		require.NoError(t, err)

		// creating a remote volume on the second node should turn into a call to OpenFiat on the first node
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
	t.Run("Remote/Queue", func(t *testing.T) {
		QueueAPI(t, func(t testing.TB) (blobcache.QueueAPI, blobcache.Handle) {
			ctx := testutil.Context(t)
			svcs := mk(t, 2)
			s1, s2 := svcs[0], svcs[1]
			s1ep := Endpoint(t, s1)
			qh, err := s2.CreateQueue(ctx, &s1ep, blobcache.QueueSpec{
				Memory: &blobcache.QueueBackend_Memory{
					MaxDepth:             16,
					MaxBytesPerMessage:   1024,
					MaxHandlesPerMessage: 16,
				},
			})
			require.NoError(t, err)
			require.NotNil(t, qh)
			qh, err = s2.CreateQueue(ctx, nil, remoteQueueSpec(s1ep, qh.OID))
			require.NoError(t, err)
			return s2, *qh
		})
	})
	t.Run("Remote/SubToVol", func(t *testing.T) {
		TestVolumeSubscribe(t, func(t testing.TB) (blobcache.Service, blobcache.Handle, blobcache.Handle) {
			ctx := testutil.Context(t)
			svcs := mk(t, 2)
			s1, s2 := svcs[0], svcs[1]
			s1ep := Endpoint(t, s1)

			volh, err := s2.CreateVolume(ctx, &s1ep, blobcache.DefaultLocalSpec())
			require.NoError(t, err)

			qh, err := s2.CreateQueue(ctx, &s1ep, blobcache.QueueSpec{
				Memory: &blobcache.QueueBackend_Memory{
					MaxDepth:             1,
					MaxBytesPerMessage:   1024,
					MaxHandlesPerMessage: 16,
				},
			})
			require.NoError(t, err)

			return s2, *volh, *qh
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

func remoteQueueSpec(ep blobcache.Endpoint, volid blobcache.OID) blobcache.QueueSpec {
	return blobcache.QueueSpec{
		Remote: &blobcache.QueueBackend_Remote{
			Endpoint: ep,
			OID:      volid,
		},
	}
}
