package blobcachetests

import (
	"testing"
	"time"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestVolumeSubscribe(t *testing.T, setup func(testing.TB) (svc blobcache.Service, volh, qh blobcache.Handle)) {
	// Each test calls SubToVol to subscribe the queue to the Volume.

	// Empty checks that empty messages show up whenever there is a commit to the Volume.
	t.Run("Empty", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh, qh := setup(t)
		err := s.SubToVolume(ctx, qh, volh, blobcache.VolSubSpec{})
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			Modify(t, s, volh, func(tx *bcsdk.Tx) ([]byte, error) {
				return []byte{byte(i)}, nil
			})

			buf := make([]blobcache.Message, 1)
			maxWait := 1 * time.Second
			n, err := s.Dequeue(ctx, qh, buf, blobcache.DequeueOpts{Min: 1, MaxWait: &maxWait})
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.Empty(t, buf[0].Bytes)
			require.Empty(t, buf[0].Handles)
		}
	})
}
