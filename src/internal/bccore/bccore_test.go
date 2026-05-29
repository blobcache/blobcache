package bccore

import (
	"testing"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend/memory"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestRootHandles(t *testing.T) {
	s := New(Params{Root: memory.NewVolume(1000)})

	var hs []blobcache.Handle
	now := time.Now()
	for i := range hs {
		hs[i] = s.Mint(blobcache.OID{}, blobcache.Action_ALL, now, time.Minute)
	}
	ctx := testutil.Context(t)
	for i := range hs {
		volInfo, err := s.InspectVolume(ctx, hs[i])
		require.NoError(t, err)
		require.Equal(t, volInfo.ID, blobcache.OID{})
	}
}
