package bcfuse

import (
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewFS(t *testing.T) {
	fs := newFS(t)
	t.Log(fs)
}

func newFS(t testing.TB) *FS[string] {
	ctx := testutil.Context(t)
	db := dbutil.OpenMemory()
	require.NoError(t, SetupDB(ctx, db))
	svc := bclocal.NewTestService(t)
	volh, err := svc.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	require.NoError(t, err)
	return New[string](db, svc, *volh, nil)
}
