package bclocal

import (
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/dbutil"
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

func TestAPI(t *testing.T) {
	blobcachetests.ServiceAPI(t, NewTestService)
}

func NewTestService(t testing.TB) blobcache.Service {
	ctx := testutil.Context(t)
	stateDir := t.TempDir()
	dbPath := filepath.Join(stateDir, "blobcache.db")
	db, err := dbutil.OpenDB(dbPath)
	require.NoError(t, err)
	if err := SetupDB(ctx, db); err != nil {
		t.Fatal(err)
	}
	return New(Env{DB: db})
}
