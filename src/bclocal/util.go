package bclocal

import (
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

// NewTestService creates a service scoped to the life of the test.
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
