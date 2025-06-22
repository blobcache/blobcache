package bclocal

import (
	"crypto/ed25519"
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

// NewTestService creates a service scoped to the life of the test.
func NewTestService(t testing.TB) *Service {
	ctx := testutil.Context(t)
	stateDir := t.TempDir()
	dbPath := filepath.Join(stateDir, "blobcache.db")
	db, err := dbutil.OpenDB(dbPath)
	require.NoError(t, err)
	if err := SetupDB(ctx, db); err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	svc := New(Env{
		PrivateKey: privateKey,
		PacketConn: testutil.PacketConn(t),
		DB:         db,
	})
	go svc.Run(ctx)
	return svc
}
