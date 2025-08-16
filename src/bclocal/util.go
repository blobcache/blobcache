package bclocal

import (
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/simplecont"
	"blobcache.io/blobcache/src/schema/simplens"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/stretchr/testify/require"
)

// NewTestService creates a service scoped to the life of the test.
func NewTestService(t testing.TB) *Service {
	ctx := testutil.Context(t)
	stateDir := t.TempDir()
	dbPath := filepath.Join(stateDir, "blobcache.db")
	db, err := dbutil.OpenDB(dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	if err := SetupDB(ctx, db); err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	svc := New(Env{
		PrivateKey: privateKey,
		PacketConn: testutil.PacketConn(t),
		DB:         db,
		Schemas:    DefaultSchemas(),
		Root:       DefaultRoot(),
	})
	go svc.Run(ctx)
	return svc
}

func DefaultSchemas() map[blobcache.Schema]schema.Schema {
	return map[blobcache.Schema]schema.Schema{
		blobcache.Schema_NONE:            schema.None{},
		blobcache.Schema_SimpleNS:        simplens.Schema{},
		blobcache.Schema_SimpleContainer: simplecont.Schema{},
	}
}

// DefaultRoot returns the default root volume spec.
// It uses the SimpleNS schema and a 2MB byte max size.
func DefaultRoot() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			VolumeParams: blobcache.VolumeParams{
				Schema:   blobcache.Schema_SimpleNS,
				HashAlgo: blobcache.HashAlgo_BLAKE3_256,
				MaxSize:  1 << 22,
			},
		},
	}
}
