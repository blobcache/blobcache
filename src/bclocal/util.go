package bclocal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/simplecont"
	"blobcache.io/blobcache/src/schema/simplens"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// NewTestService creates a service scoped to the life of the test.
func NewTestService(t testing.TB) *Service {
	ctx := testutil.Context(t)
	stateDir := t.TempDir()

	pebbleDB, err := pebble.Open(filepath.Join(stateDir, "pebble"), &pebble.Options{})
	require.NoError(t, err)
	blobPath := filepath.Join(stateDir, "blob")
	require.NoError(t, os.MkdirAll(blobPath, 0o755))
	blobDir, err := os.OpenRoot(blobPath)
	require.NoError(t, err)

	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	svc := New(Env{
		PrivateKey: privateKey,
		PacketConn: testutil.PacketConn(t),
		DB:         pebbleDB,
		BlobDir:    blobDir,
		Schemas:    DefaultSchemas(),
		Root:       DefaultRoot(),
	})
	var eg errgroup.Group
	ctx, cf := context.WithCancel(ctx)
	eg.Go(func() error {
		return svc.Run(ctx)
	})
	t.Cleanup(func() {
		cf()
		require.NoError(t, eg.Wait())
		require.NoError(t, svc.AbortAll(ctx))
		require.NoError(t, blobDir.Close())
		require.NoError(t, pebbleDB.Close())
	})
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
