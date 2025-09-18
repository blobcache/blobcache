package bclocal

import (
	"context"
	"iter"
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

// NewTestEnv creates a test environment.
// It uses:
// - t.TempDir() to create a temporary directory
// - creates a UDP socket on a random port.
// All the resources are cleaned up when the test is done.
func NewTestEnv(t testing.TB) Env {
	stateDir := t.TempDir()
	pebbleDirPath := filepath.Join(stateDir, "pebble")
	blobDirPath := filepath.Join(stateDir, "blob")
	for _, dir := range []string{pebbleDirPath, blobDirPath} {
		require.NoError(t, os.MkdirAll(dir, 0o755))
	}

	pebbleDB, err := pebble.Open(pebbleDirPath, &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pebbleDB.Close())
	})
	blobDir, err := os.OpenRoot(blobDirPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, blobDir.Close())
	})

	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return Env{
		PrivateKey: privateKey,
		PacketConn: testutil.PacketConn(t),
		DB:         pebbleDB,
		BlobDir:    blobDir,
		Policy:     &allowAllPolicy{},
		Schemas:    DefaultSchemas(),
		Root:       DefaultRoot(),
	}
}

// NewTestServiceFromEnv creates a service from an environment.
// It runs the service in the background and forcibly aborts all transactions when the test is done.
func NewTestServiceFromEnv(t testing.TB, env Env) *Service {
	svc := New(env)

	ctx := testutil.Context(t)
	var eg errgroup.Group
	ctx, cf := context.WithCancel(ctx)
	eg.Go(func() error {
		return svc.Run(ctx)
	})
	t.Cleanup(func() {
		cf()
		require.NoError(t, eg.Wait())
		require.NoError(t, svc.AbortAll(ctx))
	})
	return svc
}

// NewTestService creates a service scoped to the life of the test.
// It calls NewTestEnv and NewTestServiceFromEnv
func NewTestService(t testing.TB) *Service {
	return NewTestServiceFromEnv(t, NewTestEnv(t))
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

// DefaultPebbleOptions
func DefaultPebbleOptions() *pebble.Options {
	return &pebble.Options{
		Logger: noOpLogger{},
	}
}

type noOpLogger struct{}

func (l noOpLogger) Infof(msg string, args ...interface{}) {}

func (l noOpLogger) Fatalf(msg string, args ...interface{}) {}

// allowAllPolicy is a policy that allows all actions for all peers.
// this is useful for testing, but is otherwise a bad idea.
// Do not make this public.
type allowAllPolicy struct{}

func (p *allowAllPolicy) GrantsFor(peer blobcache.PeerID) iter.Seq[schema.Link] {
	return func(yield func(schema.Link) bool) {
		yield(schema.Link{Target: blobcache.OID{}, Rights: blobcache.Action_ALL})
	}
}

func (p *allowAllPolicy) Open(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	return blobcache.Action_ALL
}

func (p *allowAllPolicy) CanCreate(peer blobcache.PeerID) bool {
	return true
}
