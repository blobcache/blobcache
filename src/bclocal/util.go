package bclocal

import (
	"context"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/basiccont"
	"blobcache.io/blobcache/src/schema/basicns"
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

	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return Env{
		Background: testutil.Context(t),
		StateDir:   stateDir,

		PrivateKey: privateKey,

		Policy:  &allowAllPolicy{},
		Schemas: DefaultSchemas(),
		Root:    DefaultRoot(),
	}
}

// NewTestServiceFromEnv creates a service from an environment.
// It runs the service in the background and forcibly aborts all transactions when the test is done.
func NewTestServiceFromEnv(t testing.TB, env Env) *Service {
	svc, err := New(env, Config{NoSync: true})
	require.NoError(t, err)

	ctx := testutil.Context(t)
	var eg errgroup.Group
	ctx, cf := context.WithCancel(ctx)
	eg.Go(func() error {
		pc := testutil.PacketConn(t)
		return svc.Serve(ctx, pc)
	})
	t.Cleanup(func() {
		cf()
		require.NoError(t, eg.Wait())
		require.NoError(t, svc.Close())
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
		blobcache.Schema_NONE:           schema.None{},
		blobcache.Schema_BasicNS:        basicns.Schema{},
		blobcache.Schema_BasicContainer: basiccont.Schema{},
	}
}

// DefaultRoot returns the default root volume spec.
// It uses the basicns schema and a 2MB byte max size.
func DefaultRoot() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			VolumeParams: blobcache.VolumeParams{
				Schema:   blobcache.Schema_BasicNS,
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

func (l noOpLogger) Infof(msg string, args ...any) {}

func (l noOpLogger) Fatalf(msg string, args ...any) {}

// allowAllPolicy is a policy that allows all actions for all peers.
// this is useful for testing, but is otherwise a bad idea.
// Do not make this public.
type allowAllPolicy struct{}

func (p *allowAllPolicy) CanConnect(peer blobcache.PeerID) bool {
	return true
}

func (p *allowAllPolicy) Open(peer blobcache.PeerID, target blobcache.OID) blobcache.ActionSet {
	return blobcache.Action_ALL
}

func (p *allowAllPolicy) CanCreate(peer blobcache.PeerID) bool {
	return true
}
