package bcremote

import (
	"testing"

	"github.com/cloudflare/circl/sign/ed25519"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"

	_ "blobcache.io/blobcache/src/schema/jsonns"
)

func TestService(t *testing.T) {
	t.Parallel()
	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		server := bclocal.NewTestService(t)
		_, privateKey, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		client, err := Dial(privateKey, blobcachetests.Endpoint(t, server))
		require.NoError(t, err)
		ctx := testutil.Context(t)
		client.AwaitReady(ctx)
		return client
	})
}
