package bcremote

import (
	"testing"

	"github.com/cloudflare/circl/sign/ed25519"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		server := bclocal.NewTestService(t)
		_, privateKey, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		client, err := Dial(privateKey, blobcachetests.Endpoint(t, server))
		require.NoError(t, err)
		return client
	})
}
