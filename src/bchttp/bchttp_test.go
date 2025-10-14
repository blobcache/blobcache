package bchttp

import (
	"fmt"
	"net/http"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/testutil"

	_ "blobcache.io/blobcache/src/schema/basicns"
)

func TestService(t *testing.T) {
	t.Parallel()
	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		srv := Server{
			Service: bclocal.NewTestService(t),
		}
		lis := testutil.Listen(t)
		go func() {
			if err := http.Serve(lis, &srv); err != nil {
				t.Log(err)
			}
		}()
		return NewClient(nil, fmt.Sprintf("http://%s", lis.Addr().String()))
	})
}
