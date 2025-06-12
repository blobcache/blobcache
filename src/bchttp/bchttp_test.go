package bchttp

import (
	"fmt"
	"net/http"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/testutil"
)

func TestService(t *testing.T) {
	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		ctx := testutil.Context(t)
		db := dbutil.OpenMemory()
		if err := bclocal.SetupDB(ctx, db); err != nil {
			t.Fatal(err)
		}
		srv := Server{
			Service: bclocal.New(bclocal.Env{DB: db}),
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
