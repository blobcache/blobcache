package blobcachecmd_test

import (
	"path/filepath"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/blobcachecmd"
	"blobcache.io/blobcache/src/internal/blobcached"
	"blobcache.io/blobcache/src/internal/testutil"
)

func TestAPI(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	execPath := filepath.Join(dir, "blobcache")
	t.Log("building blobcache command")
	testutil.BuildGoExec(t, execPath, "../../cmd/blobcache")
	t.Log("built blobcache command. written to ", execPath)

	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		if strings.Contains(t.Name(), "Queue") {
			// If we ever add `blobcache queue` to the API then we can start running these tests.
			t.SkipNow()
		}
		_, apiAddr := blobcached.BGTestDaemon(t)
		return &blobcachecmd.Service{
			APIAddr:  apiAddr,
			ExecPath: execPath,
		}
	})
}
