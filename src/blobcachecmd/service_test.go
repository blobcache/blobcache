package blobcachecmd

import (
	"os/exec"
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/blobcached"
	"github.com/stretchr/testify/require"
)

func TestAPI(t *testing.T) {
	t.Parallel()
	t.SkipNow()

	dir := t.TempDir()
	execPath := filepath.Join(dir, "blobcache")
	require.NoError(t, buildCmd(execPath))

	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		_, apiAddr := blobcached.RunTestDaemon(t)
		return &Service{
			APIAddr:  apiAddr,
			ExecPath: execPath,
		}
	})
}

func buildCmd(dst string) error {
	cmd := exec.Command("go", "build", "-o", dst, "../../cmd/blobcache")
	return cmd.Run()
}
