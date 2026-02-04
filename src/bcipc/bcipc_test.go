package bcipc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	_ "blobcache.io/blobcache/src/schema/jsonns"
)

func waitForSocket(ctx context.Context, t testing.TB, sockPath string) {
	t.Helper()
	timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(sockPath); err == nil {
			return
		} else if !os.IsNotExist(err) {
			require.NoError(t, err)
			return
		}
		select {
		case <-timeoutCtx.Done():
			require.NoError(t, timeoutCtx.Err())
			return
		case <-tick.C:
		}
	}
}

func TestService(t *testing.T) {
	t.Parallel()

	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		ctx := testutil.Context(t)
		ctx, cancel := context.WithCancel(ctx)

		dir := t.TempDir()
		sockPath := filepath.Join(dir, "bcipc-test.sock")
		svc := bclocal.NewTestService(t)

		var eg errgroup.Group
		eg.Go(func() error {
			return ListenAndServe(ctx, sockPath, &bcp.Server{
				Access: func(blobcache.PeerID) blobcache.Service {
					return svc
				},
			})
		})

		waitForSocket(ctx, t, sockPath)

		client := NewClient(sockPath)
		t.Cleanup(func() {
			cancel()
			_ = client.Close()
			_ = eg.Wait()
		})
		return client
	})
}
