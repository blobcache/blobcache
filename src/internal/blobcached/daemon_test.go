package blobcached

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"blobcache.io/blobcache/src/bcipc"
	"blobcache.io/blobcache/src/internal/testutil"
)

func TestRun(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithTimeout(ctx, 5*time.Second)
	defer cf()
	dir, err := os.OpenRoot(t.TempDir())
	require.NoError(t, err)

	done := make(chan struct{})
	sockPath := filepath.Join(dir.Name(), "test.sock")
	lis, err := bcipc.Listen(sockPath)
	require.NoError(t, err)
	d := Daemon{StateDir: dir}
	require.NoError(t, d.EnsurePolicyFiles())
	go func() {
		err := d.Run(ctx, testutil.PacketConn(t), nil, []*net.UnixListener{lis})
		require.NoError(t, err)
		close(done)
	}()
	c := bcipc.NewClient(sockPath)
	ep, err := c.Endpoint(ctx)
	require.NoError(t, err)
	t.Log("endpoint", ep)
	require.NoError(t, c.Close())
	lis.Close()

	cf()
	<-done
	t.Log("done")
}
