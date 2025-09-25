package blobcached

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/internal/testutil"
)

func TestRun(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithTimeout(ctx, 5*time.Second)
	dir := t.TempDir()

	done := make(chan struct{})
	lis := testutil.Listen(t)
	d := Daemon{StateDir: dir}
	require.NoError(t, d.EnsurePolicyFiles())
	go func() {
		err := d.Run(ctx, testutil.PacketConn(t), lis)
		require.NoError(t, err)
		close(done)
	}()
	hc := bchttp.NewClient(nil, "http://"+lis.Addr().String())

	t.Log("awaiting healthy")
	require.NoError(t, AwaitHealthy(ctx, hc))
	t.Log("shutting down")

	cf()
	<-done
	t.Log("done")
}
