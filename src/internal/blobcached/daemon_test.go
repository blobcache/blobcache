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
	d := Daemon{StateDir: dir, ConfigDir: dir}
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

func TestListenUnix(t *testing.T) {
	sockDir := t.TempDir()

	t.Run("creates socket in test temp dir", func(t *testing.T) {
		for _, name := range []string{"one.sock", "two.sock"} {
			sockPath := filepath.Join(sockDir, name)
			lis, err := ListenUnix(sockPath)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, lis.Close())
			})
			_, err = os.Stat(sockPath)
			require.NoError(t, err)
		}
	})

	t.Run("replaces stale socket", func(t *testing.T) {
		sockPath := filepath.Join(sockDir, "stale.sock")

		laddr := net.UnixAddr{Name: sockPath, Net: "unix"}
		staleLis, err := net.ListenUnix("unix", &laddr)
		require.NoError(t, err)
		staleLis.SetUnlinkOnClose(false)
		require.NoError(t, staleLis.Close())

		_, err = os.Stat(sockPath)
		require.NoError(t, err)

		lis, err := ListenUnix(sockPath)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lis.Close())
		})
	})
}
