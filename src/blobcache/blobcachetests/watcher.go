package blobcachetests

import (
	"context"
	"testing"
	"time"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestWatcher(t *testing.T, setup func(testing.TB) (blobcache.Service, blobcache.Handle)) {
	t.Run("ForEach", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := setup(t)

		w := bcsdk.NewWatcher(s, volh)
		defer w.Close()

		// Modify the volume in the background, spacing out changes so the poller picks them up.
		go func() {
			for i := 0; i < 3; i++ {
				Modify(t, s, volh, func(tx *bcsdk.Tx) ([]byte, error) {
					return []byte{byte(i)}, nil
				})
				time.Sleep(1500 * time.Millisecond)
			}
		}()

		var received int
		ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		err := w.ForEach(ctx2, func(tx *bcsdk.Tx) error {
			received++
			if received >= 3 {
				cancel()
			}
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.GreaterOrEqual(t, received, 3)
	})
	t.Run("ForEachClose", func(t *testing.T) {
		s, volh := setup(t)
		ctx := testutil.Context(t)

		w := bcsdk.NewWatcher(s, volh)
		done := make(chan error, 1)
		go func() {
			done <- w.ForEach(ctx, func(tx *bcsdk.Tx) error {
				return nil
			})
		}()
		// Close should cause ForEach to return nil.
		require.NoError(t, w.Close())
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for ForEach to return")
		}
	})
	t.Run("Out", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := setup(t)

		w := bcsdk.NewWatcher(s, volh)
		defer w.Close()

		Modify(t, s, volh, func(tx *bcsdk.Tx) ([]byte, error) {
			return []byte{1, 2, 3}, nil
		})

		select {
		case tx := <-w.Out():
			require.NotNil(t, tx)
			tx.Abort(ctx)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for watcher to emit a Tx")
		}
	})
	t.Run("NoChangeNoEmit", func(t *testing.T) {
		s, volh := setup(t)

		w := bcsdk.NewWatcher(s, volh)
		defer w.Close()

		// Without any modifications, nothing should be emitted after the initial change.
		// Wait a bit longer than the poll interval.
		select {
		case <-w.Out():
			// The first emission is the initial state (nil -> empty root).
			// After that, no more should come.
		case <-time.After(3 * time.Second):
		}

		select {
		case <-w.Out():
			t.Fatal("watcher emitted a Tx without any volume change")
		case <-time.After(2 * time.Second):
			// expected: no emission
		}
	})
}
