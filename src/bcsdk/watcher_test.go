package bcsdk_test

import (
	"context"
	"testing"
	"time"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func setup(t testing.TB) (blobcache.Service, blobcache.Handle) {
	ctx := testutil.Context(t)
	svc := bclocal.NewTestService(t)
	volh, err := svc.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	require.NoError(t, err)
	return svc, *volh
}

func modify(t testing.TB, s blobcache.Service, volh blobcache.Handle, data []byte) {
	ctx := testutil.Context(t)
	tx, err := bcsdk.BeginTx(ctx, s, volh, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	require.NoError(t, tx.Save(ctx, data))
	require.NoError(t, tx.Commit(ctx))
}

func TestWatcher_ForEach(t *testing.T) {
	ctx := testutil.Context(t)
	s, volh := setup(t)

	w, err := bcsdk.NewWatcher(s, volh)
	require.NoError(t, err)
	defer w.Close()

	// Modify the volume in the background.
	go func() {
		for i := 0; i < 3; i++ {
			modify(t, s, volh, []byte{byte(i)})
			time.Sleep(500 * time.Millisecond)
		}
	}()

	var received int
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = w.ForEach(ctx2, func(tx *bcsdk.Tx) error {
		received++
		if received >= 3 {
			cancel()
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.GreaterOrEqual(t, received, 3)
}

func TestWatcher_ForEachClose(t *testing.T) {
	s, volh := setup(t)
	ctx := testutil.Context(t)

	w, err := bcsdk.NewWatcher(s, volh)
	require.NoError(t, err)
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
}

func TestWatcher_Out(t *testing.T) {
	ctx := testutil.Context(t)
	s, volh := setup(t)

	w, err := bcsdk.NewWatcher(s, volh)
	require.NoError(t, err)
	defer w.Close()

	modify(t, s, volh, []byte{1, 2, 3})

	select {
	case tx := <-w.Out():
		require.NotNil(t, tx)
		tx.Abort(ctx)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watcher to emit a Tx")
	}
}

func TestWatcher_NoChangeNoEmit(t *testing.T) {
	s, volh := setup(t)

	w, err := bcsdk.NewWatcher(s, volh)
	require.NoError(t, err)
	defer w.Close()

	// Without any modifications, nothing should be emitted.
	select {
	case <-w.Out():
		t.Fatal("watcher emitted a Tx without any volume change")
	case <-time.After(2 * time.Second):
		// expected: no emission
	}
}
