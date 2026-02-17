package blobcachetests

import (
	"context"
	"testing"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

func QueueAPI(t *testing.T, setup func(testing.TB) (blobcache.QueueAPI, blobcache.Handle)) {
	// Need tests for:
	// - max bytes
	// - max handles
	// - 0 length buffer returns error
	// - simple Enqueue, Dequeue works
	// - Min works 0, 1, 2
	// - MaxWait works
	// - Concurrent Dequeue | wait 1s then Enqueue, Dequeue should return.

	t.Run("MaxBytes", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, qh := setup(t)
		info, err := s.InspectQueue(ctx, qh)
		require.NoError(t, err)
		max := info.Config.MaxBytesPerMessage

		_, err = s.Enqueue(ctx, nil, qh, []blobcache.Message{
			{Bytes: make([]byte, int(max))},
		})
		require.NoError(t, err)
		buf := make([]blobcache.Message, 1)
		n, err := s.Dequeue(ctx, qh, buf, blobcache.DequeueOpts{Min: 1})
		require.NoError(t, err)
		require.Equal(t, 1, n)

		_, err = s.Enqueue(ctx, nil, qh, []blobcache.Message{
			{Bytes: make([]byte, int(max)+1)},
		})
		require.Error(t, err)
	})
	t.Run("MaxHandles", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, qh := setup(t)
		info, err := s.InspectQueue(ctx, qh)
		require.NoError(t, err)
		max := info.Config.MaxHandlesPerMessage

		handles := make([]blobcache.Handle, int(max))
		for i := range handles {
			handles[i] = blobcache.Handle{OID: blobcache.RandomOID()}
		}
		_, err = s.Enqueue(ctx, nil, qh, []blobcache.Message{
			{Handles: handles},
		})
		require.NoError(t, err)
		buf := make([]blobcache.Message, 1)
		n, err := s.Dequeue(ctx, qh, buf, blobcache.DequeueOpts{Min: 1})
		require.NoError(t, err)
		require.Equal(t, 1, n)

		handlesOver := make([]blobcache.Handle, int(max)+1)
		for i := range handlesOver {
			handlesOver[i] = blobcache.Handle{OID: blobcache.RandomOID()}
		}
		_, err = s.Enqueue(ctx, nil, qh, []blobcache.Message{
			{Handles: handlesOver},
		})
		require.Error(t, err)
	})
	t.Run("ZeroLengthBuffer", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, qh := setup(t)
		_, err := s.Dequeue(ctx, qh, []blobcache.Message{}, blobcache.DequeueOpts{})
		require.Error(t, err)
	})
	t.Run("EnqueueDequeue", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, qh := setup(t)
		msg := blobcache.Message{Bytes: []byte("hello")}
		resp, err := s.Enqueue(ctx, nil, qh, []blobcache.Message{msg})
		require.NoError(t, err)
		require.Equal(t, uint32(1), resp.Success)

		buf := make([]blobcache.Message, 1)
		n, err := s.Dequeue(ctx, qh, buf, blobcache.DequeueOpts{Min: 1})
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, msg.Bytes, buf[0].Bytes)
	})
	t.Run("Min", func(t *testing.T) {
		t.Run("0", func(t *testing.T) {
			t.Parallel()
			ctx := testutil.Context(t)
			s, qh := setup(t)
			ctx2, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			buf := make([]blobcache.Message, 1)
			n, err := s.Dequeue(ctx2, qh, buf, blobcache.DequeueOpts{Min: 0})
			require.NoError(t, err)
			require.Equal(t, 0, n)
		})
		t.Run("1", func(t *testing.T) {
			t.Parallel()
			ctx := testutil.Context(t)
			s, qh := setup(t)
			ctx2, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			go func() {
				time.Sleep(50 * time.Millisecond)
				_, _ = s.Enqueue(ctx2, nil, qh, []blobcache.Message{{Bytes: []byte("a")}})
			}()
			buf := make([]blobcache.Message, 1)
			maxWait := 500 * time.Millisecond
			n, err := s.Dequeue(ctx2, qh, buf, blobcache.DequeueOpts{Min: 1, MaxWait: &maxWait})
			require.NoError(t, err)
			require.Equal(t, 1, n)
		})
		t.Run("2", func(t *testing.T) {
			t.Parallel()
			ctx := testutil.Context(t)
			s, qh := setup(t)
			_, err := s.Enqueue(ctx, nil, qh, []blobcache.Message{
				{Bytes: []byte("a")},
				{Bytes: []byte("b")},
			})
			require.NoError(t, err)

			buf := make([]blobcache.Message, 2)
			n, err := s.Dequeue(ctx, qh, buf, blobcache.DequeueOpts{Min: 2})
			require.NoError(t, err)
			require.Equal(t, 2, n)
		})
	})
	t.Run("MaxWait", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, qh := setup(t)
		ctx2, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		buf := make([]blobcache.Message, 1)
		maxWait := 50 * time.Millisecond
		n, err := s.Dequeue(ctx2, qh, buf, blobcache.DequeueOpts{Min: 1, MaxWait: &maxWait})
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
	t.Run("ConcurrentDequeue", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, qh := setup(t)
		ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		type result struct {
			n   int
			err error
		}
		resCh := make(chan result, 1)
		maxWait := 2 * time.Second
		go func() {
			buf := make([]blobcache.Message, 1)
			n, err := s.Dequeue(ctx2, qh, buf, blobcache.DequeueOpts{Min: 1, MaxWait: &maxWait})
			resCh <- result{n: n, err: err}
		}()

		time.Sleep(1 * time.Second)
		_, err := s.Enqueue(ctx2, nil, qh, []blobcache.Message{{Bytes: []byte("x")}})
		require.NoError(t, err)

		select {
		case res := <-resCh:
			require.NoError(t, res.err)
			require.Equal(t, 1, res.n)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for dequeue")
		}
	})
}
