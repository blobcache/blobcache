package blobcachetests

import (
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

// TxAPI runs a test suite for transactions on a given volume.
// The function mk should return a service and a volume handle.
// The volume handle will be used to open multiple transactions, sometimes concurrently.
func TxAPI(t *testing.T, mk func(t testing.TB) (blobcache.Service, blobcache.Handle)) {
	t.Run("TxAbortNoOp", func(t *testing.T) {
		t.Parallel()
		ctx := testutil.Context(t)
		s, volh := mk(t)
		for _, p := range []blobcache.TxParams{
			{Mutate: false},
			{Mutate: true},
		} {
			txh, err := s.BeginTx(ctx, volh, p)
			require.NoError(t, err)
			require.NotNil(t, txh)
			err = s.Abort(ctx, *txh)
			require.NoError(t, err)
		}
	})
	t.Run("TxCommit", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Mutate: true})
		require.NoError(t, err)
		require.NotNil(t, txh)
		err = s.Save(ctx, *txh, []byte{1, 2, 3})
		require.NoError(t, err)
		err = s.Commit(ctx, *txh)
		require.NoError(t, err)
	})
	t.Run("TxCommitReadOnly", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Mutate: false})
		require.NoError(t, err)
		require.NotNil(t, txh)
		err = s.Save(ctx, *txh, []byte{1, 2, 3})
		require.Error(t, err)
		err = s.Commit(ctx, *txh)
		require.Error(t, err)
	})
	t.Run("TxReadOnly", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Mutate: false})
		require.NoError(t, err)
		require.NotNil(t, txh)

		cid, err := s.Post(ctx, *txh, nil, []byte{1, 2, 3})
		require.Error(t, err)
		err = s.Delete(ctx, *txh, []blobcache.CID{cid})
		require.Error(t, err)
	})
	t.Run("PostExists", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Mutate: true})
		require.NoError(t, err)
		require.NotNil(t, txh)

		data := []byte{1, 2, 3}
		require.False(t, Exists(t, s, *txh, blake3.Sum256(data)), "should not exist before post")
		cid := Post(t, s, *txh, nil, data)
		require.True(t, Exists(t, s, *txh, cid), "should exist after post")
	})
	t.Run("PostGet", func(t *testing.T) {
		s, volh := mk(t)
		txh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})

		data1 := []byte("hello world")
		cid := Post(t, s, txh, nil, data1)
		data2 := Get(t, s, txh, cid, nil, 100)
		require.Equal(t, data1, data2)
	})
	t.Run("Exists", func(t *testing.T) {
		s, volh := mk(t)
		hf := defaultLocalSpec().Local.HashAlgo.HashFunc()

		txh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})
		data1 := []byte("hello world")
		require.False(t, Exists(t, s, txh, hf(nil, data1)))
		Post(t, s, txh, nil, data1)
		require.True(t, Exists(t, s, txh, hf(nil, data1)))
		Delete(t, s, txh, hf(nil, data1))
		require.False(t, Exists(t, s, txh, hf(nil, data1)))
	})
	t.Run("1WriterNReaders", func(t *testing.T) {
		s, volh := mk(t)

		wtxh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})
		buf := Load(t, s, wtxh)
		require.Equal(t, []byte{}, buf)

		// Open 10 readers.
		rtxhs := make([]blobcache.Handle, 10)
		for i := range rtxhs {
			rtxhs[i] = BeginTx(t, s, volh, blobcache.TxParams{Mutate: false})
			defer Abort(t, s, rtxhs[i])
		}
		// commit the write transaction.
		root2 := []byte{1, 2, 3}
		SaveCommit(t, s, wtxh, root2)

		// all of the readers should still see the empty blob.
		for _, rtxh := range rtxhs {
			buf := Load(t, s, rtxh)
			require.Equal(t, buf, []byte{})
		}

		// this reader should see the new root.
		rtxh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})
		defer Abort(t, s, rtxh)
		buf2 := Load(t, s, rtxh)
		require.Equal(t, root2, buf2)
	})
	t.Run("WriteN", func(t *testing.T) {
		s, volh := mk(t)
		const N = 10
		for i := 0; i < N; i++ {
			func() {
				wtxh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true})
				SaveCommit(t, s, wtxh, []byte{byte(i)})
			}()
		}
		txh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: false})
		defer Abort(t, s, txh)
		buf := Load(t, s, txh)
		require.Equal(t, []byte{9}, buf)
	})
	t.Run("Visited", func(t *testing.T) {
		// check to see if IsVisited and Visit work.
		s, volh := mk(t)
		txh := BeginTx(t, s, volh, blobcache.TxParams{Mutate: true, GC: true})
		defer Abort(t, s, txh)

		hf := defaultLocalSpec().Local.HashAlgo.HashFunc()
		data1 := []byte("hello world")
		cid1 := hf(nil, data1)
		// should not be visited yet.
		require.Equal(t, []bool{false}, IsVisited(t, s, txh, []blobcache.CID{cid1}))
		Post(t, s, txh, nil, data1)
		require.Equal(t, []bool{false}, IsVisited(t, s, txh, []blobcache.CID{cid1}))
		Visit(t, s, txh, []blobcache.CID{cid1})
		require.Equal(t, []bool{true}, IsVisited(t, s, txh, []blobcache.CID{cid1}))
	})
}
