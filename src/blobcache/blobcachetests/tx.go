package blobcachetests

import (
	"fmt"
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
			{Modify: false},
			{Modify: true},
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
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Modify: true})
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
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Modify: false})
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
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Modify: false})
		require.NoError(t, err)
		require.NotNil(t, txh)

		cid, err := s.Post(ctx, *txh, []byte{1, 2, 3}, blobcache.PostOpts{})
		require.Error(t, err)
		err = s.Delete(ctx, *txh, []blobcache.CID{cid})
		require.Error(t, err)
	})
	t.Run("PostExists", func(t *testing.T) {
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh, err := s.BeginTx(ctx, volh, blobcache.TxParams{Modify: true})
		require.NoError(t, err)
		require.NotNil(t, txh)

		data := []byte{1, 2, 3}
		require.False(t, Exists(t, s, *txh, blake3.Sum256(data)), "should not exist before post")
		cid := Post(t, s, *txh, data, blobcache.PostOpts{})
		require.True(t, Exists(t, s, *txh, cid), "should exist after post")
	})
	t.Run("PostGet", func(t *testing.T) {
		s, volh := mk(t)
		txh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true})

		data1 := []byte("hello world")
		cid := Post(t, s, txh, data1, blobcache.PostOpts{})
		data2 := GetBytes(t, s, txh, cid, blobcache.GetOpts{}, 100)
		require.Equal(t, data1, data2)
	})
	t.Run("Exists", func(t *testing.T) {
		s, volh := mk(t)
		hf := defaultLocalSpec().Local.HashAlgo.HashFunc()

		txh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true})
		data1 := []byte("hello world")
		require.False(t, Exists(t, s, txh, hf(nil, data1)))
		Post(t, s, txh, data1, blobcache.PostOpts{})
		require.True(t, Exists(t, s, txh, hf(nil, data1)))
		Delete(t, s, txh, hf(nil, data1))
		require.False(t, Exists(t, s, txh, hf(nil, data1)))
	})
	t.Run("1WriterNReaders", func(t *testing.T) {
		s, volh := mk(t)

		wtxh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true})
		buf := Load(t, s, wtxh)
		require.Equal(t, []byte{}, buf)

		// Open 10 readers.
		rtxhs := make([]blobcache.Handle, 10)
		for i := range rtxhs {
			rtxhs[i] = BeginTx(t, s, volh, blobcache.TxParams{Modify: false})
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
		rtxh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true})
		defer Abort(t, s, rtxh)
		buf2 := Load(t, s, rtxh)
		require.Equal(t, root2, buf2)
	})
	t.Run("WriteN", func(t *testing.T) {
		s, volh := mk(t)
		const N = 10
		for i := 0; i < N; i++ {
			func() {
				wtxh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true})
				SaveCommit(t, s, wtxh, []byte{byte(i)})
			}()
		}
		txh := BeginTx(t, s, volh, blobcache.TxParams{Modify: false})
		defer Abort(t, s, txh)
		buf := Load(t, s, txh)
		require.Equal(t, []byte{9}, buf)
	})
	t.Run("Visited", func(t *testing.T) {
		// check to see if IsVisited and Visit work.
		s, volh := mk(t)
		txh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true, GCBlobs: true})
		defer Abort(t, s, txh)

		hf := defaultLocalSpec().Local.HashAlgo.HashFunc()
		data1 := []byte("hello world")
		cid1 := hf(nil, data1)
		// should not be visited yet.
		require.Equal(t, []bool{false}, IsVisited(t, s, txh, []blobcache.CID{cid1}))
		Post(t, s, txh, data1, blobcache.PostOpts{})
		require.Equal(t, []bool{false}, IsVisited(t, s, txh, []blobcache.CID{cid1}))
		Visit(t, s, txh, []blobcache.CID{cid1})
		require.Equal(t, []bool{true}, IsVisited(t, s, txh, []blobcache.CID{cid1}))
	})
	t.Run("GCHalf", func(t *testing.T) {
		// This test performs 3 transactions.
		// 1. Open a transaction, add 20 blobs to a volume, then commit.
		// 2. Open a GC transaction, visit half of the blobs, then commit.
		// 3. Open a read only transaction and ensure each of the visited blobs exist, and none of the unvisited blobs exist.
		ctx := testutil.Context(t)
		s, volh := mk(t)
		// 1.
		txh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true})
		defer s.Abort(ctx, txh)
		var cids []blobcache.CID
		for i := 0; i < 20; i++ {
			data := fmt.Appendf(nil, "some data %d", i)
			cid := Post(t, s, txh, data, blobcache.PostOpts{})
			cids = append(cids, cid)
		}
		Commit(t, s, txh)
		// 2.
		txh = BeginTx(t, s, volh, blobcache.TxParams{Modify: true, GCBlobs: true})
		defer s.Abort(ctx, txh)
		for i, cid := range cids {
			if i%2 > 0 {
				continue // odd blobs are unvisited.
			}
			IsVisited(t, s, txh, []blobcache.CID{cid})
			Visit(t, s, txh, []blobcache.CID{cid})
		}
		Commit(t, s, txh)
		// 3.
		txh = BeginTx(t, s, volh, blobcache.TxParams{})
		defer s.Abort(ctx, txh)
		for i, cid := range cids {
			if i%2 == 0 {
				// check that the even blobs are visited.
				require.True(t, Exists(t, s, txh, cid))
				GetBytes(t, s, txh, cid, blobcache.GetOpts{}, 100)
			} else {
				// check that the odd blobs are unvisited.
				require.False(t, Exists(t, s, txh, cid))
			}
		}
	})
	t.Run("GCVisitResets", func(t *testing.T) {
		// This test checks that the visited set is reset every time a GC transaction is opened.
		ctx := testutil.Context(t)
		s, volh := mk(t)
		// Open a transaction add 20 blobs, the commit.
		txh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true})
		defer s.Abort(ctx, txh)
		var cids []blobcache.CID
		for i := 0; i < 20; i++ {
			data := fmt.Appendf(nil, "some data %d", i)
			cid := Post(t, s, txh, data, blobcache.PostOpts{})
			cids = append(cids, cid)
		}
		Commit(t, s, txh)

		// Visited should reset at the start of each GC transaction.
		for i := 0; i < 3; i++ {
			// Open a GC transaction, for each blob, check that it is unvisited, then mark it visited.
			// This transaction should be a no-op.
			txh = BeginTx(t, s, volh, blobcache.TxParams{Modify: true, GCBlobs: true})
			defer s.Abort(ctx, txh)
			for _, cid := range cids {
				vis := IsVisited(t, s, txh, []blobcache.CID{cid})
				require.False(t, vis[0])
				Visit(t, s, txh, []blobcache.CID{cid})
				vis = IsVisited(t, s, txh, []blobcache.CID{cid})
				require.True(t, vis[0])
			}
			Commit(t, s, txh)
		}
	})
	t.Run("GCPostUnvisited", func(t *testing.T) {
		// This test checks that blobs posted, but not visited, are still removed by GC.
		ctx := testutil.Context(t)
		s, volh := mk(t)
		txh := BeginTx(t, s, volh, blobcache.TxParams{Modify: true, GCBlobs: true})
		defer s.Abort(ctx, txh)
		var cids []blobcache.CID
		for i := 0; i < 20; i++ {
			data := fmt.Appendf(nil, "some data %d", i)
			cid := Post(t, s, txh, data, blobcache.PostOpts{})
			cids = append(cids, cid)
		}
		Commit(t, s, txh)

		txh = BeginTx(t, s, volh, blobcache.TxParams{})
		defer s.Abort(ctx, txh)
		for _, cid := range cids {
			require.False(t, Exists(t, s, txh, cid))
		}
	})
	t.Run("Link", func(t *testing.T) {
		// This test checks that volumes with the NONE schema can be nested arbitrarily deep.
		ctx := testutil.Context(t)
		s, rootVolh := mk(t)

		// Create a spec for NONE schema volumes.
		noneSpec := blobcache.VolumeSpec{
			Local: &blobcache.VolumeBackend_Local{
				Schema:   blobcache.SchemaSpec{Name: blobcache.Schema_NONE},
				HashAlgo: blobcache.HashAlgo_BLAKE3_256,
				MaxSize:  1 << 21,
			},
		}

		// Create 10 nested volumes with NONE schema.
		vol1h := rootVolh
		for i := 0; i < 10; i++ {
			// Open a transaction on the current volume.
			txh := BeginTx(t, s, vol1h, blobcache.TxParams{Modify: true})

			// Create a new child volume.
			vol2h, _ := CreateOnSameHost(t, s, vol1h, noneSpec)

			// Link the child volume to grant access from the parent.
			ltok := Link(t, s, txh, vol2h, blobcache.Action_ALL)

			// Store the child volume's OID in the parent's cell.
			Save(t, s, txh, ltok.Marshal(nil))
			Commit(t, s, txh)

			vol1h = vol2h
		}

		// Now traverse back down the nesting to verify we can open each level.
		vol1h = rootVolh
		for i := 0; i < 10; i++ {
			// Open a read-only transaction.
			txh := BeginTx(t, s, vol1h, blobcache.TxParams{Modify: false})
			defer Abort(t, s, txh)

			// Load the child OID from the parent's cell.
			rootData := Load(t, s, txh)
			require.NotEmpty(t, rootData, "root should not be empty at level %d", i)

			// Parse the OID.
			var ltok blobcache.LinkToken
			require.NoError(t, ltok.Unmarshal(rootData))

			// Open the child volume from the parent.
			vol2h, err := s.OpenFrom(ctx, vol1h, ltok, blobcache.Action_ALL)
			require.NoError(t, err)
			require.NotNil(t, vol2h)

			vol1h = *vol2h
		}
	})
}
