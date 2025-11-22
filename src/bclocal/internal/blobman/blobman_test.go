package blobman

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"blobcache.io/blobcache/src/bclocal/internal/blobman/shard"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

func mkKey(t testing.TB, i int) CID {
	t.Helper()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:8], uint64(i))
	h := blake3.Sum256(b[:])
	return CID(h)
}

func TestStorePutGetSingle(t *testing.T) {
	st := setup(t, 256, 1024)

	key := mkKey(t, 0)
	val := []byte("hello-world")

	ok, err := st.Put(key, val)
	require.NoError(t, err)
	require.True(t, ok)

	// duplicate put should be ignored
	ok2, err := st.Put(key, []byte("hello-world-2"))
	require.NoError(t, err)
	require.False(t, ok2)

	// get should return original value
	got := make([]byte, 0, len(val))
	found, err := st.Get(key, func(data []byte) {
		got = append(got[:0], data...)
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, val, got)
}

func TestPutGetBatch(t *testing.T) {
	st := setup(t, 256, 1024)

	const N = 2000
	keys := make([]CID, N)
	vals := make([][]byte, N)
	for i := range N {
		keys[i] = mkKey(t, i)
		vals[i] = fmt.Appendf(nil, "val-%d", i)
		ok, err := st.Put(keys[i], vals[i])
		require.NoError(t, err)
		require.True(t, ok)
	}
	for i := range N {
		var got []byte
		ok, err := st.Get(keys[i], func(data []byte) { got = append([]byte(nil), data...) })
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, vals[i], got)
	}
}

func TestStoreGetMissing(t *testing.T) {
	st := setup(t, 256, 1024)

	missing := mkKey(t, 0)
	ok, err := st.Get(missing, func(data []byte) {})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPutDelete(t *testing.T) {
	st := setup(t, 256, 1024)

	var keys []CID
	for i := 0; i < 10; i++ {
		key := mkKey(t, i)
		val := []byte("hello-world")
		_, err := st.Put(key, val)
		require.NoError(t, err)
		keys = append(keys, key)
	}

	for i := 0; i < len(keys)-1; i++ {
		err := st.Delete(keys[i])
		require.NoError(t, err)
		ok, err := st.Get(keys[i], func(data []byte) {})
		require.NoError(t, err)
		require.False(t, ok)
		ok, err = st.Get(keys[i+1], func(data []byte) {})
		require.NoError(t, err)
		require.True(t, ok)
	}
}

func TestPutReloadGet(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	st := New(root)
	val := []byte("hello-world")

	var keys []CID
	for i := 0; i < 10; i++ {
		key := mkKey(t, i)
		ok, err := st.Put(key, val)
		require.NoError(t, err)
		require.True(t, ok)
		keys = append(keys, key)
	}
	require.NoError(t, st.Close())

	root2, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root2.Close()
	st2 := New(root2)
	for i := range keys {
		ok, err := st2.Get(keys[i], func(data []byte) {})
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, st2.Close())
}

func TestExceedMaxPackSize(t *testing.T) {
	st := setup(t, 256, 1024)

	key := mkKey(t, 0)
	val := make([]byte, 1025)
	_, err := rand.Read(val)
	require.NoError(t, err)

	ok, err := st.Put(key, val)
	require.Error(t, err)
	require.False(t, ok)

	ok, err = st.Put(key, val[:10])
	require.NoError(t, err)
	require.True(t, ok)
}

func TestPutLarge(t *testing.T) {
	// make sure we run out of pack space not table space
	st := setup(t, 1024, 1024)

	for i := range 10 {
		key := mkKey(t, i)
		val := make([]byte, 500)
		ok, err := st.Put(key, val)
		require.NoError(t, err, i)
		require.True(t, ok, i)
	}

	for i := range 10 {
		key := mkKey(t, i)
		ok, err := st.Get(key, func(data []byte) {})
		require.NoError(t, err)
		require.True(t, ok)
	}
	if tl := st.trie.shard.TableLen(); tl > 2 {
		t.Fatalf("table len is %d", tl)
	}
}

func TestGetAfterRestartWithChildShard(t *testing.T) {
	// Create store with tiny pack to trigger child shard writes.
	st := setup(t, 256, 1024)

	key0 := mkKey(t, 0)
	key1 := mkKey(t, 1)
	key2 := mkKey(t, 2)
	val := make([]byte, 500)
	_, _ = rand.Read(val)

	for _, k := range []CID{key0, key1, key2} {
		ok, err := st.Put(k, val)
		require.NoError(t, err)
		require.True(t, ok)
	}
	// Verify all are readable before restart.
	for _, k := range []CID{key0, key1, key2} {
		found, err := st.Get(k, func([]byte) {})
		require.NoError(t, err)
		require.True(t, found)
	}
	require.NoError(t, st.Close())

	// Reopen the same root and try to read again.
	// Rebuild like setup() but without writing any children pointers.
	dir := st.root.Name() // if setup exposes path; otherwise refactor setup to return the dir.
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	st2 := New(root)
	defer st2.Close()

	found0, _ := st2.Get(key0, func([]byte) {})
	found1, _ := st2.Get(key1, func([]byte) {})
	found2, _ := st2.Get(key2, func([]byte) {})
	require.True(t, found0)
	require.True(t, found1)
	require.True(t, found2) // This was the bug.
}

func BenchmarkGet(b *testing.B) {
	st := setup(b, shard.DefaultMaxTableLen, shard.DefaultMaxPackSize)

	const numKeys = 1e5
	var keys []CID
	for i := 0; i < numKeys; i++ {
		key := mkKey(b, i)
		_, err := st.Put(key, []byte("hello-world"))
		require.NoError(b, err)
		if i%100 == 0 {
			// retain 1% of the keys
			keys = append(keys, key)
		}
	}

	for i := 0; b.Loop(); i++ {
		key := keys[i%len(keys)]
		_, err := st.Get(key, func(data []byte) {})
		if err != nil {
			require.NoError(b, err)
		}
	}
}

func setup(t testing.TB, maxTabLen uint32, maxPackSize uint32) *Store {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, root.Close())
	})
	st := New(root)
	st.maxTableLen = maxTabLen
	st.maxPackSize = maxPackSize
	return st
}
