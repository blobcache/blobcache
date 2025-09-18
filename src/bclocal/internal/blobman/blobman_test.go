package blobman

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPack(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	maxSize := uint32(1 << 20)
	pf, err := CreatePackFile(root, NewPrefix121([15]byte{}, 0), maxSize)
	require.NoError(t, err)
	defer pf.Close()

	pk, err := NewPack(pf, 0)
	require.NoError(t, err)
	var wg sync.WaitGroup
	const N = 100000
	offsets := make([]uint32, N)
	datas := make([][]byte, N)
	for i := range offsets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			datas[i] = fmt.Appendf(nil, "hello-%d", i)
			offsets[i] = pk.Append(datas[i])
		}()
	}
	wg.Wait()
	require.NoError(t, pk.Flush())

	for i := 0; i < N-1; i++ {
		if offsets[i] == math.MaxUint32 {
			break
		}
		pk.Get(offsets[i], uint32(len(datas[i])), func(data []byte) {
			require.Equal(t, datas[i], data)
		})
	}
}

func randKey(t testing.TB) Key {
	t.Helper()
	var b [16]byte
	_, err := rand.Read(b[:])
	require.NoError(t, err)
	return Key{
		binary.LittleEndian.Uint64(b[:8]),
		binary.LittleEndian.Uint64(b[8:]),
	}
}

func TestStorePutGetSingle(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	st := New(root)

	key := randKey(t)
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

func TestStorePutGetBatch(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	st := New(root)

	const N = 2000
	keys := make([]Key, N)
	vals := make([][]byte, N)
	for i := 0; i < N; i++ {
		keys[i] = randKey(t)
		vals[i] = []byte(fmt.Sprintf("val-%d", i))
		ok, err := st.Put(keys[i], vals[i])
		require.NoError(t, err)
		require.True(t, ok)
	}
	for i := 0; i < N; i++ {
		var got []byte
		ok, err := st.Get(keys[i], func(data []byte) { got = append([]byte(nil), data...) })
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, vals[i], got)
	}
}

func TestStoreGetMissing(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	st := New(root)
	missing := randKey(t)
	ok, err := st.Get(missing, func(data []byte) {})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestStoreLongestPrefixUsesExistingChild(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	st := New(root)

	key := randKey(t)
	childIdx := key.Uint8(0)
	// Pre-create a child shard so Put will use longest available prefix
	child := &shard{}
	st.shard.children[childIdx] = child

	val := []byte("child-route")
	ok, err := st.Put(key, val)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, err)

	// Read back
	var got []byte
	ok, err = st.Get(key, func(data []byte) { got = append([]byte(nil), data...) })
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, val, got)

	// Ensure inserted into child, not root
	require.NoError(t, st.loadShard(&st.shard, key.ToPrefix(0)))
	require.NoError(t, st.loadShard(child, key.ToPrefix(8)))
	require.Equal(t, uint32(0), st.shard.tab.Len())
	require.Equal(t, uint32(1), child.tab.Len())
}

func TestPutDelete(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	st := New(root)

	var keys []Key
	for i := 0; i < 10; i++ {
		key := randKey(t)
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

	var keys []Key
	for i := 0; i < 10; i++ {
		key := randKey(t)
		ok, err := st.Put(key, val)
		require.NoError(t, err)
		require.True(t, ok)
		keys = append(keys, key)
	}
	require.NoError(t, st.Close())

	st2 := New(root)
	for i := range keys {
		ok, err := st2.Get(keys[i], func(data []byte) {})
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, st2.Close())
}

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(b, err)
	defer root.Close()
	st := New(root)

	const numKeys = 1e6
	var keys []Key
	for i := 0; i < numKeys; i++ {
		key := randKey(b)
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
