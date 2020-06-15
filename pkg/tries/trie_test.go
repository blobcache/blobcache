package tries

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"testing"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	ctx := context.TODO()
	s := blobs.NewMem()
	t1 := NewWithPrefix(s, []byte{'0'})

	const N = 10
	for i := 0; i < N; i++ {
		buf := make([]byte, 32)
		mrand.Read(buf)
		buf[0] = '0' // match prefix from above
		err := t1.Put(ctx, buf, []byte(fmt.Sprint("test value", i)))
		assert.Nil(t, err)
	}
	data := t1.Marshal()
	t.Log(hex.Dump(data))
	t2, err := FromBytes(s, data)
	assert.Nil(t, err)
	assert.Equal(t, t1.IsParent(), t2.IsParent())
	assert.Equal(t, t1.ListEntries(), t2.ListEntries())
	assert.True(t, Equal(t1, t2))
}

func TestSplit(t *testing.T) {
	ctx := context.TODO()
	s := blobs.NewMem()
	x := New(s)

	const MAX = 10000
	split := false
	count := 0
	for i := 0; i < MAX; i++ {
		buf := []byte(fmt.Sprintf("test-value-%d", i))
		id := blobs.Hash(buf)
		err := x.Put(ctx, id[:], buf)
		require.Nil(t, err)

		if x.IsParent() {
			split = true
			break
		}
		count++
	}
	if !split {
		t.Error("trie did not split")
	}

	for i := 0; i < count; i++ {
		buf := []byte(fmt.Sprintf("test-value-%d", i))
		id := blobs.Hash(buf)
		v, err := x.Get(ctx, id[:])
		require.Nil(t, err)
		require.Equal(t, string(buf), string(v), "split trie was missing value")
	}
}

func TestMultiSplit(t *testing.T) {
	ctx := context.TODO()
	s := blobs.NewMem()
	x := New(s)

	const MAX = 10000
	split := false
	count := 0
	largeData := make([]byte, 1<<12)
	for i := 0; i < MAX; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))

		err := x.Put(ctx, buf, largeData)
		require.Nil(t, err)
		if x.IsParent() {
			split = true
			break
		}
		count++
	}
	if !split {
		t.Error("trie did not split")
	}

	for i := 0; i < count; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		v, err := x.Get(ctx, buf)
		require.Nil(t, err)
		require.NotNil(t, v)
	}

	assert.Greater(t, s.Len(), 2)
}

func TestPutGet(t *testing.T) {
	ctx := context.TODO()
	s := blobs.NewMem()
	const N = 10000

	x := New(s)

	// put
	for i := 0; i < N; i++ {
		buf := []byte(fmt.Sprintf("test-value-%d", i))
		id := blobs.Hash(buf)
		err := x.Put(ctx, id[:], buf)
		require.Nil(t, err)
	}
	// get
	for i := 0; i < N; i++ {
		expected := []byte(fmt.Sprintf("test-value-%d", i))
		id := blobs.Hash(expected)
		actual, err := x.Get(ctx, id[:])
		assert.Nil(t, err)
		assert.Equal(t, expected, actual)
	}
}

func TestLopsided(t *testing.T) {
	ctx := context.TODO()
	s := blobs.NewMem()
	x := New(s)

	for i := 0; i < 10000; i++ {
		blobID := blobs.ID{}
		binary.BigEndian.PutUint64(blobID[:], uint64(i))

		err := x.Put(ctx, blobID[:], nil)
		require.Nil(t, err)
	}
}
