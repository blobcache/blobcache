package tries

import (
	"context"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"testing"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
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
	const N = 10000

	x := New(s)

	for i := 0; i < N; i++ {
		buf := []byte(fmt.Sprintf("test-value-%d", i))
		id := blobs.Hash(buf)
		err := x.Put(ctx, id[:], buf)
		require.Nil(t, err)
	}
	assert.True(t, x.IsParent(), "should be parent")
	for i := 0; i < 256; i++ {
		id := x.GetChildRef(byte(i))
		_, err := s.Get(ctx, id)
		assert.Nil(t, err)
	}
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
