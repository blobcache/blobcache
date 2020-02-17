package tries

import (
	"context"
	"fmt"
	mrand "math/rand"
	"testing"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/stretchr/testify/assert"
)

func TestMarshal(t *testing.T) {
	ctx := context.TODO()
	s := blobs.NewMem()
	x := New(s)
	x.Prefix = []byte{'0'}

	const N = 10
	for i := 0; i < N; i++ {
		buf := make([]byte, 32)
		mrand.Read(buf)
		buf[0] = '0' // match prefix from above
		err := x.Put(ctx, buf, []byte(fmt.Sprint("test value", i)))
		assert.Nil(t, err)
	}
	data := x.Marshal()
	t.Log(string(data))
	_, err := FromBytes(x.store, data)
	assert.Nil(t, err)
}
