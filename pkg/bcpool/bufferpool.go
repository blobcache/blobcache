package bcpool

import (
	"sync"

	"github.com/blobcache/blobcache/pkg/blobs"
)

type buffer = [blobs.MaxSize]byte

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &buffer{}
	},
}

func WithBuffer(fn func(*[blobs.MaxSize]byte) error) error {
	buf := Acquire()
	defer Release(buf)
	return fn(buf)
}

func Acquire() *buffer {
	return bufferPool.Get().(*buffer)
}

func Release(buf *buffer) {
	bufferPool.Put(buf)
}
