package bcpool

import (
	"sync"
)

const MaxSize = 1 << 22

type buffer = [MaxSize]byte

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &buffer{}
	},
}

func WithBuffer(fn func(*[MaxSize]byte) error) error {
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
