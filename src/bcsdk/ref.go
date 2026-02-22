package bcsdk

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	lru "github.com/hashicorp/golang-lru/v2"
)

type (
	// Parser parses a type T from a slice of bytes
	Parser[T any] = func(data []byte) (T, error)
	// Marshaler marshals a value of type T and appends it to out, returning the result
	Marshaler[T any] = func(x T, out []byte) []byte
)

type Cache[T any] struct {
	maxObjSize int
	marshal    Marshaler[T]
	parse      Parser[T]
	c          lru.Cache[blobcache.CID, T]
}

func NewCache[T any](size int, maxObjSize int, marshal Marshaler[T], parse Parser[T]) Cache[T] {
	c, err := lru.New[blobcache.CID, T](size)
	if err != nil {
		panic(err)
	}
	// The copy lock warning here is fine, there are no background goroutines spawned.
	// As long as it's not copied after first use, everything should be fine.
	return Cache[T]{
		c:          *c,
		maxObjSize: maxObjSize,
		marshal:    marshal,
		parse:      parse,
	}
}

func (c *Cache[T]) Post(ctx context.Context, s WO, x T) (Ref[T], error) {
	buf := c.acquire()
	defer c.release(buf)
	data := c.marshal(x, buf[:0])
	cid, err := s.Post(ctx, data)
	if err != nil {
		return Ref[T]{}, err
	}
	return Ref[T]{cid: cid}, nil
}

func (c *Cache[T]) Get(ctx context.Context, s RO, tr Ref[T]) (T, error) {
	cid := tr.CID()
	if ret, ok := c.c.Get(cid); ok {
		return ret, nil
	}
	buf := c.acquire()
	defer c.release(buf)
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		var zero T
		return zero, err
	}
	ret, err := c.parse(buf[:n])
	if err != nil {
		var zero T
		return zero, err
	}
	c.c.Add(cid, ret)
	return ret, nil
}

func (c *Cache[T]) acquire() []byte {
	return make([]byte, c.maxObjSize)
}

func (c *Cache[T]) release([]byte) {
	// TODO
}

// Ref is a typed blobcache.CID
type Ref[T any] struct {
	_   [0]T
	cid blobcache.CID
}

func (tr *Ref[T]) IsZero() bool {
	return tr.cid.IsZero()
}

func (tr *Ref[T]) Equals(other *Ref[T]) bool {
	return tr.cid == other.cid
}

func (tr Ref[T]) Marshal(out []byte) []byte {
	return append(out, tr.cid[:]...)
}

func (tr *Ref[T]) Unmarshal(data []byte) error {
	if len(data) != blobcache.CIDSize {
		return fmt.Errorf("wrong size for blobcache.CID")
	}
	tr.cid = blobcache.CID(data)
	return nil
}

func (tr *Ref[T]) CID() blobcache.CID {
	return tr.cid
}

func NewRef[T any](cid blobcache.CID) Ref[T] {
	return Ref[T]{cid: cid}
}
