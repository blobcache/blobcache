package bcsdk

import (
	"context"
	"crypto/hmac"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/crypto/chacha20"
)

type CCacheParams[T any] struct {
	Size       int
	MaxObjSize int
	Salt       blobcache.CID
	Marshal    Marshaler[T]
	Parse      Parser[T]
}

// CCache caches decrypted and parsed values from a store
type CCache[T any] struct {
	maxObjSize int
	salt       [32]byte
	marshal    Marshaler[T]
	parse      Parser[T]
	deriveKey  func(key *[32]byte, extra []byte) [32]byte

	c lru.Cache[blobcache.CID, T]
}

func NewCCache[T any](p CCacheParams[T]) CCache[T] {
	cache, err := lru.New[blobcache.CID, T](p.Size)
	if err != nil {
		panic(err)
	}
	return CCache[T]{
		salt:       p.Salt,
		maxObjSize: p.MaxObjSize,
		marshal:    p.Marshal,
		parse:      p.Parse,

		c: *cache,
	}
}

func (c *CCache[T]) Post(ctx context.Context, s WO, x T) (CRef[T], error) {
	buf := c.acquire()
	defer c.release(buf)
	data := c.marshal(x, buf[:0])
	dek := c.encrypt(s, data, data)
	cid, err := s.Post(ctx, data)
	if err != nil {
		return CRef[T]{}, err
	}
	return NewCRef[T](cid, dek), nil
}

func (c *CCache[T]) Get(ctx context.Context, s RO, ref CRef[T]) (T, error) {
	cid := ref.CID()
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
	c.decrypt(ref.DEK(), buf)
	ret, err := c.parse(buf[:n])
	if err != nil {
		var zero T
		return zero, err
	}
	c.c.Add(cid, ret)
	return ret, nil
}

func (c *CCache[T]) acquire() []byte {
	return make([]byte, c.maxObjSize)
}

func (c *CCache[T]) release([]byte) {
	// TODO
}

func (c *CCache[T]) encrypt(s WO, src []byte, dst []byte) blobcache.DEK {
	pthash := s.Hash(src)
	dek := c.deriveKey(&c.salt, pthash[:])
	c.xorKeyStream(dek, src, dst)
	return dek
}

func (c *CCache[T]) decrypt(dek blobcache.DEK, buf []byte) {
	c.xorKeyStream(dek, buf, buf)
}

func (c *CCache[T]) xorKeyStream(key [32]byte, src, dst []byte) {
	var nonce [12]byte
	ciph, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	ciph.XORKeyStream(dst, src)
}

// CRef is a reference to data encrypted with a cipher
type CRef[T any] struct {
	ref Ref[T]
	dek blobcache.DEK
}

func NewCRef[T any](cid blobcache.CID, dek [32]byte) CRef[T] {
	return CRef[T]{ref: NewRef[T](cid), dek: dek}
}

// IsZero returns true if the CID is zero, it does not check the DEK.
func (tr *CRef[T]) IsZero() bool {
	return tr.ref.IsZero()
}

// Equals returns true if the CID and DEK match other.
func (tr *CRef[T]) Equals(other *CRef[T]) bool {
	return tr.ref.cid == other.ref.cid && hmac.Equal(tr.dek[:], other.dek[:])
}

func (tr CRef[T]) Marshal(out []byte) []byte {
	out = append(out, tr.ref.cid[:]...)
	out = append(out, tr.dek[:]...)
	return out
}

func (tr *CRef[T]) Unmarshal(data []byte) error {
	if len(data) != blobcache.CIDSize+blobcache.DEKSize {
		return fmt.Errorf("wrong size for blobcache.CID")
	}
	// CID
	tr.ref.cid = blobcache.CID(data[:blobcache.CIDSize])
	data = data[blobcache.CIDSize:]
	// DEK
	tr.dek = [32]byte(data[:blobcache.DEKSize])
	return nil
}

func (tr *CRef[T]) CID() blobcache.CID {
	return tr.ref.cid
}

func (tr *CRef[T]) DEK() blobcache.DEK {
	return tr.dek
}
