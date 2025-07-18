package tries

import (
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
	"go.brendoncarroll.net/state/cadata"
	"lukechampine.com/blake3"

	"blobcache.io/blobcache/src/internal/bccrypto"
)

type Ref struct {
	ID     cadata.ID     `json:"id"`
	DEK    *bccrypto.DEK `json:"dek"`
	Length uint32        `json:"length"`
}

const refSize = cadata.IDSize + 32 + 4

func marshalRef(x Ref) []byte {
	buf := [refSize]byte{}
	copy(buf[0:32], x.ID[:])
	copy(buf[32:64], x.DEK[:])
	binary.BigEndian.PutUint32(buf[64:68], uint32(x.Length))
	return buf[:]
}

func parseRef(x []byte) (*Ref, error) {
	if len(x) < refSize {
		return nil, errors.Errorf("tries: data too small to be Ref")
	}
	y := &Ref{DEK: new(bccrypto.DEK)}
	copy(y.ID[:], x[:32])
	copy(y.DEK[:], x[32:64])
	y.Length = binary.BigEndian.Uint32(x[64:])
	return y, nil
}

func (o *Operator) post(ctx context.Context, s cadata.Poster, ptext []byte) (*Ref, error) {
	l := len(ptext)
	ref, err := o.crypto.Post(ctx, s, ptext)
	if err != nil {
		return nil, err
	}
	return &Ref{
		ID:     ref.CID,
		DEK:    &ref.DEK,
		Length: uint32(l),
	}, nil
}

func (o *Operator) getF(ctx context.Context, s cadata.Getter, ref Ref, fn func([]byte) error) error {
	key := blake3.Sum256(marshalRef(ref))
	v, exists := o.cache.Get(key)
	if exists {
		return fn(v.([]byte))
	}
	// TODO: populate cache
	buf := make([]byte, ref.Length)
	ref2 := bccrypto.Ref{CID: ref.ID, DEK: *ref.DEK}
	n, err := o.crypto.Get(ctx, s, ref2, buf)
	if err != nil {
		return err
	}
	o.cache.ContainsOrAdd(key, buf[:])
	return fn(buf[:n])
}
