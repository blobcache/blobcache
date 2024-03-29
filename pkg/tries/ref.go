package tries

import (
	"context"
	"encoding/binary"

	"github.com/blobcache/blobcache/pkg/bccrypto"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
	"lukechampine.com/blake3"
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
	id, dek, err := bccrypto.Post(ctx, s, bccrypto.Convergent, ptext)
	if err != nil {
		return nil, err
	}
	return &Ref{
		ID:     id,
		DEK:    dek,
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
	l := ref.Length
	return bccrypto.GetF(ctx, s, *ref.DEK, ref.ID, func(data []byte) error {
		o.cache.ContainsOrAdd(key, append([]byte{}, data...))
		return fn(data[:l])
	})
}
