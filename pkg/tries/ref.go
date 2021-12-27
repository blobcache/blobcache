package tries

import (
	"context"

	"github.com/blobcache/blobcache/pkg/bccrypto"
	"github.com/brendoncarroll/go-state/cadata"
)

type Ref struct {
	ID     cadata.ID
	DEK    *bccrypto.DEK
	Length int
}

func toChildProto(r Ref) *ChildRef {
	var dek []byte
	if r.DEK != nil {
		dek = r.DEK[:]
	}
	return &ChildRef{Id: r.ID[:], Dek: dek}
}

func fromChildProto(x *ChildRef) Ref {
	dek := new(bccrypto.DEK)
	copy(dek[:], x.Dek)
	return Ref{
		ID:  cadata.IDFromBytes(x.Id),
		DEK: dek,
	}
}

func post(ctx context.Context, s cadata.Poster, ptext []byte) (*Ref, error) {
	l := len(ptext)
	id, dek, err := bccrypto.Post(ctx, s, bccrypto.Convergent, ptext)
	if err != nil {
		return nil, err
	}
	return &Ref{
		ID:     id,
		DEK:    dek,
		Length: l,
	}, nil
}

func getF(ctx context.Context, s cadata.Getter, ref Ref, fn func([]byte) error) error {
	l := ref.Length
	return bccrypto.GetF(ctx, s, *ref.DEK, ref.ID, func(data []byte) error {
		return fn(data[:l])
	})
}
