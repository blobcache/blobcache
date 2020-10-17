package tries

import (
	"context"

	"github.com/blobcache/blobcache/pkg/bcpool"
	"github.com/blobcache/blobcache/pkg/blobs"
	"golang.org/x/crypto/chacha20"
)

type Ref struct {
	ID     blobs.ID
	DEK    *[32]byte
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
	dek := new([32]byte)
	copy(dek[:], x.Dek)
	return Ref{
		ID:  blobs.IDFromBytes(x.Id),
		DEK: dek,
	}
}

func post(ctx context.Context, s blobs.Poster, ptext []byte) (*Ref, error) {
	key := blobs.Hash(ptext)
	ctext := bcpool.Acquire()
	defer bcpool.Release(ctext)
	l := len(ptext)
	cryptoXOR(key[:], ctext[:], ptext)
	// TODO: not sure if we want to make all the blobs max size
	// cryptoXOR(key[:], ctext[l:], ctext[l:])
	// id, err := s.Post(ctx, ctext[:])
	id, err := s.Post(ctx, ctext[:l])
	if err != nil {
		return nil, err
	}
	return &Ref{ID: id, DEK: (*[32]byte)(&key), Length: l}, nil
}

func getF(ctx context.Context, s blobs.Getter, ref Ref, fn func([]byte) error) error {
	l := ref.Length
	buf := bcpool.Acquire()
	defer bcpool.Release(buf)
	if err := s.GetF(ctx, ref.ID, func(ctext []byte) error {
		cryptoXOR(ref.DEK[:], buf[:l], ctext[:l])
		return nil
	}); err != nil {
		return err
	}
	return fn(buf[:l])
}

func cryptoXOR(key, dst, src []byte) {
	nonce := [12]byte{}
	cipher, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	cipher.XORKeyStream(dst, src)
}
