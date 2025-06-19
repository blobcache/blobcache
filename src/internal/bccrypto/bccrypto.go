package bccrypto

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/blake3"
)

type KeyFunc func(ptextHash cadata.ID) DEK

func SaltedConvergent(salt *blobcache.CID) KeyFunc {
	return func(ptextHash cadata.ID) DEK {
		var x []byte
		x = append(x, salt[:]...)
		x = append(x, ptextHash[:]...)
		return DEK(blake3.Sum256(x))
	}
}

func Convergent(ptextHash cadata.ID) DEK {
	return DEK(ptextHash[:])
}

func RandomKey(cadata.ID) DEK {
	dek := DEK{}
	if _, err := rand.Read(dek[:]); err != nil {
		panic(err)
	}
	return dek
}

type DEK [32]byte

func (dek *DEK) IsZero() bool {
	zero := [32]byte{}
	return subtle.ConstantTimeCompare(dek[:], zero[:]) == 1
}

func (dek DEK) MarshalJSON() ([]byte, error) {
	return []byte(`"` + base64.RawURLEncoding.EncodeToString(dek[:]) + `"`), nil
}

func (dek *DEK) UnmarshalJSON(data []byte) error {
	_, err := base64.RawURLEncoding.Decode(dek[:], data[1:len(data)-1])
	return err
}

// Ref contains a CID and a DEK.
type Ref struct {
	CID blobcache.CID
	DEK DEK
}

func (r Ref) IsZero() bool {
	return r.CID.IsZero() && r.DEK.IsZero()
}

// Worker contains caches and configuration.
type Worker struct {
	keyFunc KeyFunc
}

func NewWorker(salt *blobcache.CID) *Worker {
	kf := Convergent
	if salt != nil {
		kf = SaltedConvergent(salt)
	}
	return &Worker{keyFunc: kf}
}

func (w *Worker) Post(ctx context.Context, s cadata.Poster, data []byte) (Ref, error) {
	ptextCID := s.Hash(data)
	dek := w.keyFunc(ptextCID)
	ctext := make([]byte, len(data))
	cryptoXOR(&dek, ctext, data)
	ctextCID, err := s.Post(ctx, ctext)
	if err != nil {
		return Ref{}, err
	}
	return Ref{CID: ctextCID, DEK: dek}, nil
}

func (w *Worker) Get(ctx context.Context, s cadata.Getter, ref Ref, buf []byte) (int, error) {
	n, err := s.Get(ctx, ref.CID, buf)
	if err != nil {
		return 0, err
	}
	cryptoXOR(&ref.DEK, buf[:n], buf[:n])
	return n, nil
}

func (w *Worker) GetF(ctx context.Context, s cadata.Getter, ref Ref, fn func([]byte) error) error {
	buf := make([]byte, s.MaxSize())
	n, err := s.Get(ctx, ref.CID, buf)
	if err != nil {
		return err
	}
	return fn(buf[:n])
}
func cryptoXOR(key *DEK, dst, src []byte) {
	nonce := [chacha20.NonceSize]byte{}
	cipher, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	cipher.XORKeyStream(dst, src)
}
