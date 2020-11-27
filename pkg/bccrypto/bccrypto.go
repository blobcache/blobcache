package bccrypto

import (
	"context"
	"crypto/rand"

	"golang.org/x/crypto/chacha20"

	"github.com/blobcache/blobcache/pkg/bcpool"
	"github.com/blobcache/blobcache/pkg/blobs"
)

type KeyFunc func(ptextHash blobs.ID) DEK

func SaltedConvergent(salt []byte) KeyFunc {
	return func(ptextHash blobs.ID) DEK {
		var x []byte
		x = append(x, salt[:]...)
		x = append(x, ptextHash[:]...)
		return DEK(blobs.Hash(ptextHash[:]))
	}
}

func Convergent(ptextHash blobs.ID) DEK {
	return DEK(blobs.Hash(ptextHash[:]))
}

func RandomKey(blobs.ID) DEK {
	dek := DEK{}
	if _, err := rand.Read(dek[:]); err != nil {
		panic(err)
	}
	return dek
}

type DEK [32]byte

func Post(ctx context.Context, s blobs.Poster, keyFunc KeyFunc, data []byte) (blobs.ID, *DEK, error) {
	id := blobs.Hash(data)
	dek := keyFunc(id)
	buf := bcpool.Acquire()
	defer bcpool.Release(buf)
	ctext := buf[:len(data)]
	cryptoXOR(dek, ctext, data)
	id, err := s.Post(ctx, ctext)
	if err != nil {
		return blobs.ID{}, nil, err
	}
	return id, &dek, nil
}

func GetF(ctx context.Context, s blobs.Getter, dek DEK, id blobs.ID, fn func([]byte) error) error {
	buf := bcpool.Acquire()
	defer bcpool.Release(buf)
	var ptext []byte
	if err := s.GetF(ctx, id, func(ctext []byte) error {
		ptext = buf[:len(ctext)]
		cryptoXOR(dek, ptext, ctext)
		return nil
	}); err != nil {
		return err
	}
	return fn(ptext)
}

func cryptoXOR(key DEK, dst, src []byte) {
	nonce := [chacha20.NonceSize]byte{}
	cipher, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		panic(err)
	}
	cipher.XORKeyStream(dst, src)
}
