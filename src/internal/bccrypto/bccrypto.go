package bccrypto

import (
	"context"
	"crypto/rand"
	"encoding/base64"

	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/blake3"
)

func Hash(x []byte) cadata.ID {
	return blake3.Sum256(x)
}

type KeyFunc func(ptextHash cadata.ID) DEK

func SaltedConvergent(salt []byte) KeyFunc {
	return func(ptextHash cadata.ID) DEK {
		var x []byte
		x = append(x, salt[:]...)
		x = append(x, ptextHash[:]...)
		return DEK(Hash(x))
	}
}

func Convergent(ptextHash cadata.ID) DEK {
	return DEK(Hash(ptextHash[:]))
}

func RandomKey(cadata.ID) DEK {
	dek := DEK{}
	if _, err := rand.Read(dek[:]); err != nil {
		panic(err)
	}
	return dek
}

type DEK [32]byte

func (dek DEK) MarshalJSON() ([]byte, error) {
	return []byte(`"` + base64.RawURLEncoding.EncodeToString(dek[:]) + `"`), nil
}

func (dek *DEK) UnmarshalJSON(data []byte) error {
	_, err := base64.RawURLEncoding.Decode(dek[:], data[1:len(data)-1])
	return err
}

func Post(ctx context.Context, s cadata.Poster, keyFunc KeyFunc, data []byte) (cadata.ID, *DEK, error) {
	id := Hash(data)
	dek := keyFunc(id)
	ctext := make([]byte, len(data))
	cryptoXOR(dek, ctext, data)
	id, err := s.Post(ctx, ctext)
	if err != nil {
		return cadata.ID{}, nil, err
	}
	return id, &dek, nil
}

func GetF(ctx context.Context, s cadata.Getter, dek DEK, id cadata.ID, fn func([]byte) error) error {
	buf := make([]byte, s.MaxSize())
	var ptext []byte
	if err := cadata.GetF(ctx, s, id, func(ctext []byte) error {
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
