package bccrypto

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"golang.org/x/crypto/chacha20"
)

// DeriveKey takes 256 bits of entropy, a hash function, and additional material and returns a 32 byte DEK.
func DeriveKey(hf blobcache.HashFunc, entropy *[32]byte, additional []byte) DEK {
	salt := (*blobcache.CID)(entropy)
	return DEK(hf(salt, additional))
}

// DEKSize is the number of bytes in a DEK.
const DEKSize = 32

// DEK is a Data Encryption Key.
type DEK [DEKSize]byte

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

const RefSize = 32 + 32

// Ref contains a CID and a DEK.
type Ref struct {
	CID blobcache.CID
	DEK DEK
}

func (r Ref) IsZero() bool {
	return r.CID.IsZero() && r.DEK.IsZero()
}

func (r Ref) Marshal(out []byte) []byte {
	out = append(out, r.CID[:]...)
	out = append(out, r.DEK[:]...)
	return out
}

func (r *Ref) Unmarshal(data []byte) error {
	if len(data) < RefSize {
		return fmt.Errorf("too short to be ref")
	}
	r.CID = blobcache.CID(data[0:32])
	r.DEK = DEK(data[32:64])
	return nil
}

// Machine contains caches and configuration.
type Machine struct {
	salt *[32]byte
	hf   blobcache.HashFunc
}

func NewMachine(salt *blobcache.CID, hf blobcache.HashFunc) *Machine {
	return &Machine{hf: hf, salt: (*[32]byte)(salt)}
}

func (w *Machine) Post(ctx context.Context, s schema.WO, data []byte) (Ref, error) {
	ptextCID := s.Hash(data)
	dek := DeriveKey(w.hf, w.salt, ptextCID[:])
	ctext := make([]byte, len(data))
	cryptoXOR(&dek, ctext, data)
	ctextCID, err := s.Post(ctx, ctext)
	if err != nil {
		return Ref{}, err
	}
	return Ref{CID: ctextCID, DEK: dek}, nil
}

func (w *Machine) Get(ctx context.Context, s schema.RO, ref Ref, buf []byte) (int, error) {
	n, err := s.Get(ctx, ref.CID, buf)
	if err != nil {
		return 0, err
	}
	cryptoXOR(&ref.DEK, buf[:n], buf[:n])
	return n, nil
}

func (w *Machine) GetF(ctx context.Context, s schema.RO, ref Ref, fn func([]byte) error) error {
	buf := make([]byte, s.MaxSize())
	n, err := w.Get(ctx, s, ref, buf)
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
