package volumes

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"golang.org/x/crypto/chacha20poly1305"
)

var _ Volume = &RootAEAD{}

// RootAEAD is a volume that encrypts the root with an AEAD.
// The blobs are left unchanged.
// This is useful for schemas that already encrypt the blobs that they store.
type RootAEAD struct {
	Inner Volume
	AEAD  cipher.AEAD
}

func NewChaCha20Poly1305(inner Volume, secret *[32]byte) *RootAEAD {
	aead, err := chacha20poly1305.NewX(secret[:])
	if err != nil {
		panic(err)
	}
	return &RootAEAD{AEAD: aead, Inner: inner}
}

func (v *RootAEAD) BeginTx(ctx context.Context, spec blobcache.TxParams) (Tx, error) {
	inner, err := v.Inner.BeginTx(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &RootAEADTx{aead: v.AEAD, inner: inner, vol: v}, nil
}

func (v *RootAEAD) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return v.Inner.Await(ctx, prev, next)
}

var _ Tx = &RootAEADTx{}

type RootAEADTx struct {
	aead  cipher.AEAD
	inner Tx
	vol   Volume
}

func (tx *RootAEADTx) Volume() Volume {
	return tx.vol
}

func (tx *RootAEADTx) Commit(ctx context.Context, ptext []byte) error {
	nonce := make([]byte, tx.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	ctext := tx.aead.Seal(nonce, nonce, ptext, nil)
	return tx.inner.Commit(ctx, ctext)
}

func (tx *RootAEADTx) Abort(ctx context.Context) error {
	return tx.inner.Abort(ctx)
}

func (tx *RootAEADTx) Load(ctx context.Context, dst *[]byte) error {
	if err := tx.inner.Load(ctx, dst); err != nil {
		return err
	}
	// as a special case if the plaintext is empty, then we return nil.
	if len(*dst) == 0 {
		*dst = (*dst)[:0]
		return nil
	}
	if len(*dst) < tx.aead.NonceSize() {
		return fmt.Errorf("too small to contain 24 byte nonce: %d", len(*dst))
	}
	nonce := (*dst)[:tx.aead.NonceSize()]
	ctext := (*dst)[tx.aead.NonceSize():]
	plaintext, err := tx.aead.Open(ctext[:0], nonce[:], ctext, nil)
	if err != nil {
		return err
	}
	*dst = plaintext
	return nil
}

func (tx *RootAEADTx) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	return tx.inner.Post(ctx, salt, data)
}

func (tx *RootAEADTx) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	return tx.inner.Get(ctx, cid, salt, buf)
}

func (tx *RootAEADTx) Delete(ctx context.Context, cid blobcache.CID) error {
	return tx.inner.Delete(ctx, cid)
}

func (tx *RootAEADTx) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	return tx.inner.Exists(ctx, cid)
}

func (tx *RootAEADTx) MaxSize() int {
	return tx.inner.MaxSize()
}

func (tx *RootAEADTx) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return tx.inner.Hash(salt, data)
}
