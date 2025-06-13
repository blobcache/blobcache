package volumes

import (
	"context"
	"crypto/cipher"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/tries"
	"golang.org/x/crypto/chacha20poly1305"
)

var _ Tx[[]byte] = &Vault{}

type Vault struct {
	inner  Tx[[]byte]
	cipher cipher.AEAD
	hash   blobcache.HashFunc

	blobs map[blobcache.CID]struct{}
	tb    tries.Builder
}

func NewVault(inner Tx[[]byte], secret *[32]byte) *Vault {
	cipher, err := chacha20poly1305.NewX(secret[:])
	if err != nil {
		panic(err)
	}
	return &Vault{
		inner:  inner,
		cipher: cipher,
	}
}

func (v *Vault) ID() blobcache.OID {
	return blobcache.OID{}
}

func (v *Vault) Load(ctx context.Context, dst *[]byte) error {
	return v.inner.Load(ctx, dst)
}

func (v *Vault) Commit(ctx context.Context, root []byte) error {
	if err := v.tb.Put(ctx, []byte{}, root); err != nil {
		return err
	}
	trieRoot, err := v.tb.Finish(ctx)
	if err != nil {
		return err
	}
	root2, err := json.Marshal(trieRoot)
	if err != nil {
		return err
	}
	return v.inner.Commit(ctx, root2)
}

func (v *Vault) Abort(ctx context.Context) error {
	return v.inner.Abort(ctx)
}

func (v *Vault) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	panic("not implemented")
}

func (v *Vault) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	return 0, nil
}

func (v *Vault) Delete(ctx context.Context, cid blobcache.CID) error {
	return nil
}

func (v *Vault) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	return false, nil
}

func (v *Vault) MaxSize() int {
	return v.inner.MaxSize()
}

func (v *Vault) Hash(data []byte) blobcache.CID {
	return v.inner.Hash(data)
}
