package volumes

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bccrypto"
	"blobcache.io/blobcache/src/internal/tries"
	"go.brendoncarroll.net/state/cadata"
)

var _ Volume = &Vault{}

type Vault struct {
	inner  Volume
	secret [32]byte
}

func NewVault(inner Volume, secret [32]byte) *Vault {
	return &Vault{
		inner:  inner,
		secret: secret,
	}
}

func (v *Vault) BeginTx(ctx context.Context, params blobcache.TxParams) (Tx, error) {
	inner, err := v.inner.BeginTx(ctx, params)
	if err != nil {
		return nil, err
	}
	return newVaultTx(v, inner), nil
}

func (v *Vault) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return v.inner.Await(ctx, prev, next)
}

var _ Tx = &VaultTx{}

type VaultTx struct {
	vol    *Vault
	inner  Tx
	crypto *bccrypto.Worker
	tries  *tries.Operator

	blobs map[blobcache.CID]bccrypto.Ref
}

func newVaultTx(vol *Vault, inner Tx) *VaultTx {
	trieOp := tries.NewOperator()
	return &VaultTx{
		vol:    vol,
		inner:  inner,
		crypto: bccrypto.NewWorker(nil),
		tries:  trieOp,

		blobs: make(map[blobcache.CID]bccrypto.Ref),
	}
}

func (tx *VaultTx) Volume() Volume {
	return tx.vol
}

func (v *VaultTx) Load(ctx context.Context, dst *[]byte) error {
	return v.inner.Load(ctx, dst)
}

func (v *VaultTx) Commit(ctx context.Context, root []byte) error {
	b := v.tries.NewBuilder(UnsaltedStore{v.inner}, 1024)
	for cid, ref := range v.blobs {
		if ref.IsZero() {
			continue
		}
		if err := b.Put(ctx, cid[:], ref.DEK[:]); err != nil {
			return err
		}
	}
	trieRoot, err := b.Finish(ctx)
	if err != nil {
		return err
	}
	root2, err := json.Marshal(trieRoot)
	if err != nil {
		return err
	}
	return v.inner.Commit(ctx, root2)
}

func (v *VaultTx) Abort(ctx context.Context) error {
	return v.inner.Abort(ctx)
}

func (v *VaultTx) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	ref, err := v.crypto.Post(ctx, UnsaltedStore{v.inner}, data)
	if err != nil {
		return blobcache.CID{}, err
	}
	ptextCID := v.inner.Hash(salt, data)
	v.blobs[ptextCID] = ref
	return ptextCID, nil
}

func (v *VaultTx) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	if ref, ok := v.blobs[cid]; ok && !ref.IsZero() {
		return v.crypto.Get(ctx, UnsaltedStore{v.inner}, ref, buf)
	} else if ok {
		return 0, cadata.ErrNotFound{Key: cid}
	}
	var root []byte
	if err := v.inner.Load(ctx, &root); err != nil {
		return 0, err
	}
	var trieRoot tries.Root
	if err := json.Unmarshal(root, &trieRoot); err != nil {
		return 0, err
	}
	dek, err := v.tries.Get(ctx, UnsaltedStore{v.inner}, trieRoot, cid[:])
	if err != nil {
		return 0, err
	}
	ref := bccrypto.Ref{CID: cid, DEK: bccrypto.DEK(dek)}
	return v.crypto.Get(ctx, UnsaltedStore{v.inner}, ref, buf)
}

func (v *VaultTx) Delete(ctx context.Context, cid blobcache.CID) error {
	if ref, exists := v.blobs[cid]; exists {
		v.inner.Delete(ctx, ref.CID)
	}
	v.blobs[cid] = bccrypto.Ref{}
	return nil
}

func (v *VaultTx) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	if ref, exists := v.blobs[cid]; exists {
		return !ref.IsZero(), nil
	}
	return v.inner.Exists(ctx, cid)
}

func (v *VaultTx) MaxSize() int {
	return v.inner.MaxSize()
}

func (v *VaultTx) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.inner.Hash(salt, data)
}

// This is an adapter to a store, since we added salts to the API.
type UnsaltedStore struct {
	inner Tx
}

func NewUnsaltedStore(inner Tx) *UnsaltedStore {
	return &UnsaltedStore{inner: inner}
}

func (v UnsaltedStore) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return v.inner.Post(ctx, nil, data)
}

func (v UnsaltedStore) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return v.inner.Get(ctx, cid, nil, buf)
}

func (v UnsaltedStore) Delete(ctx context.Context, cid blobcache.CID) error {
	return v.inner.Delete(ctx, cid)
}

func (v UnsaltedStore) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	return v.inner.Exists(ctx, cid)
}

func (v UnsaltedStore) MaxSize() int {
	return v.inner.MaxSize()
}

func (v UnsaltedStore) Hash(data []byte) blobcache.CID {
	return v.inner.Hash(nil, data)
}
