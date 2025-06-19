package volumes

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bccrypto"
	"blobcache.io/blobcache/src/internal/tries"
	"go.brendoncarroll.net/state/cadata"
)

var _ Volume[[]byte] = &Vault{}

type Vault struct {
	inner Volume[[]byte]
}

func (v *Vault) BeginTx(ctx context.Context, params blobcache.TxParams) (Tx[[]byte], error) {
	inner, err := v.inner.BeginTx(ctx, params)
	if err != nil {
		return nil, err
	}
	return newVaultTx(inner), nil
}

func (v *Vault) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return v.inner.Await(ctx, prev, next)
}

var _ Tx[[]byte] = &VaultTx{}

type VaultTx struct {
	inner  Tx[[]byte]
	crypto *bccrypto.Worker
	tries  *tries.Operator

	blobs map[blobcache.CID]bccrypto.Ref
}

func newVaultTx(inner Tx[[]byte]) *VaultTx {
	trieOp := tries.NewOperator()
	return &VaultTx{
		inner:  inner,
		crypto: bccrypto.NewWorker(nil),
		tries:  trieOp,

		blobs: make(map[blobcache.CID]bccrypto.Ref),
	}
}

func (v *VaultTx) ID() blobcache.OID {
	return blobcache.OID{}
}

func (v *VaultTx) Load(ctx context.Context, dst *[]byte) error {
	return v.inner.Load(ctx, dst)
}

func (v *VaultTx) Commit(ctx context.Context, root []byte) error {
	b := v.tries.NewBuilder(unsaltedStore{v.inner}, 1024)
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
	ref, err := v.crypto.Post(ctx, unsaltedStore{v.inner}, data)
	if err != nil {
		return blobcache.CID{}, err
	}
	ptextCID := v.inner.Hash(data)
	v.blobs[ptextCID] = ref
	return ptextCID, nil
}

func (v *VaultTx) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	if ref, ok := v.blobs[cid]; ok && !ref.IsZero() {
		return v.crypto.Get(ctx, unsaltedStore{v.inner}, ref, buf)
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
	dek, err := v.tries.Get(ctx, unsaltedStore{v.inner}, trieRoot, cid[:])
	if err != nil {
		return 0, err
	}
	ref := bccrypto.Ref{DEK: bccrypto.DEK(dek)}
	return v.crypto.Get(ctx, unsaltedStore{v.inner}, ref, buf)
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

func (v *VaultTx) Hash(data []byte) blobcache.CID {
	return v.inner.Hash(data)
}

// This is an adapter to a store, since we added salts to the API.
type unsaltedStore struct {
	inner Tx[[]byte]
}

func (v unsaltedStore) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	return v.inner.Post(ctx, nil, data)
}

func (v unsaltedStore) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	return v.inner.Get(ctx, cid, nil, buf)
}

func (v unsaltedStore) Delete(ctx context.Context, cid blobcache.CID) error {
	return v.inner.Delete(ctx, cid)
}

func (v unsaltedStore) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	return v.inner.Exists(ctx, cid)
}

func (v unsaltedStore) MaxSize() int {
	return v.inner.MaxSize()
}

func (v unsaltedStore) Hash(data []byte) blobcache.CID {
	return v.inner.Hash(data)
}
