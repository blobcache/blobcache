package volumes

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bccrypto"
	"blobcache.io/blobcache/src/internal/tries"
	"go.brendoncarroll.net/state/cadata"
)

var _ Volume = &Vault{}

type Vault struct {
	Inner  Volume
	secret [32]byte
}

func NewVault(inner Volume, secret [32]byte) *Vault {
	return &Vault{
		Inner:  inner,
		secret: secret,
	}
}

func (v *Vault) BeginTx(ctx context.Context, params blobcache.TxParams) (Tx, error) {
	inner, err := v.Inner.BeginTx(ctx, params)
	if err != nil {
		return nil, err
	}
	return newVaultTx(v, inner), nil
}

func (v *Vault) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return v.Inner.Await(ctx, prev, next)
}

func (v *Vault) ReadLinks(ctx context.Context, dst LinkSet) error {
	return v.Inner.ReadLinks(ctx, dst)
}

var _ Tx = &VaultTx{}

type VaultTx struct {
	vol    *Vault
	inner  Tx
	crypto *bccrypto.Worker
	tries  *tries.Machine

	root  []byte
	blobs map[blobcache.CID]bccrypto.Ref
}

func newVaultTx(vol *Vault, inner Tx) *VaultTx {
	trieOp := tries.NewMachine()
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
	if v.root != nil {
		*dst = append((*dst)[:0], v.root...)
		return nil
	}

	var innerRoot []byte
	if err := v.inner.Load(ctx, &innerRoot); err != nil {
		return err
	}
	var trieRoot tries.Root
	if err := json.Unmarshal(innerRoot, &trieRoot); err != nil {
		return err
	}
	rootVal, err := v.tries.Get(ctx, UnsaltedStore{v.inner}, trieRoot, nil)
	if err != nil {
		return err
	}
	v.root = rootVal
	*dst = append((*dst)[:0], rootVal...)
	return nil
}

func (v *VaultTx) Save(ctx context.Context, src []byte) error {
	v.root = append(v.root[:0], src...)
	return nil
}

func (v *VaultTx) Commit(ctx context.Context) error {
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
	if err := v.inner.Save(ctx, root2); err != nil {
		return err
	}
	return v.inner.Commit(ctx)
}

func (v *VaultTx) Abort(ctx context.Context) error {
	return v.inner.Abort(ctx)
}

func (v *VaultTx) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	ref, err := v.crypto.Post(ctx, UnsaltedStore{v.inner}, data)
	if err != nil {
		return blobcache.CID{}, err
	}
	ptextCID := v.inner.Hash(opts.Salt, data)
	v.blobs[ptextCID] = ref
	return ptextCID, nil
}

func (v *VaultTx) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
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

func (v *VaultTx) Delete(ctx context.Context, cids []blobcache.CID) error {
	var innerCIDs []blobcache.CID
	for _, cid := range cids {
		if ref, exists := v.blobs[cid]; exists {
			innerCIDs = append(innerCIDs, ref.CID)
		}
	}
	return v.inner.Delete(ctx, innerCIDs)
}

func (v *VaultTx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	for i := range cids {
		if ref, exists := v.blobs[cids[i]]; exists {
			dst[i] = !ref.IsZero()
		}
	}
	return nil
}

func (v *VaultTx) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	// TODO: these CIDs only exist in the plaintext space, we need to transform them to the ciphertext space.
	// and then mark those as visited.
	// Additionally, and blobs that we access along the path from the root to the key-value entry
	// must also be marked as visited.
	panic("not implemented")
}

func (v *VaultTx) Visit(ctx context.Context, cids []blobcache.CID) error {
	panic("not implemented")
}

func (v *VaultTx) MaxSize() int {
	return v.inner.MaxSize()
}

func (v *VaultTx) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.inner.Hash(salt, data)
}

func (v *VaultTx) Link(ctx context.Context, subvol blobcache.OID, mask blobcache.ActionSet) error {
	return fmt.Errorf("vault: Link not implemented")
}

func (v *VaultTx) Unlink(ctx context.Context, targets []blobcache.OID) error {
	return fmt.Errorf("vault: Unlink not implemented")
}

func (v *VaultTx) VisitLinks(ctx context.Context, targets []blobcache.OID) error {
	return fmt.Errorf("vault: VisitLinks not implemented")
}
