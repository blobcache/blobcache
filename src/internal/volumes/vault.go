package volumes

import (
	"context"
	"encoding/json"
	"fmt"
	"crypto/rand"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bccrypto"
	"blobcache.io/blobcache/src/internal/tries"
	"go.brendoncarroll.net/state"
	"golang.org/x/crypto/chacha20poly1305"
)

var _ Volume = &Vault{}

type Vault struct {
	Inner Volume

	secret [32]byte
	tmach  *tries.Machine
	cmach  *bccrypto.Machine
}

func NewVault(inner Volume, secret [32]byte) *Vault {
	return &Vault{
		Inner:  inner,
		secret: secret,
		tmach:  tries.NewMachine(),
	}
}

func (v *Vault) BeginTx(ctx context.Context, params blobcache.TxParams) (Tx, error) {
	inner, err := v.Inner.BeginTx(ctx, params)
	if err != nil {
		return nil, err
	}
	return newVaultTx(v, inner, params.GC), nil
}

func (v *Vault) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return v.Inner.Await(ctx, prev, next)
}

<<<<<<< HEAD
func (v *Vault) ReadLinks(ctx context.Context, dst LinkSet) error {
	return v.Inner.ReadLinks(ctx, dst)
=======
func (v *Vault) aeadSeal(out []byte, ptext []byte) []byte {
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		panic(err)
	}
	out = append(out, nonce[:]...)
	aead, err := chacha20poly1305.NewX(v.secret[:])
	if err != nil {
		panic(err)
	}
	return aead.Seal(out, nonce[:], ptext, nil)
}

func (v *Vault) aeadOpen(out []byte, ctext []byte) ([]byte, error) {
	if len(ctext) == 0 {
		return []byte{}, nil
	}
	var nonce [24]byte
	if len(ctext) < len(nonce) {
		return nil, fmt.Errorf("too small to contain nonce")
	}
	copy(nonce[:], ctext[0:len(nonce)])
	ctext = ctext[len(nonce):]
	aead, err := chacha20poly1305.NewX(v.secret[:])
	if err != nil {
		return nil, err
	}
	return aead.Open(out, nonce[:], ctext, nil)
>>>>>>> 24d5630 (Vault volume backend WIP)
}

var _ Tx = &VaultTx{}

type VaultTx struct {
	vol   *Vault
	inner Tx
	isGC  bool

	mu  sync.RWMutex
	ttx *tries.Tx
	// newTx is only set for GC transactions
	newTx *tries.Tx
}

func newVaultTx(vol *Vault, inner Tx, isGC bool) *VaultTx {
	return &VaultTx{
		vol:   vol,
		inner: inner,
		isGC:  isGC,
	}
}

func (tx *VaultTx) Volume() Volume {
	return tx.vol
}

func (v *VaultTx) Load(ctx context.Context, dst *[]byte) error {
	if err := v.setup(ctx); err != nil {
		return err
	}
	s := NewUnsaltedStore(v)
	return v.ttx.Get(ctx, s, []byte{}, dst)
}

func (v *VaultTx) Save(ctx context.Context, src []byte) error {
	if err := v.setup(ctx); err != nil {
		return err
	}
	return v.ttx.Put(ctx, []byte{}, src)
}

func (v *VaultTx) Commit(ctx context.Context) error {
	if v.ttx == nil {
		return nil
	}
	s := NewUnsaltedStore(v.inner)

	if v.isGC {
		// if this is a GC transaction, then we need to go through and mark everything not visited as deleted.
	}
	root, err := v.ttx.Finish(ctx, s)
	if err != nil {
		return err
	}
	it := v.vol.tmach.NewIterator(s, *root, state.ByteSpan{})
	if v.isGC {
		walker := tries.Walker{
			ShouldWalk: func(root tries.Root) bool {
				var visited [1]bool
				if err := v.inner.IsVisited(ctx, []blobcache.CID{root.Ref.ID}, visited[:]); err != nil {
					return true // if this fails, then we do want to traverse it.
				}
				// otherwise traverse if it's not visited.
				return !visited[0]
			},
			EntryFn: func(ent *tries.Entry) error {
				return nil
			},
			NodeFn: func(tr tries.Root) error {
				return v.inner.Visit(ctx, []blobcache.CID{tr.Ref.ID})
			},
		}
		if err := v.vol.tmach.Walk(ctx, s, *root, walker); err != nil {
			return err
		}
	}
	rootCtext := v.vol.aeadSeal(nil, root.Marshal())
	if err := v.inner.Save(ctx, rootCtext); err != nil {
		return err
	}
	return v.inner.Commit(ctx)
}

func (v *VaultTx) Abort(ctx context.Context) error {
	return v.inner.Abort(ctx)
}

func (v *VaultTx) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	s := NewUnsaltedStore(v.inner)
	cid := v.inner.Hash(nil, data)
	ref, err := v.vol.cmach.Post(ctx, s, data)
	if err != nil {
		return blobcache.CID{}, err
	}
	if err := v.putRef(ctx, cid, ref); err != nil {
		return blobcache.CID{}, err
	}
	return cid, nil
}

func (v *VaultTx) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	if err := v.setup(ctx); err != nil {
		return 0, err
	}
	ref, err := v.getRef(ctx, cid)
	if err != nil {
		return 0, err
	}
	s := NewUnsaltedStore(v.inner)
	// get and decrypt the ciphertext blob
	return v.vol.cmach.Get(ctx, s, ref, buf)
}

func (v *VaultTx) Delete(ctx context.Context, cids []blobcache.CID) error {
	if err := v.setup(ctx); err != nil {
		return err
	}
	for _, cid := range cids {
		ref, err := v.getRef(ctx, cid)
		if err != nil {
			return err
		}
		if err := v.inner.Delete(ctx, []blobcache.CID{ref.CID}); err != nil {
			return err
		}
		if err := v.ttx.Delete(ctx, cid[:]); err != nil {
			return err
		}
	}
	return nil
}

func (v *VaultTx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	if err := v.setup(ctx); err != nil {
		return err
	}
	ctcids, err := v.translateCIDs(ctx, cids)
	if err != nil {
		return err
	}
	return v.inner.Exists(ctx, ctcids, dst)
}

func (v *VaultTx) IsVisited(ctx context.Context, ptcids []blobcache.CID, dst []bool) error {
	ctcids, err := v.translateCIDs(ctx, ptcids)
	if err != nil {
		return err
	}
	return v.inner.IsVisited(ctx, ctcids, dst)
}

func (v *VaultTx) Visit(ctx context.Context, cids []blobcache.CID) error {
	ctcids, err := v.translateCIDs(ctx, cids)
	if err != nil {
		return err
	}
	if err := v.inner.Visit(ctx, ctcids); err != nil {
		return err
	}
	return nil
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

func (vtx *VaultTx) setup(ctx context.Context) error {
	vtx.mu.RLock()
	ttx := vtx.ttx
	vtx.mu.RUnlock()
	if ttx != nil {
		return nil
	}

	vtx.mu.Lock()
	defer vtx.mu.Unlock()
	if vtx.ttx != nil {
		return nil
	}
	var rootCtext []byte
	if err := vtx.inner.Load(ctx, &rootCtext); err != nil {
		return err
	}
	rootPtext, err := vtx.vol.aeadOpen(nil, rootCtext)
	if err != nil {
		return err
	}
	troot, err := tries.ParseRoot(rootPtext)
	if err != nil {
		return err
	}
	vtx.ttx = vtx.vol.tmach.NewTx(*troot)
	return nil
}

// getRef returns the crypto ref for the plaintext CID
func (v *VaultTx) getRef(ctx context.Context, ptcid blobcache.CID) (bccrypto.Ref, error) {
	if err := v.setup(ctx); err != nil {
		return bccrypto.Ref{}, err
	}
	s := NewUnsaltedStore(v.inner)
	var crefData []byte
	if err := v.ttx.Get(ctx, s, ptcid[:], &crefData); err != nil {
		return bccrypto.Ref{}, err
	}
	var ref bccrypto.Ref
	if err := ref.Unmarshal(crefData); err != nil {
		return ref, err
	}
	return ref, nil
}

func (v *VaultTx) putRef(ctx context.Context, ptcid blobcache.CID, ref bccrypto.Ref) error {
	if err := v.setup(ctx); err != nil {
		return err
	}
	return v.ttx.Put(ctx, ptcid[:], ref.Marshal(nil))
}

// translateCIDs resolves plaintext CIDs into ciphertext CIDs
func (v *VaultTx) translateCIDs(ctx context.Context, ptcids []blobcache.CID) ([]blobcache.CID, error) {
	var ctcids []blobcache.CID
	for _, ptcid := range ptcids {
		ref, err := v.getRef(ctx, ptcid)
		if err != nil {
			if tries.IsErrNotFound(err) {
				ctcids = append(ctcids, blobcache.CID{})
				continue
			}
			return nil, err
		}
		ctcids = append(ctcids, ref.CID)
	}
	return ctcids, nil
}
