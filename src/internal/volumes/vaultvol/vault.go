package vaultvol

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bccrypto"
	"blobcache.io/blobcache/src/internal/tries"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
	"golang.org/x/crypto/chacha20poly1305"
)

var _ volumes.Volume = &Vault{}

type Vault struct {
	inner volumes.Volume
	hf    blobcache.HashFunc

	aead  cipher.AEAD
	tmach *tries.Machine
	cmach *bccrypto.Machine
}

func New(inner volumes.Volume, secret [32]byte, hf blobcache.HashFunc) *Vault {
	dataSecret := bccrypto.DeriveKey(hf, &secret, []byte("blobcache/vault/data"))
	trieSecret := bccrypto.DeriveKey(hf, &secret, []byte("blobcache/vault/trie"))
	aeadSecret := bccrypto.DeriveKey(hf, &secret, []byte("blobcache/vault/cell"))
	aead, err := chacha20poly1305.NewX(aeadSecret[:])
	if err != nil {
		panic(err)
	}
	return &Vault{
		inner: inner,
		hf:    hf,

		aead:  aead,
		tmach: tries.NewMachine((*blobcache.CID)(&trieSecret), hf),
		cmach: bccrypto.NewMachine((*blobcache.CID)(&dataSecret), hf),
	}
}

func (v *Vault) Inner() volumes.Volume {
	return v.inner
}

func (v *Vault) BeginTx(ctx context.Context, params blobcache.TxParams) (volumes.Tx, error) {
	inner, err := v.inner.BeginTx(ctx, params)
	if err != nil {
		return nil, err
	}
	return newVaultTx(v, inner, params.GC), nil
}

func (v *Vault) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return fmt.Errorf("Await not implemented")
	// TODO: aead seal/open the next
}

func (v *Vault) aeadSeal(out []byte, ptext []byte) []byte {
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		panic(err)
	}
	out = append(out, nonce[:]...)
	return v.aead.Seal(out, nonce[:], ptext, nil)
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
	return v.aead.Open(out, nonce[:], ctext, nil)
}

var _ volumes.Tx = &VaultTx{}

type VaultTx struct {
	vol   *Vault
	inner volumes.Tx
	isGC  bool

	mu     sync.RWMutex
	isDone bool

	cellMu sync.Mutex
	cell   []byte

	// trieMu protects the ttx and newTx fields.
	trieMu sync.RWMutex
	// ttx is a transaction based on the contents of the cell.
	ttx *tries.Tx
	// newTx is only set for GC transactions.
	// it is based off of an empty trie.
	newTx *tries.Tx
}

func newVaultTx(vol *Vault, inner volumes.Tx, isGC bool) *VaultTx {
	return &VaultTx{
		vol:   vol,
		inner: inner,
		isGC:  isGC,
	}
}

func (tx *VaultTx) Volume() volumes.Volume {
	return tx.vol
}

func (v *VaultTx) Load(ctx context.Context, dst *[]byte) error {
	if err := v.setup(ctx); err != nil {
		return err
	}
	v.cellMu.Lock()
	defer v.cellMu.Unlock()
	v.trieMu.RLock()
	defer v.trieMu.RUnlock()
	s := volumes.NewUnsaltedStore(v)

	if v.cell == nil {
		// if the cell is not set, load it from the trie
		if err := loadCell(ctx, v.ttx, v.vol.cmach, s, &v.cell); err != nil {
			return err
		}
	}
	*dst = append((*dst)[:0], v.cell...)
	return nil
}

func (v *VaultTx) Save(ctx context.Context, src []byte) error {
	if err := v.setup(ctx); err != nil {
		return err
	}
	v.cellMu.Lock()
	defer v.cellMu.Unlock()
	v.cell = append(v.cell[:0], src...)
	return nil
}

func (v *VaultTx) Commit(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.ttx == nil {
		return nil
	}
	if v.isDone {
		return blobcache.ErrTxDone{}
	}

	v.cellMu.Lock()
	defer v.cellMu.Unlock()
	v.trieMu.Lock()
	defer v.trieMu.Unlock()
	s := volumes.NewUnsaltedStore(v.inner)

	var nextCell []byte
	if v.cell != nil {
		nextCell = v.cell
	} else {
		// copy over the cell data
		if err := loadCell(ctx, v.ttx, v.vol.cmach, s, &nextCell); err != nil {
			return err
		}
	}

	var root *tries.Root
	if v.isGC {
		cellRef, err := saveCell(ctx, v.newTx, v.vol.cmach, s, nextCell)
		if err != nil {
			return err
		}
		if err := v.inner.Visit(ctx, []blobcache.CID{cellRef.CID}); err != nil {
			return err
		}
		r, err := v.newTx.Flush(ctx, s)
		if err != nil {
			return err
		}
		root = r

		// we need to go through the trie nodes, and make sure they are all visited.
		walker := tries.Walker{
			ShouldWalk: func(_ tries.Root) bool {
				return true
			},
			NodeFn: func(x tries.Root) error {
				return v.inner.Visit(ctx, []blobcache.CID{x.Ref.CID})
			},
		}
		if err := v.vol.tmach.Walk(ctx, s, *root, walker); err != nil {
			return err
		}
	} else {
		_, err := saveCell(ctx, v.ttx, v.vol.cmach, s, nextCell)
		if err != nil {
			return err
		}
		r, err := v.ttx.Flush(ctx, s)
		if err != nil {
			return err
		}
		root = r
	}
	rootCtext := v.vol.aeadSeal(nil, root.Marshal())
	if err := v.inner.Save(ctx, rootCtext); err != nil {
		return err
	}
	if err := v.inner.Commit(ctx); err != nil {
		return err
	}
	v.isDone = true
	return nil
}

func (v *VaultTx) Abort(ctx context.Context) error {
	return v.inner.Abort(ctx)
}

func (v *VaultTx) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	s := volumes.NewUnsaltedStore(v.inner)
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
	s := volumes.NewUnsaltedStore(v.inner)
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

func (v *VaultTx) Visit(ctx context.Context, ptcids []blobcache.CID) error {
	if !v.isGC {
		return fmt.Errorf("Visit not allowed on non-GC transactions")
	}
	v.trieMu.Lock()
	defer v.trieMu.Unlock()
	s := volumes.NewUnsaltedStore(v.inner)

	var ctcids []blobcache.CID
	for _, ptcid := range ptcids {
		ref, err := getRef(ctx, v.ttx, s, ptcid)
		if err != nil {
			return err
		}
		if err := putRef(ctx, v.newTx, s, ptcid, ref); err != nil {
			return err
		}
		ctcids = append(ctcids, ref.CID)
	}
	return v.inner.Visit(ctx, ctcids)
}

func (v *VaultTx) MaxSize() int {
	return v.inner.MaxSize()
}

func (v *VaultTx) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.inner.Hash(salt, data)
}

func (v *VaultTx) AllowLink(ctx context.Context, subvol blobcache.Handle) error {
	return fmt.Errorf("AllowLink not allowed on Vault Volumes")
}

func (vtx *VaultTx) setup(ctx context.Context) error {
	vtx.mu.RLock()
	ttx := vtx.ttx
	vtx.mu.RUnlock()
	if vtx.isDone {
		return blobcache.ErrTxDone{}
	}
	if ttx != nil {
		return nil
	}

	vtx.mu.Lock()
	defer vtx.mu.Unlock()
	vtx.trieMu.Lock()
	defer vtx.trieMu.Unlock()
	if vtx.isDone {
		return blobcache.ErrTxDone{}
	}
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
	ttx = vtx.vol.tmach.NewTx(*troot)

	if vtx.isGC {
		vtx.newTx = vtx.vol.tmach.NewTxOnEmpty()
	}
	vtx.ttx = ttx
	return nil
}

// getRef returns the crypto ref for the plaintext CID
func (v *VaultTx) getRef(ctx context.Context, ptcid blobcache.CID) (bccrypto.Ref, error) {
	if err := v.setup(ctx); err != nil {
		return bccrypto.Ref{}, err
	}
	v.trieMu.RLock()
	defer v.trieMu.RUnlock()
	s := volumes.NewUnsaltedStore(v.inner)
	return getRef(ctx, v.ttx, s, ptcid)
}

func (v *VaultTx) putRef(ctx context.Context, ptcid blobcache.CID, ref bccrypto.Ref) error {
	if err := v.setup(ctx); err != nil {
		return err
	}
	v.trieMu.Lock()
	defer v.trieMu.Unlock()
	s := volumes.NewUnsaltedStore(v.inner)
	return putRef(ctx, v.ttx, s, ptcid, ref)
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

func saveCell(ctx context.Context, tx *tries.Tx, cm *bccrypto.Machine, s schema.RW, data []byte) (bccrypto.Ref, error) {
	ref, err := cm.Post(ctx, s, data)
	if err != nil {
		return bccrypto.Ref{}, err
	}
	if err := tx.Put(ctx, s, []byte{}, ref.Marshal(nil)); err != nil {
		return bccrypto.Ref{}, err
	}
	return ref, nil
}

func loadCell(ctx context.Context, tx *tries.Tx, cm *bccrypto.Machine, s schema.RO, dst *[]byte) error {
	var refData []byte
	if err := tx.Get(ctx, s, []byte{}, &refData); err != nil {
		if tries.IsErrNotFound(err) {
			*dst = (*dst)[:0]
			return nil
		}
		return err
	}
	var ref bccrypto.Ref
	if err := ref.Unmarshal(refData); err != nil {
		return err
	}
	return cm.GetF(ctx, s, ref, func(data []byte) error {
		*dst = append((*dst)[:0], data...)
		return nil
	})
}

func getRef(ctx context.Context, ttx *tries.Tx, s schema.RO, ptcid blobcache.CID) (bccrypto.Ref, error) {
	var crefData []byte
	if err := ttx.Get(ctx, s, ptcid[:], &crefData); err != nil {
		return bccrypto.Ref{}, err
	}
	var ref bccrypto.Ref
	if err := ref.Unmarshal(crefData); err != nil {
		return ref, err
	}
	return ref, nil
}

func putRef(ctx context.Context, ttx *tries.Tx, s schema.RW, ptcid blobcache.CID, ref bccrypto.Ref) error {
	return ttx.Put(ctx, s, ptcid[:], ref.Marshal(nil))
}
