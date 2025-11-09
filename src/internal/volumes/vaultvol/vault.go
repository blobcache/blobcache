package vaultvol

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bccrypto"
	"blobcache.io/blobcache/src/internal/tries"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
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
	return newVaultTx(v, inner, params), nil
}

func (v *Vault) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return fmt.Errorf("Await not implemented")
	// TODO: aead seal/open the next
}

func (v *Vault) AccessSubVolume(ctx context.Context, target blobcache.OID) (blobcache.ActionSet, error) {
	return v.inner.AccessSubVolume(ctx, target)
}

func (v *Vault) ReadLinks(ctx context.Context, dst volumes.LinkSet) error {
	lr, ok := v.inner.(volumes.LinkReader)
	if !ok {
		return fmt.Errorf("inner volume does not support LinkReader")
	}
	return lr.ReadLinks(ctx, dst)
}

func (v *Vault) GetBackend() blobcache.VolumeBackend[blobcache.OID] {
	return v.inner.GetBackend()
}

func (v *Vault) GetParams() blobcache.VolumeConfig {
	return v.inner.GetParams()
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

var _ volumes.Tx = &Tx{}

type Tx struct {
	vol   *Vault
	inner volumes.Tx
	txp   blobcache.TxParams

	mu     sync.RWMutex
	isDone bool

	// cellMu protects the cell field.
	cellMu sync.Mutex
	// cell holds the plaintext cell data.
	cell []byte

	// trieMu protects the ttx and newTx fields.
	trieMu sync.RWMutex
	// ttx is a transaction based on the contents of the cell.
	ttx *tries.Tx
	// newTx is only set for GC transactions.
	// it is based off of an empty trie.
	newTx *tries.Tx
}

func newVaultTx(vol *Vault, inner volumes.Tx, txp blobcache.TxParams) *Tx {
	return &Tx{
		vol:   vol,
		inner: inner,
		txp:   txp,
	}
}

func (tx *Tx) Volume() volumes.Volume {
	return tx.vol
}

func (v *Tx) Load(ctx context.Context, dst *[]byte) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()

	v.cellMu.Lock()
	defer v.cellMu.Unlock()
	v.trieMu.RLock()
	defer v.trieMu.RUnlock()
	s := volumes.NewUnsaltedStore(v.inner)

	if v.cell == nil {
		// if the cell is not set, load it from the trie
		if err := loadCell(ctx, v.ttx, v.vol.cmach, s, &v.cell); err != nil {
			return err
		}
	}
	*dst = append((*dst)[:0], v.cell...)
	return nil
}

func (v *Tx) Save(ctx context.Context, src []byte) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()

	v.cellMu.Lock()
	defer v.cellMu.Unlock()
	v.cell = append(v.cell[:0], src...)
	return nil
}

func (v *Tx) Commit(ctx context.Context) error {
	// don't call beginOp here, we want to get a write lock.
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.txp.Mutate {
		return blobcache.ErrTxReadOnly{Op: "Commit"}
	}
	if v.isDone {
		return blobcache.ErrTxDone{}
	}

	v.cellMu.Lock()
	defer v.cellMu.Unlock()
	v.trieMu.Lock()
	defer v.trieMu.Unlock()
	if v.ttx == nil {
		return nil
	}
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
	if v.txp.GC {
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
	rootCtext := v.vol.aeadSeal(nil, root.Marshal(nil))
	if err := v.inner.Save(ctx, rootCtext); err != nil {
		return err
	}
	if err := v.inner.Commit(ctx); err != nil {
		return err
	}
	v.isDone = true
	return nil
}

func (v *Tx) Abort(ctx context.Context) error {
	return v.inner.Abort(ctx)
}

func (v *Tx) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	release, err := v.beginOp(ctx)
	if err != nil {
		return blobcache.CID{}, err
	}
	defer release()

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

func (v *Tx) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	release, err := v.beginOp(ctx)
	if err != nil {
		return 0, err
	}
	defer release()

	ref, err := v.getRef(ctx, cid)
	if err != nil {
		return 0, err
	}
	s := volumes.NewUnsaltedStore(v.inner)
	// get and decrypt the ciphertext blob
	return v.vol.cmach.Get(ctx, s, ref, buf)
}

func (v *Tx) Delete(ctx context.Context, cids []blobcache.CID) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()

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

func (v *Tx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	ctcids, err := v.translateCIDs(ctx, cids)
	if err != nil {
		return err
	}
	return v.inner.Exists(ctx, ctcids, dst)
}

func (v *Tx) IsVisited(ctx context.Context, ptcids []blobcache.CID, dst []bool) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	ctcids, err := v.translateCIDs(ctx, ptcids)
	if err != nil {
		return err
	}
	return v.inner.IsVisited(ctx, ctcids, dst)
}

func (v *Tx) Visit(ctx context.Context, ptcids []blobcache.CID) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()

	if !v.txp.GC {
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

func (v *Tx) MaxSize() int {
	return v.inner.MaxSize()
}

func (v *Tx) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.inner.Hash(salt, data)
}

func (v *Tx) Link(ctx context.Context, subvol blobcache.OID, rights blobcache.ActionSet, targetVol volumes.Volume) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	return v.inner.Link(ctx, subvol, rights, targetVol)
}

func (v *Tx) Unlink(ctx context.Context, targets []blobcache.OID) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	return v.inner.Unlink(ctx, targets)
}

func (v *Tx) VisitLinks(ctx context.Context, targets []blobcache.OID) error {
	release, err := v.beginOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	return v.inner.VisitLinks(ctx, targets)
}

// getRef returns the crypto ref for the plaintext CID
func (v *Tx) getRef(ctx context.Context, ptcid blobcache.CID) (bccrypto.Ref, error) {
	v.trieMu.RLock()
	defer v.trieMu.RUnlock()
	s := volumes.NewUnsaltedStore(v.inner)
	return getRef(ctx, v.ttx, s, ptcid)
}

func (vtx *Tx) putRef(ctx context.Context, ptcid blobcache.CID, ref bccrypto.Ref) error {
	vtx.trieMu.Lock()
	defer vtx.trieMu.Unlock()
	s := volumes.NewUnsaltedStore(vtx.inner)
	return putRef(ctx, vtx.ttx, s, ptcid, ref)
}

// translateCIDs resolves plaintext CIDs into ciphertext CIDs
func (v *Tx) translateCIDs(ctx context.Context, ptcids []blobcache.CID) ([]blobcache.CID, error) {
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

// init is called to initialize the trie and cell.
// It requires exclusive locks on mu and trieMu.
func (vtx *Tx) init(ctx context.Context) error {
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
	var s schema.WO = volumes.NewUnsaltedStore(vtx.inner)
	var ttx *tries.Tx
	if len(rootCtext) == 0 {
		var err error
		if !vtx.txp.Mutate {
			s = schema.NewMem(vtx.inner.Hash, vtx.inner.MaxSize())
		}
		ttx, err = vtx.vol.tmach.NewTxOnEmpty(ctx, s)
		if err != nil {
			return err
		}
	} else {
		rootPtext, err := vtx.vol.aeadOpen(nil, rootCtext)
		if err != nil {
			return err
		}
		troot, err := tries.ParseRoot(rootPtext)
		if err != nil {
			return err
		}
		ttx = vtx.vol.tmach.NewTx(*troot)
	}

	if vtx.txp.GC {
		newTx, err := vtx.vol.tmach.NewTxOnEmpty(ctx, s)
		if err != nil {
			return err
		}
		vtx.newTx = newTx
	}
	vtx.ttx = ttx
	return nil
}

// beginOp should be called at the start of every operation.
// If an error is returned, it should be returned immediately.
// If err is nil, then the caller is responsible for calling release
// at the end of the operation.
func (vtx *Tx) beginOp(ctx context.Context) (release func(), _ error) {
	vtx.mu.RLock()
	ttx := vtx.ttx
	if ttx != nil {
		// if there is already a transaction, then setup has been run.
		return vtx.mu.RUnlock, nil
	}
	if vtx.isDone {
		vtx.mu.RUnlock()
		return nil, blobcache.ErrTxDone{}
	}
	vtx.mu.RUnlock()

	if err := vtx.init(ctx); err != nil {
		return nil, err
	}
	// re-acquire the read lock
	vtx.mu.RLock()
	return vtx.mu.RUnlock, nil
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
