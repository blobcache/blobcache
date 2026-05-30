package memory

import (
	"context"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
)

type Volume struct {
	cfg      blobcache.VolumeConfig
	maxBytes uint64

	// txMu is held by all active transactions
	txMu sync.RWMutex

	// stateMu is held in
	// - write mode when a transaction commits
	// - read mode whenever these fields are read
	stateMu sync.RWMutex
	cell    []byte
	blobs   map[blobcache.CID][]byte
	links   map[blobcache.LinkTokenID]blobcache.LinkToken
}

func NewVolume(maxBytes uint64) *Volume {
	return &Volume{maxBytes: maxBytes}
}

var _ backend.Volume = (*Volume)(nil)
var _ backend.Tx = (*Tx)(nil)

func (vol *Volume) GetParams() blobcache.VolumeConfig {
	return vol.cfg
}

func (vol *Volume) GetBackend() blobcache.VolumeBackend[blobcache.OID] {
	return blobcache.VolumeBackend[blobcache.OID]{
		Local: blobcache.VolumeBackend_LocalFromConfig(vol.cfg),
	}
}

func (vol *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (backend.Tx, error) {
	_ = ctx
	if spec.Modify {
		vol.txMu.Lock()
	} else {
		vol.txMu.RLock()
	}
	tx := &Tx{
		p:    spec,
		vol:  vol,
		cell: append([]byte(nil), vol.cell...),
	}
	return tx, nil
}

func (vol *Volume) VolumeDown(ctx context.Context) error {
	_ = ctx
	return nil
}

func (vol *Volume) AccessSubVolume(ctx context.Context, target blobcache.LinkToken) (blobcache.ActionSet, error) {
	_ = ctx
	vol.stateMu.RLock()
	defer vol.stateMu.RUnlock()
	if ltok, exists := vol.links[target.GetID(vol.cfg.HashAlgo)]; exists && ltok == target {
		return target.Rights, nil
	}
	return 0, nil
}

func (vol *Volume) Load(ctx context.Context, dst *[]byte) error {
	vol.stateMu.RLock()
	defer vol.stateMu.RUnlock()
	*dst = append((*dst)[:0], vol.cell...)
	return nil
}

type Tx struct {
	p   blobcache.TxParams
	vol *Volume

	doneMu sync.RWMutex
	isDone bool
	cellMu sync.Mutex
	cell   []byte
	blobMu sync.RWMutex
	blobs  map[blobcache.CID][]byte
	linkMu sync.RWMutex
	links  map[blobcache.LinkTokenID]blobcache.LinkToken
}

func (tx *Tx) setDone() {
	if !tx.isDone {
		if tx.p.Modify {
			tx.vol.txMu.Unlock()
		} else {
			tx.vol.txMu.RUnlock()
		}
	}
	tx.isDone = true
}

func (tx *Tx) Abort(ctx context.Context) error {
	tx.doneMu.Lock()
	defer tx.doneMu.Unlock()
	tx.setDone()
	return nil
}

func (tx *Tx) Commit(ctx context.Context) error {
	tx.doneMu.Lock()
	defer tx.doneMu.Unlock()
	if tx.isDone {
		return blobcache.ErrTxDone{}
	}

	if tx.p.Modify {
		tx.vol.cell = append(tx.vol.cell[:0], tx.cell...)
		if tx.vol.blobs == nil && len(tx.blobs) > 0 {
			tx.vol.blobs = make(map[blobcache.CID][]byte)
		}
		for cid, data := range tx.blobs {
			if data != nil {
				tx.vol.blobs[cid] = append([]byte(nil), data...)
			} else {
				delete(tx.vol.blobs, cid)
			}
		}
		if tx.vol.links == nil && len(tx.links) > 0 {
			tx.vol.links = make(map[blobcache.LinkTokenID]blobcache.LinkToken)
		}
		for ltid, ltok := range tx.links {
			if ltok.Target == (blobcache.OID{}) && ltok.Rights == 0 {
				delete(tx.vol.links, ltid)
				continue
			}
			tx.vol.links[ltid] = ltok
		}
	}
	tx.setDone()
	return nil
}

func (tx *Tx) Load(ctx context.Context, dst *[]byte) error {
	_ = ctx
	tx.doneMu.RLock()
	defer tx.doneMu.RUnlock()
	tx.cellMu.Lock()
	defer tx.cellMu.Unlock()
	if tx.isDone {
		return blobcache.ErrTxDone{}
	}
	*dst = append((*dst)[:0], tx.cell...)
	return nil
}

func (tx *Tx) Params() blobcache.TxParams {
	return tx.p
}

func (tx *Tx) Save(ctx context.Context, src []byte) error {
	_ = ctx
	tx.doneMu.Lock()
	defer tx.doneMu.Unlock()
	if tx.isDone {
		return blobcache.ErrTxDone{}
	}
	tx.cell = append(tx.cell[:0], src...)
	return nil
}

func (tx *Tx) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	_ = ctx
	tx.doneMu.Lock()
	defer tx.doneMu.Unlock()
	if tx.isDone {
		return blobcache.CID{}, blobcache.ErrTxDone{}
	}
	if uint64(len(data)) > tx.vol.maxBytes {
		return blobcache.CID{}, blobcache.ErrTooLarge{BlobSize: len(data), MaxSize: int(tx.vol.maxBytes)}
	}
	ha := tx.HashAlgo()
	var cid blobcache.CID
	if opts.Salt != nil {
		cid = ha.KeyedHash(opts.Salt, data)
	} else {
		cid = ha.Hash(data)
	}
	tx.blobMu.Lock()
	defer tx.blobMu.Unlock()
	if tx.blobs == nil {
		tx.blobs = make(map[blobcache.CID][]byte)
	}
	tx.blobs[cid] = append([]byte(nil), data...)
	return cid, nil
}

func (tx *Tx) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	_ = ctx
	_ = opts
	tx.doneMu.RLock()
	defer tx.doneMu.RUnlock()
	if tx.isDone {
		return 0, blobcache.ErrTxDone{}
	}
	tx.blobMu.RLock()
	data, exists := tx.blobs[cid]
	tx.blobMu.RUnlock()
	if exists {
		if data == nil {
			return 0, blobcache.ErrNotFound{CID: cid}
		}
		return copyBlob(buf, cid, data)
	}
	tx.vol.stateMu.RLock()
	defer tx.vol.stateMu.RUnlock()
	data, exists = tx.vol.blobs[cid]
	if !exists {
		return 0, blobcache.ErrNotFound{CID: cid}
	}
	return copyBlob(buf, cid, data)
}

func (tx *Tx) Delete(ctx context.Context, cids []blobcache.CID) error {
	_ = ctx
	tx.doneMu.Lock()
	defer tx.doneMu.Unlock()
	if tx.isDone {
		return blobcache.ErrTxDone{}
	}
	tx.blobMu.Lock()
	defer tx.blobMu.Unlock()
	if tx.blobs == nil {
		tx.blobs = make(map[blobcache.CID][]byte)
	}
	for _, cid := range cids {
		tx.blobs[cid] = nil
	}
	return nil
}

func (tx *Tx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	_ = ctx
	tx.doneMu.RLock()
	defer tx.doneMu.RUnlock()
	if tx.isDone {
		return blobcache.ErrTxDone{}
	}
	tx.blobMu.RLock()
	defer tx.blobMu.RUnlock()
	tx.vol.stateMu.RLock()
	defer tx.vol.stateMu.RUnlock()
	for i, cid := range cids {
		data, shadowed := tx.blobs[cid]
		_, base := tx.vol.blobs[cid]
		dst[i] = (shadowed && data != nil) || (!shadowed && base)
	}
	return nil
}

func (tx *Tx) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	_ = ctx
	for i := range cids {
		dst[i] = false
	}
	return nil
}

func (tx *Tx) Visit(ctx context.Context, cids []blobcache.CID) error {
	_ = ctx
	_ = cids
	return nil
}

func (tx *Tx) MaxSize() int {
	return int(tx.vol.maxBytes)
}

func (tx *Tx) HashAlgo() blobcache.HashAlgo {
	if tx.vol.cfg.HashAlgo == "" {
		return blobcache.HashAlgo_BLAKE3_256
	}
	return tx.vol.cfg.HashAlgo
}

func (tx *Tx) Link(ctx context.Context, svoid blobcache.OID, rights blobcache.ActionSet, subvol backend.Volume) (*blobcache.LinkToken, error) {
	_ = ctx
	_ = subvol
	tx.doneMu.Lock()
	defer tx.doneMu.Unlock()
	if tx.isDone {
		return nil, blobcache.ErrTxDone{}
	}
	ltok := &blobcache.LinkToken{Target: svoid, Rights: rights}
	ltid := ltok.GetID(tx.HashAlgo())
	if tx.links == nil {
		tx.links = make(map[blobcache.LinkTokenID]blobcache.LinkToken)
	}
	tx.links[ltid] = *ltok
	return ltok, nil
}

func (tx *Tx) Unlink(ctx context.Context, targets []blobcache.LinkTokenID) error {
	_ = ctx
	tx.doneMu.Lock()
	defer tx.doneMu.Unlock()
	if tx.isDone {
		return blobcache.ErrTxDone{}
	}
	if tx.links == nil {
		tx.links = make(map[blobcache.LinkTokenID]blobcache.LinkToken)
	}
	for _, target := range targets {
		tx.links[target] = blobcache.LinkToken{}
	}
	return nil
}

func (tx *Tx) VisitLinks(ctx context.Context, targets []blobcache.LinkTokenID) error {
	_ = ctx
	_ = targets
	return nil
}

func copyBlob(buf []byte, cid blobcache.CID, data []byte) (int, error) {
	if len(buf) < len(data) {
		return 0, blobcache.ErrTooSmall{BlobSize: int32(len(data)), BufferSize: int32(len(buf))}
	}
	n := copy(buf, data)
	if n != len(data) {
		return 0, fmt.Errorf("copy blob %v: short copy", cid)
	}
	return n, nil
}
