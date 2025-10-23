package localvol

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/state/cadata"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
)

var _ volumes.Volume = &Volume{}

type Volume struct {
	sys    *System
	lvid   ID
	params blobcache.VolumeParams
}

func newLocalVolume(sys *System, lvid ID, params blobcache.VolumeParams) *Volume {
	return &Volume{
		sys:    sys,
		lvid:   lvid,
		params: params,
	}
}

func (v *Volume) ID() ID {
	return v.lvid
}

func (v *Volume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return fmt.Errorf("Await not implemented")
}

func (v *Volume) BeginTx(ctx context.Context, tp blobcache.TxParams) (volumes.Tx, error) {
	return v.sys.beginTx(ctx, v, tp)
}

func (v *Volume) ReadLinks(ctx context.Context, dst volumes.LinkSet) error {
	return v.sys.readLinksFrom(v.lvid, dst)
}

var _ volumes.Tx = &localTxnMut{}

// localTxnMut is a mutating transaction on a local volume.
type localTxnMut struct {
	localSys *System
	vol      *Volume
	mvid     pdb.MVTag
	txParams blobcache.TxParams
	schema   schema.Schema

	hf blobcache.HashFunc

	// mu protects the finished and links fields.
	// mu must be taken exclusively to {Commit, Abort, AllowLink}
	// mu must be taken in read mode for all other operations {Save, Delete, Post, Get, Exists}.
	// checkFinished is the most convenient way to make sure the transaction is not finished, during an operation.
	mu sync.RWMutex
	// finished is set to true when the transaction is finished.
	finished     bool
	links        map[blobcache.OID]blobcache.ActionSet
	visitedLinks map[blobcache.OID]struct{}
}

// newLocalTxn creates a localTxn.
// It does not change the database state.
// the caller should have already created the transaction at txid, and volInfo.
func newLocalTxn(localSys *System, vol *Volume, mvid pdb.MVTag, txParams blobcache.TxParams, schema schema.Schema) (*localTxnMut, error) {
	links := make(map[blobcache.OID]blobcache.ActionSet)
	if err := ReadVolumeLinks(localSys.db, OIDFromLocalID(vol.lvid), links); err != nil {
		return nil, err
	}
	hf := vol.params.HashAlgo.HashFunc()
	return &localTxnMut{
		localSys: localSys,
		vol:      vol,
		mvid:     mvid,
		txParams: txParams,
		schema:   schema,
		hf:       hf,
		links:    links,
	}, nil
}

func (v *localTxnMut) Volume() volumes.Volume {
	return v.vol
}

func (v *localTxnMut) MaxSize() int {
	return int(v.vol.params.MaxSize)
}

func (v *localTxnMut) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.hf(salt, data)
}

func (v *localTxnMut) checkFinished() (func(), error) {
	v.mu.RLock()
	if v.finished {
		return nil, blobcache.ErrTxDone{}
	}
	return v.mu.RUnlock, nil
}

func (ltx *localTxnMut) Commit(ctx context.Context) error {
	ltx.mu.Lock()
	defer ltx.mu.Unlock()
	if ltx.finished {
		return blobcache.ErrTxDone{}
	}

	if ltx.txParams.GC {
		if err := ltx.localSys.gc(ctx, ltx.vol.lvid, ltx.mvid); err != nil {
			return err
		}
		// filter out the links that are not visited
		for oid := range ltx.links {
			if _, ok := ltx.visitedLinks[oid]; !ok {
				delete(ltx.links, oid)
			}
		}
	}
	if err := ltx.localSys.commit(ltx.vol.lvid, ltx.mvid, ltx.links); err != nil {
		return err
	}
	ltx.finished = true
	return nil
}

func (v *localTxnMut) Abort(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.finished {
		return nil
	}
	if err := v.localSys.abortMut(v.vol.lvid, v.mvid); err != nil {
		return err
	}
	v.finished = true
	return nil
}

func (v *localTxnMut) Save(ctx context.Context, root []byte) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	if len(root) > v.MaxSize()/2 {
		return fmt.Errorf("root cannot be more than half the max blob size. %d", len(root))
	}
	return lvSave(v.localSys.db, v.vol.lvid, v.mvid, root)
}

func (v *localTxnMut) Load(ctx context.Context, dst *[]byte) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return v.localSys.load(v.vol.lvid, v.mvid, dst)
}

func (v *localTxnMut) Delete(ctx context.Context, cids []blobcache.CID) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return v.localSys.deleteBlob(v.vol.lvid, v.mvid, cids)
}

func (v *localTxnMut) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	unlock, err := v.checkFinished()
	if err != nil {
		return blobcache.CID{}, err
	}
	defer unlock()
	if len(data) > int(v.vol.params.MaxSize) {
		return blobcache.CID{}, cadata.ErrTooLarge
	}
	salt := opts.Salt
	if salt != nil && !v.vol.params.Salted {
		return blobcache.CID{}, blobcache.ErrCannotSalt{}
	}
	cid := v.Hash(salt, data)
	if err := v.localSys.postBlob(ctx, v.vol.lvid, v.mvid, cid, salt, data); err != nil {
		return blobcache.CID{}, err
	}
	return cid, nil
}

func (v *localTxnMut) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	unlock, err := v.checkFinished()
	if err != nil {
		return 0, err
	}
	defer unlock()
	return v.localSys.getBlob(v.vol.lvid, v.mvid, cid, buf)
}

func (v *localTxnMut) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return v.localSys.blobExists(v.vol.lvid, v.mvid, cids, dst)
}

func (v *localTxnMut) Visit(ctx context.Context, cids []blobcache.CID) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	if !v.txParams.GC {
		return blobcache.ErrTxNotGC{Op: "Visit"}
	}
	return v.localSys.visit(v.vol.lvid, v.mvid, cids)
}

func (v *localTxnMut) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	if !v.txParams.GC {
		return blobcache.ErrTxNotGC{Op: "IsVisited"}
	}
	return v.localSys.isVisited(v.vol.lvid, v.mvid, cids, dst)
}

func (v *localTxnMut) Link(ctx context.Context, subvol blobcache.OID, rights blobcache.ActionSet) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.finished {
		return blobcache.ErrTxDone{}
	}
	// Link merges rights.  The original handle passed to the Service will have been resolved and had the rights applied.
	v.links[subvol] |= rights
	return nil
}

func (v *localTxnMut) Unlink(ctx context.Context, targets []blobcache.OID) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.finished {
		return blobcache.ErrTxDone{}
	}
	for _, oid := range targets {
		delete(v.links, oid)
	}
	return nil
}

func (txn *localTxnMut) VisitLinks(ctx context.Context, targets []blobcache.OID) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.finished {
		return blobcache.ErrTxDone{}
	}
	if txn.visitedLinks == nil {
		txn.visitedLinks = make(map[blobcache.OID]struct{})
	}
	for _, oid := range targets {
		txn.visitedLinks[oid] = struct{}{}
	}
	return nil
}

var _ volumes.Tx = &localTxnRO{}

// localTxnRO is a read-only transaction on a local volume.
type localTxnRO struct {
	sys *System
	vol *Volume
	sp  *pebble.Snapshot

	// activeTxns is the set of active transactions for the volume.
	mu         sync.RWMutex
	closed     bool
	activeTxns map[pdb.MVTag]struct{}
}

func newLocalTxnRO(sys *System, vol *Volume, sp *pebble.Snapshot) *localTxnRO {
	return &localTxnRO{
		sys: sys,
		vol: vol,
		sp:  sp,
	}
}

func (v *localTxnRO) checkClosed() (func(), error) {
	v.mu.RLock()
	if v.closed {
		return nil, blobcache.ErrTxDone{}
	}
	return v.mu.RUnlock, nil
}

func (v *localTxnRO) Abort(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	if err := v.sp.Close(); err != nil {
		return err
	}
	v.closed = true
	return nil
}

func (v *localTxnRO) Commit(ctx context.Context) error {
	return blobcache.ErrTxReadOnly{Op: "Commit"}
}

func (v *localTxnRO) Load(ctx context.Context, dst *[]byte) error {
	activeTxns, err := v.getExcluded()
	if err != nil {
		return err
	}
	unlock, err := v.checkClosed()
	if err != nil {
		return err
	}
	defer unlock()
	mvr, closer, err := pdb.MVGet(v.sp, dbtab.TID_LOCAL_VOLUME_CELLS, v.vol.lvid.Marshal(nil), activeTxns)
	if err != nil {
		return err
	}
	defer closer.Close()
	if mvr == nil {
		*dst = (*dst)[:0]
	} else {
		*dst = append((*dst)[:0], mvr.Value...)
	}
	return nil
}

func (v *localTxnRO) Save(ctx context.Context, root []byte) error {
	return blobcache.ErrTxReadOnly{Op: "Save"}
}

func (v *localTxnRO) Delete(ctx context.Context, cids []blobcache.CID) error {
	return blobcache.ErrTxReadOnly{Op: "Delete"}
}

func (v *localTxnRO) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return blobcache.CID{}, blobcache.ErrTxReadOnly{Op: "Post"}
}

func (v *localTxnRO) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	var exists [1]bool
	if err := v.Exists(ctx, []blobcache.CID{cid}, exists[:]); err != nil {
		return 0, err
	} else if !exists[0] {
		return 0, cadata.ErrNotFound{Key: cid}
	}
	unlock, err := v.checkClosed()
	if err != nil {
		return 0, err
	}
	defer unlock()
	return v.sys.readBlobData(blobKey(cid), buf)
}

func (v *localTxnRO) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	activeTxns, err := v.getExcluded()
	if err != nil {
		return err
	}
	for i, cid := range cids {
		exists, err := volumeBlobExists(v.sp, v.vol.lvid, cid, activeTxns)
		if err != nil {
			return err
		}
		dst[i] = exists
	}
	return nil
}

func (v *localTxnRO) Link(ctx context.Context, subvol blobcache.OID, mask blobcache.ActionSet) error {
	return blobcache.ErrTxReadOnly{Op: "AllowLink"}
}

func (v *localTxnRO) Unlink(ctx context.Context, targets []blobcache.OID) error {
	return blobcache.ErrTxReadOnly{Op: "Unlink"}
}

func (v *localTxnRO) VisitLinks(ctx context.Context, targets []blobcache.OID) error {
	return blobcache.ErrTxReadOnly{Op: "VisitLinks"}
}

func (v *localTxnRO) MaxSize() int {
	return int(v.vol.params.MaxSize)
}

func (v *localTxnRO) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.vol.params.HashAlgo.HashFunc()(salt, data)
}

func (txn *localTxnRO) Volume() volumes.Volume {
	return txn.vol
}

func (v *localTxnRO) Visit(ctx context.Context, cids []blobcache.CID) error {
	return blobcache.ErrTxReadOnly{Op: "Visit"}
}

func (v *localTxnRO) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return blobcache.ErrTxReadOnly{Op: "IsVisited"}
}

func (v *localTxnRO) getExcluded() (func(pdb.MVTag) bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.activeTxns == nil {
		activeTxns := make(map[pdb.MVTag]struct{})
		if err := v.sys.txSys.ReadActive(v.sp, activeTxns); err != nil {
			return nil, err
		}
		v.activeTxns = activeTxns
	}
	return v.isExcluded, nil
}

// isExcluded returns true if the given transaction is excluded from the snapshot.
// Do not call this directly, use getExcluded instead.
func (v *localTxnRO) isExcluded(mvid pdb.MVTag) bool {
	_, ok := v.activeTxns[mvid]
	return ok
}
