package localvol

import (
	"context"
	"sync"

	"blobcache.io/blobcache/src/bccore"
	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"github.com/cockroachdb/pebble"
)

var _ backend.Tx = &localTxnRO{}

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

func (v *localTxnRO) Params() blobcache.TxParams {
	return blobcache.TxParams{}
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
		return 0, blobcache.ErrNotFound{CID: cid}
	}
	unlock, err := v.checkClosed()
	if err != nil {
		return 0, err
	}
	defer unlock()
	return v.sys.readBlobData(cid, buf)
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

func (v *localTxnRO) Link(ctx context.Context, svoid blobcache.OID, rights blobcache.ActionSet, targetVol bccore.AnyObject) (*blobcache.LinkToken, error) {
	return nil, blobcache.ErrTxReadOnly{Op: "AllowLink"}
}

func (v *localTxnRO) Unlink(ctx context.Context, targets []blobcache.LinkTokenID) error {
	return blobcache.ErrTxReadOnly{Op: "Unlink"}
}

func (v *localTxnRO) VisitLinks(ctx context.Context, targets []blobcache.LinkTokenID) error {
	return blobcache.ErrTxReadOnly{Op: "VisitLinks"}
}

func (v *localTxnRO) MaxSize() int {
	return int(v.vol.params.MaxSize)
}

func (v *localTxnRO) HashAlgo() blobcache.HashAlgo {
	return v.vol.params.HashAlgo
}

func (v *localTxnRO) Hash(data []byte) blobcache.CID {
	return v.vol.params.HashAlgo.Hash(data)
}

func (txn *localTxnRO) Volume() backend.Volume {
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
