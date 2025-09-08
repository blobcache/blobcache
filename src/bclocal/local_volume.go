package bclocal

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sqlutil"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
	"github.com/cockroachdb/pebble"
	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/pkg/dbutil"
	"go.brendoncarroll.net/state/cadata"
)

// MVCCID is used to order transactions on volumes.
type MVCCID uint64

// localSystem manages the local volumes and transactions on those volumes.
type localSystem struct {
	db      *pebble.DB
	blobDir *os.Root

	// volumes prevents concurrent operations on:
	// txns allows a transaction to be locked.
	txns mapOfLocks[blobcache.OID]
	// blobs prevents concurrent operations on blobs
	blobs mapOfLocks[blobcache.CID]
}

func newLocalSystem(db *pebble.DB, blobDir *os.Root) localSystem {
	return localSystem{
		db:      db,
		blobDir: blobDir,
	}
}

func (s *localSystem) beginTx(ctx context.Context, volID blobcache.OID) (*localTxn, error) {
	ba := s.db.NewBatch()
	defer ba.Close()
	ve, err := getVolume(ba, volID)
	if err != nil {
		return nil, err
	}
	if ve == nil {
		return nil, blobcache.ErrNotFound{ID: volID}
	}
	putLocalTxn(ba, ve.RowID, false)
	txnid, err := createLocalTxn(ba, ve.RowID, false)
	if err != nil {
		return nil, err
	}
	volInfo, err := inspectVolume(ba, volID)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// postBlob adds a blob to a local volume.
func (s *localSystem) postBlob(volID blobcache.OID, mvid MVCCID, cid blobcache.CID, salt *blobcache.CID, data []byte) error {
	if exists, err := blobExists(s.db, volID, cid); err != nil {
		return err
	} else if exists {
		return nil
	}

	ba := s.db.NewIndexedBatch()
	defer ba.Close()

	// add the blob if necessary.
	if yes, err := haveBlobMeta(ba, cid); err != nil {
		return err
	} else if !yes {
		flags := uint8(0)
		if salt != nil {
			flags |= 1 << 0
		}
		if err := putBlobMeta(ba, blobMeta{
			cid: cid,

			flags:    flags,
			size:     uint32(len(data)),
			refCount: 1,
			salt:     salt,
		}); err != nil {
			return err
		}
		if err := putBlobData(ba, cid, data); err != nil {
			return err
		}
	}

	// add the blob to the volume.

	return ba.Commit(nil)
}

// dropVolume removes all state associated with a volume.
func (s *localSystem) dropVolume(ctx context.Context, volID blobcache.OID) error {
	ba := s.db.NewIndexedBatch()
	defer ba.Close()
	return ba.Commit(nil)
}

func blobExists(db dbReading, volID blobcache.OID, mvc MVCCID, cid blobcache.CID) (bool, error) {
	k := tableKey(nil, tid_LOCAL_VOLUME_BLOBS, slices.Concat(volID[:], cid[:], binary.BigEndian.AppendUint64(nil, uint64(mvc))))
	_, closer, err := db.Get(k)
	if err != nil {
		return false, err
	}
	defer closer.Close()
	return true, nil
}

var _ volumes.Volume = &localVolume{}

type localVolume struct {
	s   *Service
	oid blobcache.OID
}

func newLocalVolume(s *Service, oid blobcache.OID) *localVolume {
	return &localVolume{
		s:   s,
		oid: oid,
	}
}

func (v *localVolume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	panic("not implemented")
}

func (v *localVolume) BeginTx(ctx context.Context, spec blobcache.TxParams) (volumes.Tx, error) {
	// loop until there is no active tx on the volume.
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	var volInfo *blobcache.VolumeInfo
	var ltxid *LocalTxnID
	for {
		var err error
		if err = sqlutil.DoTx(ctx, v.s.db, func(tx *sqlx.Tx) error {
			volRow, err := getLocalVolumeByOID(tx, v.oid)
			if err != nil {
				return err
			}
			if spec.Mutate {
				if yes, err := volumeHasMutatingTx(tx, volRow.RowID); err != nil {
					return err
				} else if yes {
					return nil
				}
			}
			volInfo, err = inspectVolume(tx, v.oid)
			if err != nil {
				return err
			}
			ltxid2, err := createLocalTxn(tx, volRow.RowID, spec.Mutate)
			if err != nil {
				return err
			}
			ltxid = &ltxid2
			return nil
		}); err != nil {
			return nil, err
		}
		if ltxid != nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
		}
	}
	return newLocalTxn(ctx, v.s, *ltxid, volInfo)
}

// createLocalTxn creates a new transaction object in the database.
func createLocalTxn(tx *sqlx.Tx, volID LocalVolumeID, mutate bool) (LocalTxnID, error) {
	var base LocalTxnID
	if err := tx.Get(&base, "SELECT base FROM local_volumes WHERE rowid = ?", volID); err != nil {
		return 0, err
	}
	var ret LocalTxnID
	if err := tx.Get(&ret, "INSERT INTO local_txns (volume, base, mutate) VALUES (?, ?, ?) RETURNING rowid", volID, base, mutate); err != nil {
		return 0, err
	}
	return ret, nil
}

// dropLocalTxn drops the transaction's store, and deletes the transaction.
// no pending changes are applied.
func dropLocalTxn(tx *sqlx.Tx, txid LocalTxnID) error {
	// Delete the transaction
	_, err := tx.Exec("DELETE FROM local_txns WHERE rowid = ?", txid)
	if err != nil {
		return err
	}
	return nil
}

// getLocalTxn gets a local volume transaction by its oid.
func getLocalTxn(tx *sqlx.Tx, txid LocalTxnID) (*localTxnRow, error) {
	var ret localTxnRow
	if err := tx.Get(&ret, "SELECT rowid, base, volume, mutate FROM local_txns WHERE rowid = ?", txid); err != nil {
		return nil, err
	}
	return &ret, nil
}

// txnLoadRoot reads the root of the volume at the given transaction.
func txnLoadRoot(tx *sqlx.Tx, volID LocalVolumeID, baseTxid, txid LocalTxnID, dst *[]byte) error {
	if err := tx.Get(dst, `SELECT root FROM local_vol_roots
		WHERE vol_id = ? AND (txn_id = ? OR txn_id <= ?)
		ORDER BY txn_id DESC
		LIMIT 1
	`, volID, txid, baseTxid); err != nil {
		return err
	}
	return nil
}

// txnSetRoot sets the root of a local volume.
func txnSetRoot(ba *pebble.Batch, volID blobcache.OID, txid MVCCID, root []byte) error {
	if root == nil {
		root = []byte{}
	}
	k := tableKey(nil, tid_LOCAL_VOLUME_CELLS, append(volID[:], txid[:]...))
	rootBuf := binary.LittleEndian.AppendUint64(nil, uint64(len(root)))
	rootBuf = append(rootBuf, root...)
	if err := ba.Set(k, rootBuf, nil); err != nil {
		return err
	}
	return nil
}

// putVolumeLinks puts the volume links into the database.
func putVolumeLinks(ba *pebble.Batch, volID blobcache.OID, m map[blobcache.OID]blobcache.ActionSet) error {
	for target, rights := range m {
		if err := putVolumeLink(ba, volID, target, rights); err != nil {
			return err
		}
	}
	return nil
}

// putVolumeLink puts a single volume link into the database.
func putVolumeLink(ba *pebble.Batch, fromID blobcache.OID, toID blobcache.OID, rights blobcache.ActionSet) error {
	k := tableKey(nil, tid_VOLUME_LINKS, append(fromID[:], toID[:]...))
	rightsBuf := binary.LittleEndian.AppendUint64(nil, uint64(rights))
	return ba.Set(k, rightsBuf, nil)
}

// parseVolumeLink parses an entry from the VOLUME_LINKS table.
func parseVolumeLink(k, v []byte) (blobcache.OID, schema.Link, error) {
	if len(k) < 4 {
		return blobcache.OID{}, schema.Link{}, fmt.Errorf("volume link key too short: %d", len(k))
	}
	k = k[4:]
	if len(k) < 2*blobcache.OIDSize {
		return blobcache.OID{}, schema.Link{}, fmt.Errorf("volume link key too short: %d", len(k))
	}
	fromID := blobcache.OID(k[:blobcache.OIDSize])
	toID := blobcache.OID(k[blobcache.OIDSize:])
	return fromID, schema.Link{
		Target: toID,
		Rights: blobcache.ActionSet(binary.LittleEndian.Uint64(v)),
	}, nil
}

// readVolumeLinks reads the volume links from volID into dst.
func readVolumeLinks(sp dbReading, fromVolID blobcache.OID, dst map[blobcache.OID]blobcache.ActionSet) error {
	iter, err := sp.NewIter(&pebble.IterOptions{
		LowerBound: tableKey(nil, tid_VOLUME_LINKS, fromVolID[:]),
		UpperBound: tableKey(nil, tid_VOLUME_LINKS, append(fromVolID[:], 0)),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Next() {
		volLink, err := parseVolumeLink(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		dst[volLink.Target] = volLink.Rights
	}
	return nil
}

// txnCommit commits a transaction.
// It updates the volume's last_txn field to the new transaction.
func txnCommit(tx *sqlx.Tx, volID LocalVolumeID, txid LocalTxnID, root []byte) error {
	if err := txnSetRoot(tx, volID, txid, root); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE local_volumes SET base = ? WHERE rowid = ?`, txid, volID); err != nil {
		return err
	}
	return dropLocalTxn(tx, txid)
}

var _ volumes.Tx = &localTxn{}

// localTxn is a transaction on a local volume.
type localTxn struct {
	s           *Service
	localTxnRow localTxnRow
	volInfo     blobcache.VolumeInfo
	schema      schema.Schema

	mu           sync.Mutex
	allowedLinks map[blobcache.OID]blobcache.ActionSet
	root         []byte
}

// newLocalTxn creates a localTxn.
// It does not change the database state.
// the caller should have already created the transaction at txid, and volInfo.
func newLocalTxn(ctx context.Context, s *Service, txid LocalTxnID, volInfo *blobcache.VolumeInfo) (*localTxn, error) {
	txRow, err := sqlutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*localTxnRow, error) {
		return getLocalTxn(tx, txid)
	})
	if err != nil {
		return nil, err
	}
	schema, err := s.getSchema(volInfo.Schema)
	if err != nil {
		return nil, err
	}
	return &localTxn{
		s:           s,
		localTxnRow: *txRow,
		volInfo:     *volInfo,
		schema:      schema,
	}, nil
}

func (v *localTxn) Volume() volumes.Volume {
	return newLocalVolume(v.s, v.volInfo.ID)
}

func (v *localTxn) Commit(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.localTxnRow.Mutate {
		return blobcache.ErrTxReadOnly{}
	}

	// produce a final set of links
	links := make(map[blobcache.OID]blobcache.ActionSet)
	if contSch, ok := v.schema.(schema.Container); ok {
		src := volumes.NewUnsaltedStore(v)
		if err := contSch.ReadLinks(ctx, src, v.root, links); err != nil {
			return err
		}
		// constrain all claimed links by the allowed links.
		for target, rights := range links {
			links[target] |= v.allowedLinks[target] & rights
			if links[target] == 0 {
				delete(links, target)
			}
		}
	}

	return sqlutil.DoTx(ctx, v.s.db, func(tx *sqlx.Tx) error {
		if err := putVolumeLinks(tx, v.volInfo.ID, links); err != nil {
			return err
		}
		if v.root == nil {
			// if the root is nil, then it has not be changed in this transaction.
			// get the previous root from the database.
			if err := txnLoadRoot(tx, v.localTxnRow.VolID, v.localTxnRow.Base, v.localTxnRow.RowID, &v.root); err != nil {
				return err
			}
		}
		return txnCommit(tx, v.localTxnRow.VolID, v.localTxnRow.RowID, v.root)
	})
}

func (v *localTxn) Abort(ctx context.Context) error {
	return sqlutil.DoTx(ctx, v.s.db, func(tx *sqlx.Tx) error {
		return dropLocalTxn(tx, v.localTxnRow.RowID)
	})
}

func (v *localTxn) Save(ctx context.Context, root []byte) error {
	if !v.localTxnRow.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if root == nil {
		// This is to distinguish between a:
		//  - nil root (Save not called)
		//  - an empty root (Save called with an zero length slice)
		root = []byte{}
	}
	v.root = append(v.root[:0], root...)
	return nil
}

func (v *localTxn) Load(ctx context.Context, dst *[]byte) error {
	return sqlutil.DoTx(ctx, v.s.db, func(tx *sqlx.Tx) error {
		return txnLoadRoot(tx, v.localTxnRow.VolID, v.localTxnRow.Base, v.localTxnRow.RowID, dst)
	})
}

func (v *localTxn) Delete(ctx context.Context, cids []blobcache.CID) error {
	return dbutil.DoTx(ctx, v.s.db, func(tx *sqlx.Tx) error {
		for _, cid := range cids {
			if err := deleteBlob(tx, v.localTxnRow.VolID, v.localTxnRow.RowID, cid); err != nil {
				return err
			}
		}
		return nil
	})
}

func (v *localTxn) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	if salt != nil && !v.volInfo.Salted {
		return blobcache.CID{}, blobcache.ErrCannotSalt{}
	}
	cid, err := sqlutil.DoTx1(ctx, v.s.db, func(tx *sqlx.Tx) (*blobcache.CID, error) {
		cid := v.Hash(salt, data)
		if err := ensureBlob(tx, cid, nil, data); err != nil {
			return nil, err
		}
		if err := addBlob(tx, v.localTxnRow.VolID, v.localTxnRow.RowID, cid); err != nil {
			return nil, err
		}
		return &cid, nil
	})
	if err != nil {
		return blobcache.CID{}, err
	}
	return *cid, nil
}

func (v *localTxn) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	return sqlutil.DoTx1(ctx, v.s.db, func(tx *sqlx.Tx) (int, error) {
		return readBlob(tx, v.localTxnRow.VolID, v.localTxnRow.Base, v.localTxnRow.RowID, cid, buf)
	})
}

func (v *localTxn) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return dbutil.DoTx(ctx, v.s.db, func(tx *sqlx.Tx) error {
		for i, cid := range cids {
			exists, err := txnContainsBlob(tx, v.localTxnRow.VolID, v.localTxnRow.Base, v.localTxnRow.RowID, cid)
			if err != nil {
				return err
			}
			// if the blob exists, then we need to add it to the tx store.
			if exists {
				if err := addBlob(tx, v.localTxnRow.VolID, v.localTxnRow.RowID, cid); err != nil {
					return err
				}
			}
			dst[i] = exists
		}
		return nil
	})
}

func (v *localTxn) MaxSize() int {
	return int(v.volInfo.MaxSize)
}

func (v *localTxn) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	hf := v.volInfo.HashAlgo.HashFunc()
	return hf(salt, data)
}

func (v *localTxn) Info() blobcache.VolumeInfo {
	return v.volInfo
}

func (v *localTxn) AllowLink(ctx context.Context, subvol blobcache.Handle) error {
	if !v.localTxnRow.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	if _, ok := v.schema.(schema.Container); !ok {
		return fmt.Errorf("schema %T for volume %s is not a container", v.schema, v.volInfo.ID)
	}
	link, err := v.s.handleToLink(subvol)
	if err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.allowedLinks == nil {
		v.allowedLinks = make(map[blobcache.OID]blobcache.ActionSet)
	}
	v.allowedLinks[link.Target] |= link.Rights
	return nil
}

var _ volumes.Tx = &localTxnRO{}

// localTxnRO is a read-only transaction on a local volume.
type localTxnRO struct {
	sys         *localSystem
	localTxnRow localTxnRow
	volInfo     blobcache.VolumeInfo
	sp          *pebble.Snapshot
}

func (v *localTxnRO) Abort(ctx context.Context) error {
	return v.sp.Close()
}

func (v *localTxnRO) Commit(ctx context.Context) error {
	return blobcache.ErrTxReadOnly{}
}

func (v *localTxnRO) Load(ctx context.Context, dst *[]byte) error {
	iter, err := v.sp.NewIter(&pebble.IterOptions{
		LowerBound: tableKey(nil, tid_LOCAL_VOLUME_CELLS, v.volInfo.ID[:]),
		UpperBound: tableKey(nil, tid_LOCAL_VOLUME_CELLS, slices.Concat(v.volInfo.ID[:], maxMVCCID[:])),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	if iter.Last() {
		*dst = append((*dst)[:0], iter.Value()...)
	}
	return nil
}

func (v *localTxnRO) Save(ctx context.Context, root []byte) error {
	return blobcache.ErrTxReadOnly{Op: "Save"}
}

func (v *localTxnRO) Delete(ctx context.Context, cid blobcache.CID) error {
	return blobcache.ErrTxReadOnly{Op: "Delete"}
}

func (v *localTxnRO) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	return blobcache.CID{}, blobcache.ErrTxReadOnly{Op: "Post"}
}

func (v *localTxnRO) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	if ok, err := v.Exists(ctx, cid); err != nil {
		return 0, err
	} else if !ok {
		return 0, cadata.ErrNotFound{Key: cid}
	}

}

func (v *localTxnRO) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	iter, err := v.sp.NewIter(&pebble.IterOptions{
		LowerBound: tableKey(nil, tid_LOCAL_VOLUME_CELLS, slices.Concat(v.volInfo.ID[:], cid[:])),
		UpperBound: tableKey(nil, tid_LOCAL_VOLUME_CELLS, slices.Concat(v.volInfo.ID[:], cid[:], maxMVCCID[:])),
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	if iter.Last() {
		return true, nil
	}
	return false, nil
}

func (v *localTxnRO) AllowLink(ctx context.Context, subvol blobcache.Handle) error {
	return blobcache.ErrTxReadOnly{Op: "AllowLink"}
}

func (v *localTxnRO) MaxSize() int {
	return int(v.volInfo.MaxSize)
}

func (v *localTxnRO) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.volInfo.HashAlgo.HashFunc()(salt, data)
}

func (v *localTxnRO) Info() blobcache.VolumeInfo {
	return v.volInfo
}

func (v *localTxnRO) Volume() volumes.Volume {
	return newLocalVolume(v.sys, v.volInfo.ID)
}

var maxMVCCID [8]byte = func() [8]byte {
	var ret [8]byte
	for i := range ret {
		ret[i] = 0xff
	}
	return ret
}()
