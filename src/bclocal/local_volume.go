package bclocal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"

	"blobcache.io/blobcache/src/bclocal/internal/blobman"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/state/cadata"
)

type LocalVolumeID uint64

func (lvid LocalVolumeID) Marshal(out []byte) []byte {
	return binary.BigEndian.AppendUint64(out, uint64(lvid))
}

// localSystem manages the local volumes and transactions on those volumes.
type localSystem struct {
	cfg       Config
	db        *pebble.DB
	blobs     *blobman.Store
	hsys      *handleSystem
	getSchema func(blobcache.SchemaSpec) schema.Schema

	// txSys manages the transaction sequence number, and the set of active transactions.
	txSys txSystem

	// mutVol manages access to volumes
	// only one mutating transaction can access a volume at a time.
	// mutating transactions should hold one of these locks starting in beginTx, and release when {Commit, Abort} is called.
	mutVol mapOfLocks[LocalVolumeID]
	// blobLocks prevents concurrent operations on blobLocks
	// only one {Post, Delete, Add} operation can have a lock at a time.
	// - Post could potentially increment the refCount, and ingest blob data.
	// - Delete could potentially decrement the refCount, and delete blob data.
	// - Add could potentially increment the refCount, but does not ingest blob data.
	blobLocks mapOfLocks[blobman.Key]
}

func newLocalSystem(cfg Config, db *pebble.DB, blobDir *os.Root, hsys *handleSystem, getSchema func(blobcache.SchemaSpec) schema.Schema) localSystem {
	return localSystem{
		cfg:       cfg,
		db:        db,
		hsys:      hsys,
		blobs:     blobman.New(blobDir),
		getSchema: getSchema,

		txSys: newTxSystem(db),
	}
}

func (ls *localSystem) GenerateLocalID() (LocalVolumeID, error) {
	n, err := ls.txSys.allocateTxID()
	if err != nil {
		return 0, err
	}
	return LocalVolumeID(n), nil
}

func (ls *localSystem) Open(lvid LocalVolumeID) (*localVolume, error) {
	return newLocalVolume(ls, lvid), nil
}

// GCBlobs walks all of the blob reference counts, and deletes any blobs that have a reference count of 0.
func (ls *localSystem) GCBlobs(ctx context.Context, lvid LocalVolumeID) error {
	iter, err := ls.db.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: pdb.TableLowerBound(tid_BLOB_REF_COUNT),
		UpperBound: pdb.TableUpperBound(tid_BLOB_REF_COUNT),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var cid blobcache.CID
		// this will only be the prefix
		copy(cid[:], iter.Key())

		if len(iter.Value()) != 4 {
			return fmt.Errorf("invalid value length: %d", len(iter.Value()))
		}
		refCount := binary.BigEndian.Uint32(iter.Value())
		if refCount != 0 {
			continue
		}

		if err := func() error {
			k := blobKey(cid)
			if err := ls.blobLocks.Lock(ctx, k); err != nil {
				return err
			}
			defer ls.blobLocks.Unlock(k)
			return ls.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
				return ls.blobs.Delete(blobKey(cid))
			})
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (ls *localSystem) doRO(fn func(sp *pebble.Snapshot, ignore func(pdb.MVTag) bool) error) error {
	sp := ls.db.NewSnapshot()
	defer sp.Close()
	active := make(pdb.MVSet)
	if err := ls.txSys.readActive(sp, active); err != nil {
		return err
	}
	excluding := func(mvid pdb.MVTag) bool {
		_, ok := active[mvid]
		return ok
	}
	if err := fn(sp, excluding); err != nil {
		return err
	}
	return nil
}

// doRW creates an indexed batch, reads the set of active transactions from the SYS_TXNS table, and then calls fn
// with both of those things.
// if fn returns nil, the batch is committed, otherwise it is rolled back.
// the caller should use the ignore function to exclude rows written by active transactions.
func (ls *localSystem) doRW(fn func(ba *pebble.Batch, ignore func(pdb.MVTag) bool) error) error {
	ba := ls.db.NewIndexedBatch()
	defer ba.Close()
	active := make(pdb.MVSet)
	if err := ls.txSys.readActive(ba, active); err != nil {
		return err
	}
	excluding := func(mvid pdb.MVTag) bool {
		_, ok := active[mvid]
		return ok
	}
	if err := fn(ba, excluding); err != nil {
		return err
	}
	return ba.Commit(nil)
}

func (ls *localSystem) beginTx(ctx context.Context, lvid LocalVolumeID, params blobcache.TxParams) (_ volumes.Tx, retErr error) {
	if !params.Mutate {
		sp := ls.db.NewSnapshot()
		volInfo, err := inspectVolume(sp, oidFromLocalID(lvid))
		if err != nil {
			return nil, err
		}
		if volInfo == nil {
			return nil, fmt.Errorf("volume not found, but it should exist")
		}
		volParams := blobcache.VolumeParams{
			MaxSize:  volInfo.MaxSize,
			HashAlgo: blobcache.HashAlgo(volInfo.HashAlgo),
			Salted:   volInfo.Salted,
		}
		return newLocalTxnRO(ls, lvid, sp, volParams), nil
	}

	txid, err := ls.txSys.allocateTxID()
	if err != nil {
		return nil, err
	}
	// this could potentially block for a while, so do it first.
	if err := ls.mutVol.Lock(ctx, lvid); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			ls.mutVol.Unlock(lvid)
		}
	}()

	// we don't use doRW here because, nothing we do accesses MVCC tables.
	ba := ls.db.NewIndexedBatch()
	defer ba.Close()
	vinfo, err := inspectVolume(ba, oidFromLocalID(lvid))
	if err != nil {
		return nil, err
	}
	if vinfo == nil {
		return nil, fmt.Errorf("volume not found. OID: %v", oidFromLocalID(lvid))
	}
	if err := putLocalVolumeTxn(ba, lvid, txid); err != nil {
		return nil, err
	}
	if err := ba.Commit(nil); err != nil {
		return nil, err
	}

	volParams := blobcache.VolumeParams{
		Schema:   vinfo.Schema,
		MaxSize:  vinfo.MaxSize,
		HashAlgo: blobcache.HashAlgo(vinfo.HashAlgo),
		Salted:   vinfo.Salted,
	}
	txParams := blobcache.TxParams{
		Mutate: params.Mutate,
		GC:     params.GC,
	}
	sch := ls.getSchema(volParams.Schema)
	if sch == nil {
		return nil, fmt.Errorf("unknown schema %s", vinfo.Schema)
	}
	return newLocalTxn(ls, lvid, txid, volParams, txParams, sch)
}

// abortMut aborts a mutating transaction.
func (s *localSystem) abortMut(volID LocalVolumeID, mvid pdb.MVTag) error {
	if err := func() error {
		ba := s.db.NewIndexedBatch()
		defer ba.Close()
		yesActive, err := s.txSys.isActive(ba, mvid)
		if err != nil {
			return err
		}
		if !yesActive {
			return nil
		}
		if err := s.txSys.failure(ba, mvid); err != nil {
			return err
		}
		return ba.Commit(nil)
	}(); err != nil {
		return err
	}
	s.mutVol.Unlock(volID)

	if err := func() error {
		ba := s.db.NewBatch()
		defer ba.Close()
		if err := pdb.Undo(s.db, ba, tid_LOCAL_VOLUME_BLOBS, volID.Marshal(nil), mvid); err != nil {
			return err
		}
		if err := pdb.Undo(s.db, ba, tid_LOCAL_VOLUME_CELLS, volID.Marshal(nil), mvid); err != nil {
			return err
		}
		if err := s.txSys.removeFailed(ba, mvid); err != nil {
			return err
		}
		return ba.Commit(nil)
	}(); err != nil {
		return err
	}
	return nil
}

// commit commits a local volume.
// links should be the actual links returned by the schema
// newlyAllowed should be the allowed links that were added to the volume during the transaction
func (s *localSystem) commit(volID LocalVolumeID, mvid pdb.MVTag, links linkSet, newlyAllowed linkSet) error {
	ba := s.db.NewIndexedBatch()
	defer ba.Close()

	oid := oidFromLocalID(volID)
	prevLinks := make(linkSet)
	if err := readVolumeLinks(ba, oid, prevLinks); err != nil {
		return err
	}
	for target := range links {
		links[target] &= (prevLinks[target] | newlyAllowed[target])
		if links[target] == 0 {
			delete(links, target)
		}
	}
	if err := putVolumeLinks(ba, oid, links); err != nil {
		return err
	}
	if err := s.txSys.success(ba, mvid); err != nil {
		return err
	}
	if !s.cfg.NoSync {
		if err := s.blobs.Flush(); err != nil {
			return err
		}
	}
	if err := ba.Commit(nil); err != nil {
		return err
	}

	s.mutVol.Unlock(volID)
	return nil
}

// gc walks the VOLUME_BLOBS table, and deletes any blobs that are not marked as visited.
func (s *localSystem) gc(ctx context.Context, volID LocalVolumeID, mvid pdb.MVTag) error {
	return s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		iter := ba.NewIterWithContext(ctx, &pebble.IterOptions{
			LowerBound: pdb.TKey{TableID: tid_LOCAL_VOLUME_BLOBS, Key: volID.Marshal(nil)}.Marshal(nil),
			UpperBound: pdb.TKey{TableID: tid_LOCAL_VOLUME_BLOBS, Key: pdb.PrefixUpperBound(volID.Marshal(nil))}.Marshal(nil),
			SkipPoint: func(k []byte) bool {
				mvk, err := pdb.ParseMVKey(k)
				if err != nil {
					return false
				}
				// Exclude other active transactions, but include the current transaction even if active.
				return mvk.Version != mvid && excluding != nil && excluding(mvk.Version)
			},
		})
		defer iter.Close()

		// Iterate grouped by cidPrefix. For each group, if the latest included row is non-tombstoned
		// and this transaction did not mark VISITED, then write a tombstone at mvid.
		var (
			curCID    blobman.Key
			curInit   bool
			latestLen int
			visited   bool
		)
		flush := func() error {
			if !curInit {
				return nil
			}
			if latestLen > 0 && !visited {
				if err := tombVolumeBlob(ba, volID, mvid, curCID); err != nil {
					return err
				}
			}
			return nil
		}

		for iter.First(); iter.Valid(); iter.Next() {
			mvk, err := pdb.ParseMVKey(iter.Key())
			if err != nil {
				return err
			}
			cidp := blobman.KeyFromBytes(mvk.Key[8:])

			// New group
			if !curInit || cidp != curCID {
				if err := flush(); err != nil {
					return err
				}
				curCID = cidp
				curInit = true
				latestLen = -1
				visited = false
			}

			v := iter.Value()
			// Track VISITED in this transaction only
			if mvk.Version == mvid && len(v) > 0 && (v[0]&volumeBlobFlag_VISITED) != 0 {
				visited = true
			}
			// Latest row among included versions is the last encountered in ascending iteration
			latestLen = len(v)
		}
		return flush()
	})
}

// postBlob adds a blob to a local volume.
func (s *localSystem) postBlob(ctx context.Context, volID LocalVolumeID, mvid pdb.MVTag, cid blobcache.CID, salt *blobcache.CID, data []byte) error {
	// if the blob has already been added in the current transaction, then we don't need to do anything.
	vbFlags, rowExists, err := s.getVolumeBlob(s.db, volID, cid, mvid)
	if err != nil {
		return err
	}
	if rowExists && vbFlags&volumeBlobFlag_ADDED > 0 {
		// the blob has already been added in the current transaction.
		return nil
	}

	k := blobKey(cid)
	if err := s.blobLocks.Lock(ctx, k); err != nil {
		return err
	}
	defer s.blobLocks.Unlock(k)
	if err := s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		if yes, err := existsBlobMeta(ba, cid); err != nil {
			return err
		} else if !yes {
			// need to add the blob
			if _, err := s.blobs.Put(k, data); err != nil {
				return err
			}

			if err := putBlobMeta(ba, blobMeta{
				cid: cid,

				flags: mkBlobMetaFlags(salt != nil),
				size:  uint32(len(data)),
				salt:  salt,
			}); err != nil {
				return err
			}
		}
		return setVolumeBlob(ba, volID, mvid, cid, volumeBlobFlag_ADDED)
	}); err != nil {
		return err
	}
	return nil
}

// getBlob is the main entry point for the Get operation.
func (s *localSystem) getBlob(volID LocalVolumeID, mvid pdb.MVTag, cid blobcache.CID, buf []byte) (int, error) {
	k := blobKey(cid)
	var n int
	if err := s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		excluding2 := excludeExcluding(excluding, mvid)
		mvr, closer, err := pdb.MVGet(ba, tid_LOCAL_VOLUME_BLOBS, slices.Concat(volID.Marshal(nil), k.Bytes()), excluding2)
		if err != nil {
			return err
		}
		defer closer.Close()
		if mvr == nil {
			// blob is not in the volume, return not found
			return cadata.ErrNotFound{Key: cid}
		}
		// read into buffer
		n, err = s.readBlobData(k, buf)
		if err != nil {
			return err
		}
		// mark the blob as observed
		if err := setVolumeBlob(ba, volID, mvid, cid, 0); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// deleteBlob deletes a blob from a local volume.
// deleting a blob is done by adding a new row to the LOCAL_VOLUME_BLOBS table, with an empty value.
// ref counts are not affected by this operation.
func (s *localSystem) deleteBlob(volID LocalVolumeID, mvid pdb.MVTag, cids []blobcache.CID) error {
	// delete does not update ref counts, and doesn't need to check if the blobs exists
	// so we don't need to acquire a lock on the blobs.
	// if there are concurrent Post or Add operations, then they will race to set the (volume, blob) row.
	// but that's the callers fault, and any order is valid.
	return s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		for _, cid := range cids {
			if err := tombVolumeBlob(ba, volID, mvid, blobKey(cid)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *localSystem) blobExists(volID LocalVolumeID, mvid pdb.MVTag, cids []blobcache.CID, dst []bool) error {
	sn := s.db.NewSnapshot()
	defer sn.Close()
	active := make(pdb.MVSet)
	if err := s.txSys.readActive(sn, active); err != nil {
		return err
	}
	excluding := func(x pdb.MVTag) bool {
		if x == mvid {
			return false // never exclude the current transaction
		} else {
			_, ok := active[x]
			return ok
		}
	}
	for i, cid := range cids {
		exists, err := volumeBlobExists(sn, volID, cid, excluding)
		if err != nil {
			return err
		}
		dst[i] = exists
	}
	return nil
}

// load loads the root data for a local volume.
func (s *localSystem) load(volID LocalVolumeID, mvid pdb.MVTag, dst *[]byte) error {
	return s.doRO(func(sp *pebble.Snapshot, ignoring func(pdb.MVTag) bool) error {
		ignoring2 := func(x pdb.MVTag) bool {
			return x != mvid && ignoring(x)
		}
		mvr, closer, err := pdb.MVGet(sp, tid_LOCAL_VOLUME_CELLS, volID.Marshal(nil), ignoring2)
		if err != nil {
			return err
		}
		defer closer.Close()
		if mvr == nil {
			return nil
		}
		*dst = append((*dst)[:0], mvr.Value...)
		return nil
	})
}

// visit marks all the cids as visited.
// If any of them do not exist, the operation is a no-op.
func (s *localSystem) visit(volID LocalVolumeID, mvid pdb.MVTag, cids []blobcache.CID) error {
	return s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		for _, cid := range cids {
			exists, err := volumeBlobExists(ba, volID, cid, excluding)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("cannot visit, blob %s does not exist", cid)
			}
			if err := setVolumeBlob(ba, volID, mvid, cid, volumeBlobFlag_VISITED); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *localSystem) isVisited(volID LocalVolumeID, mvid pdb.MVTag, cids []blobcache.CID, dst []bool) error {
	return s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		for i, cid := range cids {
			// we only have to check for a specific version.
			cidPrefix := cid[:16]
			k := pdb.MVKey{
				TableID: tid_LOCAL_VOLUME_BLOBS,
				Key:     slices.Concat(volID.Marshal(nil), cidPrefix),
				Version: mvid,
			}
			// if the (volume, blob) association has the 2nd bit of the value set, then the blob is visited.
			isVisit, err := pdb.Exists(ba, k.Marshal(nil), func(v []byte) bool {
				return len(v) > 0 && v[0]&volumeBlobFlag_VISITED > 0
			})
			if err != nil {
				return err
			}
			dst[i] = isVisit
		}
		return nil
	})
}

// getVolumeBlob gets a row from the LOCAL_VOLUME_BLOBS table.
// It does not perform an MVCC lookup, a specific version is required.
// If the record does not exist, or it is a tombstone (empty value), then (0, false, nil) is returned.
// If the record exists, then the flags and true and returned.
// If any error occurs, then (0, false, err) is returned.
func (s *localSystem) getVolumeBlob(db pdb.RO, volID LocalVolumeID, cid blobcache.CID, mvid pdb.MVTag) (uint8, bool, error) {
	bk := blobKey(cid)
	k := pdb.TKey{
		TableID: tid_LOCAL_VOLUME_BLOBS,
		Key:     slices.Concat(volID.Marshal(nil), bk.Bytes(), mvid.Marshal(nil)),
	}
	val, closer, err := db.Get(k.Marshal(nil))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// not set
			return 0, false, nil
		}
		return 0, false, err
	}
	defer closer.Close()
	if len(val) == 0 {
		// tombstone
		return 0, false, nil
	}
	// observed, added, or visited
	return val[0], true, nil
}

func (s *localSystem) readBlobData(k blobman.Key, buf []byte) (int, error) {
	var n int
	if found, err := s.blobs.Get(k, func(data []byte) {
		n = copy(buf, data)
	}); err != nil {
		return 0, err
	} else if !found {
		return 0, fmt.Errorf("blob metadata exists, but blob data was not found. key=%v", k)
	}
	return n, nil
}

// setVolumeBlob associates a volume with a blob according to the flags.
// setVolumeBlob requires an IndexedBatch because it may increment the ref count.
func setVolumeBlob(ba *pebble.Batch, volID LocalVolumeID, mvid pdb.MVTag, cid blobcache.CID, flags uint8) error {
	k := pdb.MVKey{
		TableID: tid_LOCAL_VOLUME_BLOBS,
		Key:     slices.Concat(volID.Marshal(nil), cid[:16]),
		Version: mvid,
	}
	val, closer, err := ba.Get(k.Marshal(nil))
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	var prevFlags uint8
	if len(val) > 0 {
		prevFlags = val[0]
	}
	nextFlags := prevFlags | flags
	if len(val) > 0 && prevFlags == nextFlags {
		// the flags match, so we can return early.
		return nil
	}
	if err := ba.Set(k.Marshal(nil), []byte{nextFlags}, nil); err != nil {
		return err
	}
	if len(val) == 0 {
		// anytime we add a (non-tombstone) reference to a blob in the VOLUME_BLOBS table, we have to increment the ref count.
		if _, err := blobRefCountIncr(ba, blobKey(cid), 1); err != nil {
			return err
		}
	}
	return nil
}

// unsetVolumeBlob writes an empty value to the LOCAL_VOLUME_BLOBS table.
// empty values are tombstones, which will eventually be cleaned up by the vacuum process.
func tombVolumeBlob(ba *pebble.Batch, volID LocalVolumeID, mvid pdb.MVTag, cidp blobman.Key) error {
	k := pdb.MVKey{
		TableID: tid_LOCAL_VOLUME_BLOBS,
		Key:     slices.Concat(volID.Marshal(nil), cidp.Bytes()),
		Version: mvid,
	}
	return ba.Set(k.Marshal(nil), nil, nil)
}

// volumeBlobExists returns true if the blob exists in the volume.
// This method checks the (volune, blob) association. See also: blobExistsMeta.
func volumeBlobExists(db pdb.RO, volID LocalVolumeID, cid blobcache.CID, excluding func(pdb.MVTag) bool) (bool, error) {
	cidPrefix := cid[:16]
	mvr, closer, err := pdb.MVGet(db, tid_LOCAL_VOLUME_BLOBS, slices.Concat(volID.Marshal(nil), cidPrefix), excluding)
	if err != nil {
		return false, err
	}
	defer closer.Close()
	if mvr == nil {
		return false, nil
	}
	if len(mvr.Value) == 0 {
		return false, nil
	}
	return true, nil
}

// lvSave saves the root data for a local volume.
// This performs a blind Set on the LOCAL_VOLUME_CELLS table.
func lvSave(w pdb.WO, volID LocalVolumeID, mvid pdb.MVTag, root []byte) error {
	k := pdb.MVKey{
		TableID: tid_LOCAL_VOLUME_CELLS,
		Key:     volID.Marshal(nil),
		Version: mvid,
	}
	return w.Set(k.Marshal(nil), root, nil)
}

var _ volumes.Volume = &localVolume{}

type localVolume struct {
	sys  *localSystem
	lvid LocalVolumeID
}

func newLocalVolume(sys *localSystem, lvid LocalVolumeID) *localVolume {
	return &localVolume{
		sys:  sys,
		lvid: lvid,
	}
}

func (v *localVolume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return fmt.Errorf("Await not implemented")
}

func (v *localVolume) BeginTx(ctx context.Context, tp blobcache.TxParams) (volumes.Tx, error) {
	return v.sys.beginTx(ctx, v.lvid, tp)
}

var _ volumes.Tx = &localTxnMut{}

// localTxnMut is a mutating transaction on a local volume.
type localTxnMut struct {
	localSys  *localSystem
	volid     LocalVolumeID
	mvid      pdb.MVTag
	volParams blobcache.VolumeParams
	txParams  blobcache.TxParams
	schema    schema.Schema
	hf        blobcache.HashFunc

	// mu protects the finished and allowedLinks fields.
	// mu must be taken exclusively to {Commit, Abort, AllowLink}
	// mu must be taken in read mode for all other operations {Save, Delete, Post, Get, Exists}.
	// checkFinished is the most convenient way to make sure the transaction is not finished, during an operation.
	mu sync.RWMutex
	// finished is set to true when the transaction is finished.
	finished     bool
	allowedLinks map[blobcache.OID]blobcache.ActionSet
}

// newLocalTxn creates a localTxn.
// It does not change the database state.
// the caller should have already created the transaction at txid, and volInfo.
func newLocalTxn(localSys *localSystem, lvid LocalVolumeID, mvid pdb.MVTag, volParams blobcache.VolumeParams, txParams blobcache.TxParams, schema schema.Schema) (*localTxnMut, error) {
	hf := volParams.HashAlgo.HashFunc()
	return &localTxnMut{
		localSys:  localSys,
		volid:     lvid,
		mvid:      mvid,
		volParams: volParams,
		txParams:  txParams,
		schema:    schema,
		hf:        hf,
	}, nil
}

func (v *localTxnMut) Volume() volumes.Volume {
	return newLocalVolume(v.localSys, v.volid)
}

func (v *localTxnMut) MaxSize() int {
	return int(v.volParams.MaxSize)
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

	// produce a final set of links
	var links linkSet
	if contSch, ok := ltx.schema.(schema.Container); ok {
		links = make(linkSet)
		var root []byte
		if err := ltx.localSys.load(ltx.volid, ltx.mvid, &root); err != nil {
			return err
		}
		// this is necessary because we have the lock exclusively, but we need to
		// present the transaction as a read-only store to ReadLinks.
		// None of the public methods will work while we are holding the lock exclusively.
		// linter complains about the lock being copied, but it's okay because we zero
		// the lock on the line below.
		//nolint:copylocks // see above
		ltx2 := *ltx
		ltx2.mu = sync.RWMutex{}
		var store schema.RO = volumes.NewUnsaltedStore(&ltx2)
		if err := contSch.ReadLinks(ctx, store, root, links); err != nil {
			return err
		}
	}
	if ltx.txParams.GC {
		if err := ltx.localSys.gc(ctx, ltx.volid, ltx.mvid); err != nil {
			return err
		}
	}
	if err := ltx.localSys.commit(ltx.volid, ltx.mvid, links, ltx.allowedLinks); err != nil {
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
	if err := v.localSys.abortMut(v.volid, v.mvid); err != nil {
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
	return lvSave(v.localSys.db, v.volid, v.mvid, root)
}

func (v *localTxnMut) Load(ctx context.Context, dst *[]byte) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return v.localSys.load(v.volid, v.mvid, dst)
}

func (v *localTxnMut) Delete(ctx context.Context, cids []blobcache.CID) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return v.localSys.deleteBlob(v.volid, v.mvid, cids)
}

func (v *localTxnMut) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	unlock, err := v.checkFinished()
	if err != nil {
		return blobcache.CID{}, err
	}
	defer unlock()
	if len(data) > int(v.volParams.MaxSize) {
		return blobcache.CID{}, cadata.ErrTooLarge
	}
	salt := opts.Salt
	if salt != nil && !v.volParams.Salted {
		return blobcache.CID{}, blobcache.ErrCannotSalt{}
	}
	cid := v.Hash(salt, data)
	if err := v.localSys.postBlob(ctx, v.volid, v.mvid, cid, salt, data); err != nil {
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
	return v.localSys.getBlob(v.volid, v.mvid, cid, buf)
}

func (v *localTxnMut) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	unlock, err := v.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return v.localSys.blobExists(v.volid, v.mvid, cids, dst)
}

func (v *localTxnMut) AllowLink(ctx context.Context, subvol blobcache.Handle) error {
	if _, ok := v.schema.(schema.Container); !ok {
		return fmt.Errorf("schema %T for volume %s is not a container", v.schema, oidFromLocalID(v.volid))
	}
	subvolOID, subvolRights := v.localSys.hsys.Resolve(subvol)
	if subvolRights == 0 {
		return blobcache.ErrInvalidHandle{Handle: subvol}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.allowedLinks == nil {
		v.allowedLinks = make(map[blobcache.OID]blobcache.ActionSet)
	}
	v.allowedLinks[subvolOID] |= subvolRights
	return nil
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
	return v.localSys.visit(v.volid, v.mvid, cids)
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
	return v.localSys.isVisited(v.volid, v.mvid, cids, dst)
}

var _ volumes.Tx = &localTxnRO{}

// localTxnRO is a read-only transaction on a local volume.
type localTxnRO struct {
	sys       *localSystem
	volid     LocalVolumeID
	sp        *pebble.Snapshot
	volParams blobcache.VolumeParams

	// activeTxns is the set of active transactions for the volume.
	mu         sync.RWMutex
	closed     bool
	activeTxns map[pdb.MVTag]struct{}
}

func newLocalTxnRO(sys *localSystem, volid LocalVolumeID, sp *pebble.Snapshot, volParams blobcache.VolumeParams) *localTxnRO {
	return &localTxnRO{
		sys:       sys,
		volid:     volid,
		sp:        sp,
		volParams: volParams,
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
	mvr, closer, err := pdb.MVGet(v.sp, tid_LOCAL_VOLUME_CELLS, v.volid.Marshal(nil), activeTxns)
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
		exists, err := volumeBlobExists(v.sp, v.volid, cid, activeTxns)
		if err != nil {
			return err
		}
		dst[i] = exists
	}
	return nil
}

func (v *localTxnRO) AllowLink(ctx context.Context, subvol blobcache.Handle) error {
	return blobcache.ErrTxReadOnly{Op: "AllowLink"}
}

func (v *localTxnRO) MaxSize() int {
	return int(v.volParams.MaxSize)
}

func (v *localTxnRO) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.volParams.HashAlgo.HashFunc()(salt, data)
}

func (v *localTxnRO) Volume() volumes.Volume {
	return newLocalVolume(v.sys, v.volid)
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
		if err := v.sys.txSys.readActive(v.sp, activeTxns); err != nil {
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

// excludeExcluding returns a function that returns an excluding function but adds a special case for the given mvid.
func excludeExcluding(excluding func(pdb.MVTag) bool, mvid pdb.MVTag) func(pdb.MVTag) bool {
	return func(x pdb.MVTag) bool {
		return x != mvid && excluding(x)
	}
}
