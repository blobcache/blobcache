package localvol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"

	"blobcache.io/blobcache/src/bclocal/internal/blobman"
	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/internal/bcsys"
	"blobcache.io/blobcache/src/schema"
	"github.com/cockroachdb/pebble"
)

type Config struct {
	NoSync bool
}

type Params = bcsys.LVParams[ID]

var _ backend.VolumeSystem[Params, *Volume] = &System{}

// System manages the local volumes and transactions on those volumes.
type System struct {
	cfg       Config
	db        *pebble.DB
	blobs     *blobman.Store
	getSchema schema.Factory

	// txSys manages the transaction sequence number, and the set of active transactions.
	txSys *pdb.TxSys

	// mutVol manages access to volumes
	// only one mutating transaction can access a volume at a time.
	// mutating transactions should hold one of these locks starting in beginTx, and release when {Commit, Abort} is called.
	mutVol mapOfLocks[ID]
	// blobLocks prevents concurrent operations on blobLocks
	// only one {Post, Delete, Add} operation can have a lock at a time.
	// - Post could potentially increment the refCount, and ingest blob data.
	// - Delete could potentially decrement the refCount, and delete blob data.
	// - Add could potentially increment the refCount, but does not ingest blob data.
	blobLocks mapOfLocks[blobcache.CID]
}

type Env struct {
	DB       *pebble.DB
	BlobDir  *os.Root
	TxSys    *pdb.TxSys
	MkSchema func(blobcache.SchemaSpec) (schema.Schema, error)
}

func New(cfg Config, env Env) System {
	return System{
		cfg:       cfg,
		db:        env.DB,
		blobs:     blobman.New(env.BlobDir),
		getSchema: env.MkSchema,

		txSys: env.TxSys,
	}
}

func (ls *System) GenerateLocalID() (ID, error) {
	n, err := ls.txSys.AllocateTxID()
	if err != nil {
		return 0, err
	}
	return ID(n), nil
}

func (ls *System) VolumeUp(ctx context.Context, params Params) (*Volume, error) {
	return ls.UpNoErr(params), nil
}

// UpNoErr is like Up, but it always succeeds.
// Up implements volume.System.Up, but this implementation
// never errors, and doesn't need the context.
func (ls *System) UpNoErr(params Params) *Volume {
	return newLocalVolume(ls, params.Key, params.Params)
}

func (ls *System) VolumeDrop(ctx context.Context, vol *Volume) error {
	id := vol.lvid
	if err := ls.mutVol.Lock(ctx, id); err != nil {
		return err
	}
	defer ls.mutVol.Unlock(id)

	// TODO:
	// 1. delete all blobs, decrementing refcounts
	// 2. delete cell data,
	return nil
}

// GCBlobs walks all of the blob reference counts, and deletes any blobs that have a reference count of 0.
func (ls *System) GCBlobs(ctx context.Context, lvid ID) error {
	iter, err := ls.db.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: pdb.TableLowerBound(dbtab.TID_BLOB_REF_COUNT),
		UpperBound: pdb.TableUpperBound(dbtab.TID_BLOB_REF_COUNT),
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
			if err := ls.blobLocks.Lock(ctx, cid); err != nil {
				return err
			}
			defer ls.blobLocks.Unlock(cid)
			return ls.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
				return ls.blobs.Delete(cid)
			})
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (ls *System) Flush() error {
	return ls.blobs.Flush()
}

func (ls *System) doRO(fn func(sp *pebble.Snapshot, ignore func(pdb.MVTag) bool) error) error {
	sp := ls.db.NewSnapshot()
	defer sp.Close()
	active := make(pdb.MVSet)
	if err := ls.txSys.ReadActive(sp, active); err != nil {
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
func (ls *System) doRW(fn func(ba *pebble.Batch, ignore func(pdb.MVTag) bool) error) error {
	ba := ls.db.NewIndexedBatch()
	defer ba.Close()
	active := make(pdb.MVSet)
	if err := ls.txSys.ReadActive(ba, active); err != nil {
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

func (ls *System) beginTx(ctx context.Context, vol *Volume, params blobcache.TxParams) (_ backend.Tx, retErr error) {
	if !params.Modify {
		sp := ls.db.NewSnapshot()
		return newLocalTxnRO(ls, vol, sp), nil
	}

	txid, err := ls.txSys.AllocateTxID()
	if err != nil {
		return nil, err
	}
	// this could potentially block for a while, so do it first.
	if err := ls.mutVol.Lock(ctx, vol.lvid); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			ls.mutVol.Unlock(vol.lvid)
		}
	}()

	// we don't use doRW here because, nothing we do accesses MVCC tables.
	ba := ls.db.NewIndexedBatch()
	defer ba.Close()
	if err := putLocalVolumeTxn(ba, vol.lvid, txid); err != nil {
		return nil, err
	}
	if err := ba.Commit(nil); err != nil {
		return nil, err
	}

	sch, err := ls.getSchema(vol.params.Schema)
	if err != nil {
		return nil, err
	}
	return newLocalTxn(ls, vol, txid, params, sch)
}

// abortMut aborts a mutating transaction.
func (s *System) abortMut(volID ID, mvid pdb.MVTag) error {
	if err := func() error {
		ba := s.db.NewIndexedBatch()
		defer ba.Close()
		yesActive, err := s.txSys.IsActive(ba, mvid)
		if err != nil {
			return err
		}
		if !yesActive {
			return nil
		}
		if err := s.txSys.Failure(ba, mvid); err != nil {
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
		if err := pdb.Undo(s.db, ba, dbtab.TID_LOCAL_VOLUME_BLOBS, volID.Marshal(nil), mvid); err != nil {
			return err
		}
		if err := pdb.Undo(s.db, ba, dbtab.TID_LOCAL_VOLUME_CELLS, volID.Marshal(nil), mvid); err != nil {
			return err
		}
		if err := s.txSys.RemoveFailed(ba, mvid); err != nil {
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
func (s *System) commit(volID ID, mvid pdb.MVTag, links backend.LinkSet) error {
	ba := s.db.NewIndexedBatch()
	defer ba.Close()

	if err := s.putVolumeLinks(ba, mvid, volID, links); err != nil {
		return err
	}
	if !s.cfg.NoSync {
		if err := s.blobs.Flush(); err != nil {
			return err
		}
	}
	if err := s.txSys.Success(ba, mvid); err != nil {
		return err
	}
	if err := ba.Commit(nil); err != nil {
		return err
	}

	s.mutVol.Unlock(volID)
	return nil
}

// gc walks the VOLUME_BLOBS table, and deletes any blobs that are not marked as visited.
func (s *System) gc(ctx context.Context, volID ID, mvid pdb.MVTag) error {
	return s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		iter := ba.NewIterWithContext(ctx, &pebble.IterOptions{
			LowerBound: pdb.TKey{TableID: dbtab.TID_LOCAL_VOLUME_BLOBS, Key: volID.Marshal(nil)}.Marshal(nil),
			UpperBound: pdb.TKey{TableID: dbtab.TID_LOCAL_VOLUME_BLOBS, Key: pdb.PrefixUpperBound(volID.Marshal(nil))}.Marshal(nil),
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
			curCID    [16]byte
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
			cidp := mvk.Key[8:]

			// New group
			if !curInit || !bytes.Equal(cidp, curCID[:]) {
				if err := flush(); err != nil {
					return err
				}
				copy(curCID[:], cidp)
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
func (s *System) postBlob(ctx context.Context, volID ID, mvid pdb.MVTag, cid blobcache.CID, salt *blobcache.CID, data []byte) error {
	// if the blob has already been added in the current transaction, then we don't need to do anything.
	vbFlags, rowExists, err := s.getVolumeBlob(s.db, volID, cid, mvid)
	if err != nil {
		return err
	}
	if rowExists && vbFlags&volumeBlobFlag_ADDED > 0 {
		// the blob has already been added in the current transaction.
		return nil
	}

	if err := s.blobLocks.Lock(ctx, cid); err != nil {
		return err
	}
	defer s.blobLocks.Unlock(cid)
	if err := s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		if yes, err := existsBlobMeta(ba, cid); err != nil {
			return err
		} else if !yes {
			// need to add the blob
			if _, err := s.blobs.Put(cid, data); err != nil {
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
func (s *System) getBlob(volID ID, mvid pdb.MVTag, cid blobcache.CID, buf []byte) (int, error) {
	var n int
	if err := s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		excluding2 := excludeExcluding(excluding, mvid)
		mvr, closer, err := pdb.MVGet(ba, dbtab.TID_LOCAL_VOLUME_BLOBS, slices.Concat(volID.Marshal(nil), cid[:16]), excluding2)
		if err != nil {
			return err
		}
		defer closer.Close()
		if mvr == nil {
			// blob is not in the volume, return not found
			return blobcache.ErrNotFound{Key: cid}
		}
		// read into buffer
		n, err = s.readBlobData(cid, buf)
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
func (s *System) deleteBlob(volID ID, mvid pdb.MVTag, cids []blobcache.CID) error {
	// delete does not update ref counts, and doesn't need to check if the blobs exists
	// so we don't need to acquire a lock on the blobs.
	// if there are concurrent Post or Add operations, then they will race to set the (volume, blob) row.
	// but that's the callers fault, and any order is valid.
	return s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		for _, cid := range cids {
			cidp := [16]byte(cid[:16])
			if err := tombVolumeBlob(ba, volID, mvid, cidp); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *System) blobExists(volID ID, mvid pdb.MVTag, cids []blobcache.CID, dst []bool) error {
	sn := s.db.NewSnapshot()
	defer sn.Close()
	active := make(pdb.MVSet)
	if err := s.txSys.ReadActive(sn, active); err != nil {
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
func (s *System) load(volID ID, mvid pdb.MVTag, dst *[]byte) error {
	return s.doRO(func(sp *pebble.Snapshot, ignoring func(pdb.MVTag) bool) error {
		ignoring2 := func(x pdb.MVTag) bool {
			return x != mvid && ignoring(x)
		}
		mvr, closer, err := pdb.MVGet(sp, dbtab.TID_LOCAL_VOLUME_CELLS, volID.Marshal(nil), ignoring2)
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
func (s *System) visit(volID ID, mvid pdb.MVTag, cids []blobcache.CID) error {
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

func (s *System) isVisited(volID ID, mvid pdb.MVTag, cids []blobcache.CID, dst []bool) error {
	return s.doRW(func(ba *pebble.Batch, excluding func(pdb.MVTag) bool) error {
		for i, cid := range cids {
			// we only have to check for a specific version.
			cidPrefix := cid[:16]
			k := pdb.MVKey{
				TableID: dbtab.TID_LOCAL_VOLUME_BLOBS,
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
func (s *System) getVolumeBlob(db pdb.RO, volID ID, cid blobcache.CID, mvid pdb.MVTag) (uint8, bool, error) {
	k := pdb.TKey{
		TableID: dbtab.TID_LOCAL_VOLUME_BLOBS,
		Key:     slices.Concat(volID.Marshal(nil), cid[:16], mvid.Marshal(nil)),
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

func (s *System) readBlobData(k blobcache.CID, buf []byte) (int, error) {
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

func (s *System) readLinksFrom(mvid pdb.MVTag, fromVol ID, dst backend.LinkSet) error {
	clear(dst)
	snp := s.db.NewSnapshot()
	defer snp.Close()
	return s.readVolumeLinks(snp, mvid, fromVol, dst)
}

// setVolumeBlob associates a volume with a blob according to the flags.
// setVolumeBlob requires an IndexedBatch because it may increment the ref count.
func setVolumeBlob(ba *pebble.Batch, volID ID, mvid pdb.MVTag, cid blobcache.CID, flags uint8) error {
	k := pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_BLOBS,
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
		if _, err := blobRefCountIncr(ba, cid, 1); err != nil {
			return err
		}
	}
	return nil
}

// unsetVolumeBlob writes an empty value to the LOCAL_VOLUME_BLOBS table.
// empty values are tombstones, which will eventually be cleaned up by the vacuum process.
func tombVolumeBlob(ba *pebble.Batch, volID ID, mvid pdb.MVTag, cidp [16]byte) error {
	k := pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_BLOBS,
		Key:     slices.Concat(volID.Marshal(nil), cidp[:]),
		Version: mvid,
	}
	return ba.Set(k.Marshal(nil), nil, nil)
}

// volumeBlobExists returns true if the blob exists in the volume.
// This method checks the (volune, blob) association. See also: blobExistsMeta.
func volumeBlobExists(db pdb.RO, volID ID, cid blobcache.CID, excluding func(pdb.MVTag) bool) (bool, error) {
	cidPrefix := cid[:16]
	mvr, closer, err := pdb.MVGet(db, dbtab.TID_LOCAL_VOLUME_BLOBS, slices.Concat(volID.Marshal(nil), cidPrefix), excluding)
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
func lvSave(w pdb.WO, volID ID, mvid pdb.MVTag, root []byte) error {
	k := pdb.MVKey{
		TableID: dbtab.TID_LOCAL_VOLUME_CELLS,
		Key:     volID.Marshal(nil),
		Version: mvid,
	}
	return w.Set(k.Marshal(nil), root, nil)
}

// excludeExcluding returns a function that returns an excluding function but adds a special case for the given mvid.
func excludeExcluding(excluding func(pdb.MVTag) bool, mvid pdb.MVTag) func(pdb.MVTag) bool {
	return func(x pdb.MVTag) bool {
		return x != mvid && excluding(x)
	}
}
