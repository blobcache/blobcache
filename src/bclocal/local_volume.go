package bclocal

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"

	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sbe"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
	"github.com/cockroachdb/pebble"
	"go.brendoncarroll.net/state/cadata"
)

// localSystem manages the local volumes and transactions on those volumes.
type localSystem struct {
	db        *pebble.DB
	blobDir   *os.Root
	getSchema func(blobcache.Schema) schema.Schema

	// txSys manages the transaction sequence number, and the set of active transactions.
	txSys txSystem

	// mutVol manages access to volumes
	// only one mutating transaction can access a volume at a time.
	// mutating transactions should hold one of these locks starting in beginTx, and release when {Commit, Abort} is called.
	mutVol mapOfLocks[blobcache.OID]
	// blobs prevents concurrent operations on blobs
	// only one {Post, Delete, Add} operation can have a lock at a time.
	// - Post could potentially increment the refCount, and ingest blob data.
	// - Delete could potentially decrement the refCount, and delete blob data.
	// - Add could potentially increment the refCount, but does not ingest blob data.
	blobs mapOfLocks[blobcache.CID]
}

func newLocalSystem(db *pebble.DB, blobDir *os.Root, getSchema func(blobcache.Schema) schema.Schema) localSystem {
	return localSystem{
		db:        db,
		blobDir:   blobDir,
		getSchema: getSchema,

		txSys: newTxSystem(db),
	}
}

func (ls *localSystem) beginTx(ctx context.Context, volID blobcache.OID, params blobcache.TxParams) (_ volumes.Tx, retErr error) {
	if !params.Mutate {
		sp := ls.db.NewSnapshot()
		volInfo, err := inspectVolume(sp, volID)
		if err != nil {
			return nil, err
		}
		txInfo := blobcache.TxInfo{
			// Leave ID empty for now.
			Volume:   volInfo.ID,
			MaxSize:  volInfo.MaxSize,
			HashAlgo: blobcache.HashAlgo(volInfo.HashAlgo),
			Params:   params,
		}
		return newLocalTxnRO(ls, sp, &txInfo, volInfo), nil
	}

	txid, err := ls.txSys.allocateTxID()
	if err != nil {
		return nil, err
	}
	// this could potentially block for a while, so do it first.
	if err := ls.mutVol.Lock(ctx, volID); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			ls.mutVol.Unlock(volID)
		}
	}()

	ba := ls.db.NewIndexedBatch()
	defer ba.Close()
	vinfo, err := inspectVolume(ba, volID)
	if err != nil {
		return nil, err
	}
	if vinfo == nil {
		return nil, blobcache.ErrNotFound{ID: volID}
	}
	txInfo := blobcache.TxInfo{
		Volume:   volID,
		MaxSize:  vinfo.MaxSize,
		HashAlgo: blobcache.HashAlgo(vinfo.HashAlgo),
		Params:   params,
	}
	if err := putLocalVolumeTxn(ba, volID, txid); err != nil {
		return nil, err
	}
	if err := ba.Commit(nil); err != nil {
		return nil, err
	}
	sch := ls.getSchema(vinfo.Schema)
	if sch == nil {
		return nil, fmt.Errorf("unknown schema %s", vinfo.Schema)
	}
	return newLocalTxn(ls, vinfo, &txInfo, txid, sch)
}

type linkSet = map[blobcache.OID]blobcache.ActionSet

// commit commits a local volume.
// links should be the actual links returned by the schema
// newlyAllowed should be the allowed links that were added to the volume during the transaction
func (s *localSystem) commit(volID blobcache.OID, mvid pdb.MVID, links linkSet, newlyAllowed linkSet) error {
	ba := s.db.NewIndexedBatch()
	defer ba.Close()

	prevLinks := make(linkSet)
	if err := readVolumeLinks(ba, volID, prevLinks); err != nil {
		return err
	}

	return ba.Commit(nil)
}

// postBlob adds a blob to a local volume.
func (s *localSystem) postBlob(volID blobcache.OID, mvid pdb.MVID, cid blobcache.CID, salt *blobcache.CID, data []byte) error {
	ba := s.db.NewIndexedBatch()
	defer ba.Close()
	// in the current implementation, there can only be one mutating transaction at a time.
	// so exclude nothing.
	excluding := func(mvid pdb.MVID) bool {
		return false
	}
	if exists, err := blobExists(s.db, volID, cid, excluding); err != nil {
		return err
	} else if exists {
		return nil
	}
	// add the blob if necessary.
	if yes, err := blobExists(ba, volID, cid, excluding); err != nil {
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

func (s *localSystem) handleToLink(h blobcache.Handle) (*schema.Link, error) {
	return nil, nil
}

func putLocalTxn(ba *pebble.Batch, volID blobcache.OID, txid pdb.MVID) error {
	return nil
}

// putLocalVolumeTxn adds an entry to the LOCAL_VOLUME_TXNS table.
func putLocalVolumeTxn(ba pdb.WO, volID blobcache.OID, txid pdb.MVID) error {
	k := pdb.TKey{TableID: tid_LOCAL_VOLUME_TXNS, Key: volID[:]}
	if err := ba.Set(k.Marshal(nil), sbe.Uint64Bytes(uint64(txid)), nil); err != nil {
		return err
	}
	return nil
}

// deleteVolumeTxn removes a volume transaction.
func deleteVolumeTxn(ba pdb.WO, volID blobcache.OID, txid pdb.MVID) error {
	k := pdb.TKey{TableID: tid_LOCAL_VOLUME_TXNS, Key: volID[:]}
	return ba.Delete(k.Marshal(nil), nil)
}

// blobExists returns true if the blob exists in the volume.
// This method checks the (volune, blob) association. See also: blobExistsMeta.
func blobExists(db pdb.RO, volID blobcache.OID, cid blobcache.CID, excluding func(pdb.MVID) bool) (bool, error) {
	mvr, closer, err := pdb.MVGet(db, tid_LOCAL_VOLUME_BLOBS, slices.Concat(volID[:], cid[:]), excluding)
	if err != nil {
		return false, err
	}
	defer closer.Close()
	if mvr == nil {
		return false, nil
	}
	return true, nil
}

// lvLoad loads the root data for a local volume.
func lvLoad(sp pdb.RO, volID blobcache.OID, mvid pdb.MVID, dst *[]byte) error {
	mvr, closer, err := pdb.MVGet(sp, tid_LOCAL_VOLUME_CELLS, volID[:], nil)
	if err != nil {
		return err
	}
	defer closer.Close()
	if mvr == nil {
		return nil
	}
	*dst = append((*dst)[:0], mvr.Value...)
	return nil
}

// lvSave saves the root data for a local volume.
// This performs a blind Set on the LOCAL_VOLUME_CELLS table.
func lvSave(w pdb.WO, volID blobcache.OID, mvid pdb.MVID, root []byte) error {
	k := pdb.MVKey{
		TableID: tid_LOCAL_VOLUME_CELLS,
		Key:     volID[:],
		Version: mvid,
	}
	return w.Set(k.Marshal(nil), root, nil)
}

var _ volumes.Volume = &localVolume{}

type localVolume struct {
	sys *localSystem
	oid blobcache.OID
}

func newLocalVolume(sys *localSystem, oid blobcache.OID) *localVolume {
	return &localVolume{
		sys: sys,
		oid: oid,
	}
}

func (v *localVolume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return fmt.Errorf("Await not implemented")
}

func (v *localVolume) BeginTx(ctx context.Context, tp blobcache.TxParams) (volumes.Tx, error) {
	return v.sys.beginTx(ctx, v.oid, tp)
}

var _ volumes.Tx = &localTxn{}

// localTxn is a transaction on a local volume.
type localTxn struct {
	localSys *localSystem
	mvid     pdb.MVID
	txInfo   *blobcache.TxInfo
	volInfo  *blobcache.VolumeInfo
	schema   schema.Schema

	mu           sync.Mutex
	allowedLinks map[blobcache.OID]blobcache.ActionSet
}

// newLocalTxn creates a localTxn.
// It does not change the database state.
// the caller should have already created the transaction at txid, and volInfo.
func newLocalTxn(localSys *localSystem, volInfo *blobcache.VolumeInfo, txInfo *blobcache.TxInfo, mvid pdb.MVID, schema schema.Schema) (*localTxn, error) {
	return &localTxn{
		localSys: localSys,
		mvid:     mvid,
		txInfo:   txInfo,
		volInfo:  volInfo,
		schema:   schema,
	}, nil
}

func (v *localTxn) Volume() volumes.Volume {
	return newLocalVolume(v.localSys, v.volInfo.ID)
}

func (ltx *localTxn) Commit(ctx context.Context) error {
	if !ltx.txInfo.Params.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	ltx.mu.Lock()
	defer ltx.mu.Unlock()

	// produce a final set of links
	var links linkSet
	if contSch, ok := ltx.schema.(schema.Container); ok {
		links = make(linkSet)
		var root []byte
		if err := ltx.Load(ctx, &root); err != nil {
			return err
		}
		if err := contSch.ReadLinks(ctx, volumes.NewUnsaltedStore(ltx), root, links); err != nil {
			return err
		}
	}
	return ltx.localSys.commit(ltx.volInfo.ID, ltx.mvid, links, ltx.allowedLinks)
}

func (v *localTxn) Abort(ctx context.Context) error {
	panic("not implemented")
}

func (v *localTxn) Save(ctx context.Context, root []byte) error {
	if !v.txInfo.Params.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	return lvSave(v.localSys.db, v.volInfo.ID, v.mvid, root)
}

func (v *localTxn) Load(ctx context.Context, dst *[]byte) error {
	return nil
}

func (v *localTxn) Delete(ctx context.Context, cids []blobcache.CID) error {
	return nil
}

func (v *localTxn) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	if salt != nil && !v.volInfo.Salted {
		return blobcache.CID{}, blobcache.ErrCannotSalt{}
	}
	return v.Hash(salt, data), nil
}

func (v *localTxn) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	return 0, nil
}

func (v *localTxn) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	panic("not implemented")
}

func (v *localTxn) MaxSize() int {
	return int(v.volInfo.MaxSize)
}

func (v *localTxn) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	hf := v.volInfo.HashAlgo.HashFunc()
	return hf(salt, data)
}

func (v *localTxn) Info() blobcache.VolumeInfo {
	return *v.volInfo
}

func (v *localTxn) AllowLink(ctx context.Context, subvol blobcache.Handle) error {
	if !v.txInfo.Params.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	if _, ok := v.schema.(schema.Container); !ok {
		return fmt.Errorf("schema %T for volume %s is not a container", v.schema, v.volInfo.ID)
	}
	link, err := v.localSys.handleToLink(subvol)
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

func (v *localTxn) Visit(ctx context.Context, cids []blobcache.CID) error {
	if !v.txInfo.Params.GC {
		return fmt.Errorf("Visit called on non-GC transaction")
	}
	return nil
}

func (v *localTxn) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	panic("not implemented")
}

var _ volumes.Tx = &localTxnRO{}

// localTxnRO is a read-only transaction on a local volume.
type localTxnRO struct {
	sys     *localSystem
	sp      *pebble.Snapshot
	txInfo  *blobcache.TxInfo
	volInfo *blobcache.VolumeInfo

	// activeTxns is the set of active transactions for the volume.
	mu         sync.Mutex
	activeTxns map[pdb.MVID]struct{}
}

func newLocalTxnRO(sys *localSystem, sp *pebble.Snapshot, txInfo *blobcache.TxInfo, volInfo *blobcache.VolumeInfo) *localTxnRO {
	return &localTxnRO{
		sys:     sys,
		sp:      sp,
		txInfo:  txInfo,
		volInfo: volInfo,
	}
}

func (v *localTxnRO) Abort(ctx context.Context) error {
	return v.sp.Close()
}

func (v *localTxnRO) Commit(ctx context.Context) error {
	return blobcache.ErrTxReadOnly{Op: "Commit"}
}

func (v *localTxnRO) Load(ctx context.Context, dst *[]byte) error {
	activeTxns, err := v.getExcluded()
	if err != nil {
		return err
	}
	mvr, closer, err := pdb.MVGet(v.sp, tid_LOCAL_VOLUME_CELLS, v.volInfo.ID[:], activeTxns)
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

func (v *localTxnRO) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	return blobcache.CID{}, blobcache.ErrTxReadOnly{Op: "Post"}
}

func (v *localTxnRO) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	var exists [1]bool
	if err := v.Exists(ctx, []blobcache.CID{cid}, exists[:]); err != nil {
		return 0, err
	} else if !exists[0] {
		return 0, cadata.ErrNotFound{Key: cid}
	}
	return readBlobData(v.sp, cid, buf)
}

func (v *localTxnRO) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	activeTxns, err := v.getExcluded()
	if err != nil {
		return err
	}
	for i, cid := range cids {
		exists, err := blobExists(v.sp, v.volInfo.ID, cid, activeTxns)
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
	return int(v.volInfo.MaxSize)
}

func (v *localTxnRO) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	return v.volInfo.HashAlgo.HashFunc()(salt, data)
}

func (v *localTxnRO) Info() blobcache.VolumeInfo {
	return *v.volInfo
}

func (v *localTxnRO) Volume() volumes.Volume {
	return newLocalVolume(v.sys, v.volInfo.ID)
}

func (v *localTxnRO) Visit(ctx context.Context, cids []blobcache.CID) error {
	return fmt.Errorf("Visit called on non-GC transaction")
}

func (v *localTxnRO) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return fmt.Errorf("IsVisited called on non-GC transaction")
}

func (v *localTxnRO) getExcluded() (func(pdb.MVID) bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.activeTxns == nil {
		activeTxns := make(map[pdb.MVID]struct{})
		if err := v.sys.txSys.readActive(v.sp, activeTxns); err != nil {
			return nil, err
		}
		v.activeTxns = activeTxns
	}
	return v.isExcluded, nil
}

// isExcluded returns true if the given transaction is excluded from the snapshot.
// Do not call this directly, use getExcluded instead.
func (v *localTxnRO) isExcluded(mvid pdb.MVID) bool {
	_, ok := v.activeTxns[mvid]
	return ok
}
