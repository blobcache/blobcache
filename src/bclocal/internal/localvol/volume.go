package localvol

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/bccore"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/schema"
)

var _ backend.Volume = &Volume{}

type Volume struct {
	sys    *System
	lvid   ID
	params blobcache.VolumeConfig
}

func newLocalVolume(sys *System, lvid ID, params blobcache.VolumeConfig) *Volume {
	return &Volume{
		sys:    sys,
		lvid:   lvid,
		params: params,
	}
}

func (v *Volume) Key() ID {
	return v.lvid
}

func (v *Volume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	return fmt.Errorf("Await not implemented")
}

func (v *Volume) BeginTx(ctx context.Context, tp blobcache.TxParams) (backend.Tx, error) {
	return v.sys.beginTx(ctx, v, tp)
}

func (v *Volume) Down(ctx context.Context) error {
	return nil
}

func (v *Volume) AccessSubVolume(ctx context.Context, lt blobcache.LinkToken) (blobcache.ActionSet, error) {
	links := backend.LinkSet{}
	if err := v.sys.readLinksFrom(0, v.lvid, links); err != nil {
		return 0, err
	}
	h := lt.GetID(v.params.HashAlgo)
	if _, exists := links[h]; exists {
		return lt.Rights, nil
	}
	return 0, nil
}

func (v *Volume) ReadLinks(ctx context.Context, dst backend.LinkSet) error {
	return v.sys.readLinksFrom(0, v.lvid, dst)
}

func (v *Volume) GetBackend() blobcache.VolumeBackend[blobcache.OID] {
	return blobcache.VolumeBackend[blobcache.OID]{
		Local: blobcache.VolumeBackend_LocalFromConfig(v.params),
	}
}

func (v *Volume) GetParams() blobcache.VolumeConfig {
	return v.params
}

var _ backend.Tx = &localTxnMut{}

// localTxnMut is a mutating transaction on a local volume.
type localTxnMut struct {
	localSys *System
	vol      *Volume
	mvid     pdb.MVTag
	txParams blobcache.TxParams
	schema   schema.Schema

	ha blobcache.HashAlgo

	// mu protects the finished and links fields.
	// mu must be taken exclusively to {Commit, Abort, AllowLink}
	// mu must be taken in read mode for all other operations {Save, Delete, Post, Get, Exists}.
	// checkFinished is the most convenient way to make sure the transaction is not finished, during an operation.
	mu sync.RWMutex
	// finished is set to true when the transaction is finished.
	finished     bool
	links        backend.LinkSet
	visitedLinks map[[32]byte]struct{}
}

// newLocalTxn creates a localTxn.
// It does not change the database state.
// the caller should have already created the transaction at txid, and volInfo.
func newLocalTxn(localSys *System, vol *Volume, mvid pdb.MVTag, txParams blobcache.TxParams, schema schema.Schema) (*localTxnMut, error) {
	links := make(backend.LinkSet)
	if err := localSys.readVolumeLinks(localSys.db, mvid, vol.lvid, links); err != nil {
		return nil, err
	}
	ha := vol.params.HashAlgo
	return &localTxnMut{
		localSys: localSys,
		vol:      vol,
		mvid:     mvid,
		txParams: txParams,
		schema:   schema,
		ha:       ha,
		links:    links,
	}, nil
}

func (txn *localTxnMut) Volume() backend.Volume {
	return txn.vol
}

func (txn *localTxnMut) Params() blobcache.TxParams {
	return txn.txParams
}

func (txn *localTxnMut) MaxSize() int {
	return int(txn.vol.params.MaxSize)
}

func (txn *localTxnMut) Hash(data []byte) blobcache.CID {
	return txn.ha.Hash(data)
}

func (txn *localTxnMut) HashAlgo() blobcache.HashAlgo {
	return txn.ha
}

func (txn *localTxnMut) checkFinished() (func(), error) {
	txn.mu.RLock()
	if txn.finished {
		return nil, blobcache.ErrTxDone{}
	}
	return txn.mu.RUnlock, nil
}

func (txn *localTxnMut) Commit(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.finished {
		return blobcache.ErrTxDone{}
	}

	if txn.txParams.GCBlobs {
		if err := txn.localSys.gc(ctx, txn.vol.lvid, txn.mvid); err != nil {
			return err
		}
	}
	if txn.txParams.GCLinks {
		// filter out the links that are not visited
		for h := range txn.links {
			delete(txn.links, h)
		}
	}
	if err := txn.localSys.commit(txn.vol.lvid, txn.mvid, txn.links); err != nil {
		return err
	}
	txn.finished = true
	return nil
}

func (txn *localTxnMut) Abort(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.finished {
		return nil
	}
	if err := txn.localSys.abortMut(txn.vol.lvid, txn.mvid); err != nil {
		return err
	}
	txn.finished = true
	return nil
}

func (txn *localTxnMut) Save(ctx context.Context, root []byte) error {
	unlock, err := txn.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	if len(root) > txn.MaxSize()/2 {
		return fmt.Errorf("root cannot be more than half the max blob size. %d", len(root))
	}
	return lvSave(txn.localSys.db, txn.vol.lvid, txn.mvid, root)
}

func (txn *localTxnMut) Load(ctx context.Context, dst *[]byte) error {
	unlock, err := txn.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return txn.localSys.load(txn.vol.lvid, txn.mvid, dst)
}

func (txn *localTxnMut) Delete(ctx context.Context, cids []blobcache.CID) error {
	unlock, err := txn.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return txn.localSys.deleteBlob(txn.vol.lvid, txn.mvid, cids)
}

func (txn *localTxnMut) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	unlock, err := txn.checkFinished()
	if err != nil {
		return blobcache.CID{}, err
	}
	defer unlock()
	if len(data) > int(txn.vol.params.MaxSize) {
		return blobcache.CID{}, blobcache.ErrTooLarge{BlobSize: len(data), MaxSize: int(txn.vol.params.MaxSize)}
	}
	salt := opts.Salt
	if salt != nil && !txn.vol.params.Salted {
		return blobcache.CID{}, blobcache.ErrCannotSalt{}
	}
	var cid blobcache.CID
	if salt == nil {
		cid = txn.Hash(data)
	} else {
		cid = txn.HashAlgo().KeyedHash(salt, data)
	}
	if err := txn.localSys.postBlob(ctx, txn.vol.lvid, txn.mvid, cid, salt, data); err != nil {
		return blobcache.CID{}, err
	}
	return cid, nil
}

func (txn *localTxnMut) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	unlock, err := txn.checkFinished()
	if err != nil {
		return 0, err
	}
	defer unlock()
	return txn.localSys.getBlob(txn.vol.lvid, txn.mvid, cid, buf)
}

func (txn *localTxnMut) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	unlock, err := txn.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	return txn.localSys.blobExists(txn.vol.lvid, txn.mvid, cids, dst)
}

func (txn *localTxnMut) Visit(ctx context.Context, cids []blobcache.CID) error {
	unlock, err := txn.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	if !txn.txParams.GCBlobs {
		return blobcache.ErrTxNotGC{Op: "Visit"}
	}
	return txn.localSys.visit(txn.vol.lvid, txn.mvid, cids)
}

func (txn *localTxnMut) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	unlock, err := txn.checkFinished()
	if err != nil {
		return err
	}
	defer unlock()
	if !txn.txParams.GCBlobs {
		return blobcache.ErrTxNotGC{Op: "IsVisited"}
	}
	return txn.localSys.isVisited(txn.vol.lvid, txn.mvid, cids, dst)
}

func (txn *localTxnMut) Link(ctx context.Context, svoid blobcache.OID, rights blobcache.ActionSet, targetVol bccore.AnyObject) (*blobcache.LinkToken, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.finished {
		return nil, blobcache.ErrTxDone{}
	}
	ltok := blobcache.LinkToken{
		Target: svoid,
		Rights: rights,
	}
	if _, err := rand.Read(ltok.Secret[:]); err != nil {
		return nil, err
	}
	h := txn.hashLinkToken(ltok)
	txn.links[h] = ltok.Target
	return &ltok, nil
}

func (txn *localTxnMut) Unlink(ctx context.Context, targets []blobcache.LinkTokenID) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.finished {
		return blobcache.ErrTxDone{}
	}
	for _, target := range targets {
		delete(txn.links, [32]byte(target))
	}
	return nil
}

func (txn *localTxnMut) VisitLinks(ctx context.Context, targets []blobcache.LinkTokenID) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.finished {
		return blobcache.ErrTxDone{}
	}
	if !txn.txParams.GCBlobs {
		return blobcache.ErrTxNotGC{Op: "VisitLinks"}
	}
	if txn.visitedLinks == nil {
		txn.visitedLinks = make(map[[32]byte]struct{})
	}
	for _, target := range targets {
		txn.visitedLinks[[32]byte(target)] = struct{}{}
	}
	return nil
}

func (txn *localTxnMut) hashLinkToken(lt blobcache.LinkToken) [32]byte {
	return lt.GetID(txn.ha)
}
