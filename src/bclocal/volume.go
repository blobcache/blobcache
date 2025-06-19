package bclocal

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/volumes"
	"github.com/jmoiron/sqlx"
	"lukechampine.com/blake3"
)

func createVolume(tx *sqlx.Tx, info blobcache.VolumeInfo) (*blobcache.OID, error) {
	oid, err := createObject(tx)
	if err != nil {
		return nil, err
	}
	storeID, err := createStore(tx)
	if err != nil {
		return nil, err
	}
	backendJSON, err := json.Marshal(info.Backend)
	if err != nil {
		return nil, err
	}
	row := volumeRow{
		ID:       *oid,
		Root:     []byte{},
		StoreID:  storeID,
		HashAlgo: string(info.HashAlgo),
		MaxSize:  info.MaxSize,
		Backend:  backendJSON,
	}
	if err := insertVolume(tx, row); err != nil {
		return nil, err
	}
	return oid, nil
}

// ensureRootVolume creates the root volume if it does not exist.
func ensureRootVolume(tx *sqlx.Tx) error {
	rootOID := blobcache.OID{}
	if _, err := getVolume(tx, rootOID); err == nil {
		return nil
	} else if err != sql.ErrNoRows {
		return err
	}
	if err := insertObject(tx, rootOID, time.Now()); err != nil {
		return err
	}
	storeID, err := createStore(tx)
	if err != nil {
		return err
	}
	row := volumeRow{
		ID:       rootOID,
		Root:     []byte{},
		StoreID:  storeID,
		HashAlgo: string(blobcache.HashAlgo_BLAKE3_256),
		MaxSize:  1 << 22,
		Backend:  []byte("local"),
	}
	return insertVolume(tx, row)
}

func insertVolume(tx *sqlx.Tx, row volumeRow) error {
	_, err := tx.Exec(`INSERT INTO volumes (id, root, backend, hash_algo, max_size, store_id)
	    VALUES (?, ?, ?, ?, ?, ?)`, row.ID, row.Root, row.Backend, row.HashAlgo, row.MaxSize, row.StoreID)
	if err != nil {
		return err
	}
	return nil
}

type volumeRow struct {
	ID       blobcache.OID   `db:"id"`
	Root     []byte          `db:"root"`
	StoreID  StoreID         `db:"store_id"`
	HashAlgo string          `db:"hash_algo"`
	MaxSize  int64           `db:"max_size"`
	Backend  json.RawMessage `db:"backend"`
}

func getVolume(tx *sqlx.Tx, volID blobcache.OID) (*volumeRow, error) {
	var v volumeRow
	if err := tx.Get(&v, "SELECT id, root, store_id, hash_algo, max_size, backend FROM volumes WHERE id = ?", volID); err != nil {
		return nil, err
	}
	return &v, nil
}

// getVolumeRoot gets the root of a local volume and sets dst to it.
func getVolumeRoot(tx *sqlx.Tx, volID blobcache.OID, dst *[]byte) error {
	if err := tx.Get(dst, "SELECT root FROM volumes WHERE id = ?", volID); err != nil {
		return err
	}
	return nil
}

// setVolumeRoot sets the root of a local volume.
func setVolumeRoot(tx *sqlx.Tx, volID blobcache.OID, root []byte) error {
	if root == nil {
		root = []byte{}
	}
	_, err := tx.Exec("UPDATE volumes SET root = ? WHERE id = ?", root, volID)
	return err
}

// volumeHasActiveTx returns true if the volume has an in progress transaction.
func volumeHasActiveTx(tx *sqlx.Tx, volID blobcache.OID) (bool, error) {
	var exists bool
	if err := tx.Get(&exists, "SELECT EXISTS (SELECT 1 FROM txns WHERE volume_id = ?)", volID); err != nil {
		return false, err
	}
	return exists, nil
}

var _ volumes.Volume[[]byte] = &localVolume{}

type localVolume struct {
	db *sqlx.DB
	id blobcache.OID
}

func (v *localVolume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	panic("not implemented")
}

func (v *localVolume) BeginTx(ctx context.Context, spec blobcache.TxParams) (volumes.Tx[[]byte], error) {
	// loop until there is no active tx on the volume.
	var oid *blobcache.OID
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		var err error
		if oid, err = dbutil.DoTx1(ctx, v.db, func(tx *sqlx.Tx) (*blobcache.OID, error) {
			if spec.Mutate {
				if yes, err := volumeHasActiveTx(tx, v.id); err != nil {
					return nil, err
				} else if yes {
					return nil, nil
				}
			}
			return createTx(tx, v.id, spec.Mutate)
		}); err != nil {
			return nil, err
		}
		if oid != nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
		}
	}
	return newLocalVolumeTx(ctx, v.db, *oid)
}

// createTx creates a new transaction object in the database.
func createTx(tx *sqlx.Tx, volID blobcache.OID, mutate bool) (*blobcache.OID, error) {
	// if mutate is true then we need to create a new store
	// otherwise use the store from the volume
	var storeID StoreID
	if mutate {
		var err error
		storeID, err = createStore(tx)
		if err != nil {
			return nil, err
		}
	} else {
		if err := tx.Get(&storeID, "SELECT store_id FROM volumes WHERE id = ?", volID); err != nil {
			return nil, err
		}
	}
	txid := blobcache.NewOID()
	_, err := tx.Exec("INSERT INTO txns (id, volume_id, store_id, mutate, created_at) VALUES (?, ?, ?, ?, ?)", txid, volID, storeID, mutate, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	return &txid, nil
}

// dropTx drops the transaction's store, and deletes the transaction.
// no pending changes are applied.
func dropTx(tx *sqlx.Tx, txid blobcache.OID) error {
	// drop the tx store
	var storeID StoreID
	if err := tx.Get(&storeID, "SELECT store_id FROM txns WHERE id = ?", txid); err != nil {
		return err
	}
	if err := dropStore(tx, storeID); err != nil {
		return err
	}
	// Delete the transaction
	_, err := tx.Exec("DELETE FROM txns WHERE id = ?", txid)
	if err != nil {
		return err
	}
	return dropObject(tx, txid)
}

type txRow struct {
	ID       blobcache.OID `db:"id"`
	VolID    blobcache.OID `db:"volume_id"`
	StoreID  int64         `db:"store_id"`
	Mutate   bool          `db:"mutate"`
	IsSalted bool          `db:"is_salted"`
}

func getTx(tx *sqlx.Tx, txid blobcache.OID) (*txRow, error) {
	var t txRow
	// TODO: add is_salted to the query
	if err := tx.Get(&t, "SELECT id, volume_id, store_id, mutate FROM txns WHERE id = ?", txid); err != nil {
		return nil, err
	}
	return &t, nil
}

var _ volumes.Tx[[]byte] = &localVolumeTx{}

// localVolumeTx is a transaction on a local volume.
type localVolumeTx struct {
	db   *sqlx.DB
	txid blobcache.OID

	txRow txRow
}

// newLocalVolumeTx creates a localVolumeTx.
// It does not change the database state.
func newLocalVolumeTx(ctx context.Context, db *sqlx.DB, txid blobcache.OID) (*localVolumeTx, error) {
	txRow, err := dbutil.DoTx1(ctx, db, func(tx *sqlx.Tx) (*txRow, error) {
		return getTx(tx, txid)
	})
	if err != nil {
		return nil, err
	}
	return &localVolumeTx{db: db, txid: txid, txRow: *txRow}, nil
}

func (v *localVolumeTx) Commit(ctx context.Context, root []byte) error {
	return dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		volRow, err := getVolume(tx, v.txRow.VolID)
		if err != nil {
			return err
		}
		if err := mergeStores(tx, volRow.StoreID, []StoreID{v.txRow.StoreID}); err != nil {
			return err
		}
		if err := setVolumeRoot(tx, volRow.ID, root); err != nil {
			return err
		}
		return dropTx(tx, v.txid)
	})
}

func (v *localVolumeTx) Abort(ctx context.Context) error {
	return dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		return dropTx(tx, v.txid)
	})
}

func (v *localVolumeTx) Load(ctx context.Context, dst *[]byte) error {
	return dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		return getVolumeRoot(tx, v.txRow.VolID, dst)
	})
}

func (v *localVolumeTx) Delete(ctx context.Context, cid blobcache.CID) error {
	return dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		return deleteBlob(tx, v.txRow.StoreID, cid)
	})
}

func (v *localVolumeTx) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	if salt != nil && v.txRow.IsSalted {
		return blobcache.CID{}, blobcache.ErrCannotSalt{}
	}
	cid, err := dbutil.DoTx1(ctx, v.db, func(tx *sqlx.Tx) (*blobcache.CID, error) {
		// TODO: get hf from volume spec
		hf := func(data []byte) blobcache.CID {
			return blobcache.CID(blake3.Sum256(data))
		}
		cid := hf(data)
		if err := ensureBlob(tx, cid, nil, data); err != nil {
			return nil, err
		}
		if err := addBlob(tx, v.txRow.StoreID, cid); err != nil {
			return nil, err
		}
		return &cid, nil
	})
	if err != nil {
		return blobcache.CID{}, err
	}
	return *cid, nil
}

func (v *localVolumeTx) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	return dbutil.DoTx1(ctx, v.db, func(tx *sqlx.Tx) (int, error) {
		volRow, err := getVolume(tx, v.txRow.VolID)
		if err != nil {
			return 0, err
		}
		return readBlob(tx, []StoreID{v.txRow.StoreID, volRow.StoreID}, cid, buf)
	})
}

func (v *localVolumeTx) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	var exists bool
	err := dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		volRow, err := getVolume(tx, v.txRow.VolID)
		if err != nil {
			return err
		}
		var errExists error
		exists, errExists = storesContainsBlob(tx, []StoreID{v.txRow.StoreID, volRow.StoreID}, cid)
		if errExists != nil {
			return errExists
		}
		// if the blob exists, then we need to add it to the tx store.
		if exists {
			if err := addBlob(tx, v.txRow.StoreID, cid); err != nil {
				return err
			}
		}
		return nil
	})
	return exists, err
}

func (v *localVolumeTx) MaxSize() int {
	// TODO: change max size.
	return 1 << 21
}

func (v *localVolumeTx) Hash(data []byte) blobcache.CID {
	return blobcache.CID(blake3.Sum256(data))
}

func (v *localVolumeTx) Info() blobcache.VolumeInfo {
	return blobcache.VolumeInfo{
		ID: v.txRow.VolID,
	}
}
