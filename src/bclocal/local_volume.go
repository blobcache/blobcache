package bclocal

import (
	"context"
	"database/sql"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/volumes"
	"github.com/jmoiron/sqlx"
	"lukechampine.com/blake3"
)

// LocalVolumeID uniquely identifies a local volume.
type LocalVolumeID int64

// LocalTxnID uniquely identifies a transaction on a local volume.
// It is the rowid of the local_txns table.
type LocalTxnID int64

// localVolumeRow is a row in the local_volumes table.
type localVolumeRow struct {
	// RowID is the primary key.
	// Do not set this in calls to insertLocalVolume.
	RowID LocalVolumeID `db:"rowid"`
	OID   blobcache.OID `db:"oid"`
	// Base is the transaction_id below which all transactions have been successfully applied.
	Base LocalTxnID `db:"base"`
}

// createLocalVolume creates a new local volume.
func createLocalVolume(tx *sqlx.Tx, oid blobcache.OID) (LocalVolumeID, error) {
	var ret LocalVolumeID
	if err := tx.Get(&ret, `INSERT INTO local_volumes (oid) VALUES (?) RETURNING rowid`, oid); err != nil {
		return 0, err
	}
	// insert an empty root for the volume.
	if _, err := tx.Exec(`INSERT INTO local_vol_roots (vol_id, txn_id, root) VALUES (?, ?, ?)`, ret, 0, []byte{}); err != nil {
		return 0, err
	}
	return ret, nil
}

// dropLocalVolume deletes a local volume.
func dropLocalVolume(tx *sqlx.Tx, oid blobcache.OID) error {
	_, err := tx.Exec(`DELETE FROM local_volumes WHERE oid = ?`, oid)
	if err != nil {
		return err
	}
	return nil
}

func getLocalVolumeByOID(tx *sqlx.Tx, oid blobcache.OID) (*localVolumeRow, error) {
	var ret localVolumeRow
	if err := tx.Get(&ret, "SELECT rowid, oid, base FROM local_volumes WHERE oid = ?", oid); err != nil {
		if err == sql.ErrNoRows {
			return nil, blobcache.ErrNotFound{ID: oid}
		}
		return nil, err
	}
	return &ret, nil
}

// localTxnRow is a row in the local_txns table.
type localTxnRow struct {
	RowID  LocalTxnID    `db:"rowid"`
	Base   LocalTxnID    `db:"base"`
	VolID  LocalVolumeID `db:"volume"`
	Mutate bool          `db:"mutate"`
}

func volumeHasMutatingTx(tx *sqlx.Tx, volID LocalVolumeID) (bool, error) {
	var ret bool
	if err := tx.Get(&ret, `SELECT EXISTS (
		SELECT 1 FROM local_txns
		WHERE volume = ? AND mutate = TRUE
	)`, volID); err != nil {
		return false, err
	}
	return ret, nil
}

var _ volumes.Volume = &localVolume{}

type localVolume struct {
	db      *sqlx.DB
	oid     blobcache.OID
	resolve func(blobcache.Handle) (*volumes.Link, error)
}

func newLocalVolume(db *sqlx.DB, oid blobcache.OID, resolve func(blobcache.Handle) (*volumes.Link, error)) *localVolume {
	return &localVolume{
		db:      db,
		oid:     oid,
		resolve: resolve,
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
		if err = dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
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
	return newLocalTxn(ctx, v.db, *ltxid, volInfo, v.resolve)
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
func txnSetRoot(tx *sqlx.Tx, volID LocalVolumeID, txid LocalTxnID, root []byte) error {
	if root == nil {
		root = []byte{}
	}
	if _, err := tx.Exec(`INSERT INTO local_vol_roots (vol_id, txn_id, root) VALUES (?, ?, ?)`, volID, txid, root); err != nil {
		return err
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
	db          *sqlx.DB
	localTxnRow localTxnRow
	volInfo     blobcache.VolumeInfo
	resolve     func(blobcache.Handle) (*volumes.Link, error)

	subvols []volumes.Link
}

// newLocalTxn creates a localTxn.
// It does not change the database state.
// the caller should have already created the transaction at txid, and volInfo.
func newLocalTxn(ctx context.Context, db *sqlx.DB, txid LocalTxnID, volInfo *blobcache.VolumeInfo, resolve func(blobcache.Handle) (*volumes.Link, error)) (*localTxn, error) {
	txRow, err := dbutil.DoTx1(ctx, db, func(tx *sqlx.Tx) (*localTxnRow, error) {
		return getLocalTxn(tx, txid)
	})
	if err != nil {
		return nil, err
	}
	return &localTxn{
		db:          db,
		localTxnRow: *txRow,
		volInfo:     *volInfo,
		resolve:     resolve,
	}, nil
}

func (v *localTxn) Volume() volumes.Volume {
	return &localVolume{
		db:  v.db,
		oid: v.volInfo.ID,
	}
}

func (v *localTxn) Commit(ctx context.Context, root []byte) error {
	if !v.localTxnRow.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	return dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		return txnCommit(tx, v.localTxnRow.VolID, v.localTxnRow.RowID, root)
	})
}

func (v *localTxn) Abort(ctx context.Context) error {
	return dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		return dropLocalTxn(tx, v.localTxnRow.RowID)
	})
}

func (v *localTxn) Load(ctx context.Context, dst *[]byte) error {
	return dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		return txnLoadRoot(tx, v.localTxnRow.VolID, v.localTxnRow.Base, v.localTxnRow.RowID, dst)
	})
}

func (v *localTxn) Delete(ctx context.Context, cid blobcache.CID) error {
	return dbutil.DoTx(ctx, v.db, func(tx *sqlx.Tx) error {
		return deleteBlob(tx, v.localTxnRow.VolID, v.localTxnRow.RowID, cid)
	})
}

func (v *localTxn) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	if salt != nil && !v.volInfo.Salted {
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
	return dbutil.DoTx1(ctx, v.db, func(tx *sqlx.Tx) (int, error) {
		return readBlob(tx, v.localTxnRow.VolID, v.localTxnRow.Base, v.localTxnRow.RowID, cid, buf)
	})
}

func (v *localTxn) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	return dbutil.DoTx1(ctx, v.db, func(tx *sqlx.Tx) (bool, error) {
		exists, err := txnContainsBlob(tx, v.localTxnRow.VolID, v.localTxnRow.Base, v.localTxnRow.RowID, cid)
		if err != nil {
			return false, err
		}
		// if the blob exists, then we need to add it to the tx store.
		if exists {
			if err := addBlob(tx, v.localTxnRow.VolID, v.localTxnRow.RowID, cid); err != nil {
				return false, err
			}
		}
		return exists, nil
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
	link, err := v.resolve(subvol)
	if err != nil {
		return err
	}
	v.subvols = append(v.subvols, *link)
	return nil
}
