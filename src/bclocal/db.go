package bclocal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"blobcache.io/blobcache/src/bclocal/internal/dbmig"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/jmoiron/sqlx"
)

func SetupDB(ctx context.Context, db *sqlx.DB) error {
	migs := dbmig.ListMigrations()
	for _, mig := range migs {
		if _, err := db.ExecContext(ctx, mig); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", mig, err)
		}
	}
	return nil
}

func createObject(tx *sqlx.Tx) (*blobcache.OID, error) {
	oid := blobcache.NewOID()
	return &oid, insertObject(tx, oid, time.Now())
}

func insertObject(tx *sqlx.Tx, oid blobcache.OID, createdAt time.Time) error {
	_, err := tx.Exec("INSERT INTO objects (id, created_at) VALUES (?, ?)", oid, createdAt.Unix())
	if err != nil {
		return err
	}
	return nil
}

func dropObject(tx *sqlx.Tx, oid blobcache.OID) error {
	// drop all handles
	if _, err := tx.Exec("DELETE FROM handles WHERE target = ?", oid); err != nil {
		return err
	}
	// drop the object
	_, err := tx.Exec("DELETE FROM objects WHERE id = ?", oid)
	if err != nil {
		return err
	}
	return nil
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
	ID      blobcache.OID `db:"id"`
	VolID   blobcache.OID `db:"volume_id"`
	StoreID int64         `db:"store_id"`
	Mutate  bool          `db:"mutate"`
}

func getTx(tx *sqlx.Tx, txid blobcache.OID) (*txRow, error) {
	var t txRow
	if err := tx.Get(&t, "SELECT id, volume_id, store_id, mutate FROM txns WHERE id = ?", txid); err != nil {
		return nil, err
	}
	return &t, nil
}

// insertHandle inserts a handle into the database.
func insertHandle(tx *sqlx.Tx, handle blobcache.Handle, expiresAt *time.Time) error {
	var expiresAt2 sql.NullInt64
	if expiresAt != nil {
		expiresAt2.Int64 = expiresAt.Unix()
		expiresAt2.Valid = true
	}
	k := handleKey(handle)
	_, err := tx.Exec("INSERT INTO handles (k, target, created_at) VALUES (?, ?, ?)", k[:], handle.OID, expiresAt2)
	if err != nil {
		return err
	}
	return nil
}

// forEachWithoutHandle calls f for each object that does not have any handles pointing to it.
func forEachWithoutHandle(tx *sqlx.Tx, f func(oid blobcache.OID) error) error {
	rows, err := tx.Query("SELECT id FROM objects WHERE id NOT IN (SELECT target FROM handles)")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var oid blobcache.OID
		if err := rows.Scan(&oid); err != nil {
			return err
		}
		if err := f(oid); err != nil {
			return err
		}
	}
	return rows.Err()
}
