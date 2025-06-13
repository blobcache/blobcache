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
