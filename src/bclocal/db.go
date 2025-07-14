package bclocal

import (
	"context"
	"time"

	"blobcache.io/blobcache/src/bclocal/internal/dbmig"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/migrations"
	"github.com/jmoiron/sqlx"
)

// SetupDB idempotently applies all migrations to the database.
func SetupDB(ctx context.Context, db *sqlx.DB) error {
	migs := dbmig.ListMigrations()
	return dbutil.DoTx(ctx, db, func(tx *sqlx.Tx) error {
		return migrations.EnsureAll(tx, migs)
	})
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
	// drop the object
	_, err := tx.Exec("DELETE FROM objects WHERE id = ?", oid)
	if err != nil {
		return err
	}
	return nil
}
