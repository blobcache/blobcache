package bcfuse

import (
	"context"

	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/migrations"
	"github.com/jmoiron/sqlx"
)

func SetupDB(ctx context.Context, db *sqlx.DB) error {
	return dbutil.DoTx(ctx, db, func(tx *sqlx.Tx) error {
		return migrations.EnsureAll(tx, migs)
	})
}

var migs = []migrations.Migration{
	{
		RowID: 1,
		Name:  "create_extents_table",
		SQLText: `
		CREATE TABLE extents (
			id TEXT NOT NULL,
			start INTEGER NOT NULL,
			"end" INTEGER NOT NULL,
			data BLOB NOT NULL,
			PRIMARY KEY (id, start)
		) WITHOUT ROWID, STRICT`,
	},
}
