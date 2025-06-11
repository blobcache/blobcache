package bclocal

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/bclocal/internal/dbmig"
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
