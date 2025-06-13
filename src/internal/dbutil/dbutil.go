package dbutil

import (
	"context"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

func OpenDB(p string) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite", p)
	return db, err
}

func OpenMemory() *sqlx.DB {
	db, err := sqlx.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	return db
}

func DoTx(ctx context.Context, db *sqlx.DB, f func(tx *sqlx.Tx) error) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func DoTx1[T any](ctx context.Context, db *sqlx.DB, f func(tx *sqlx.Tx) (T, error)) (T, error) {
	var ret T
	if err := DoTx(ctx, db, func(tx *sqlx.Tx) error {
		var err error
		ret, err = f(tx)
		return err
	}); err != nil {
		return ret, err
	}
	return ret, nil
}
