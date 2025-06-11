package dbutil

import (
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
