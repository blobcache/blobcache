package bcstate

import (
	"context"
	"path"
)

type DB interface {
	Bucket(string) KV
}

type TxDB interface {
	DB
	WriteTx(context.Context, func(db DB) error) error
	ReadTx(context.Context, func(db DB) error) error
}

type PrefixedDB struct {
	Prefix string
	DB
}

func (db PrefixedDB) Bucket(p string) KV {
	p2 := path.Join(db.Prefix, p)
	return db.DB.Bucket(p2)
}

type PrefixedTxDB struct {
	Prefix string
	TxDB
}

func (tx PrefixedTxDB) WriteTx(ctx context.Context, f func(DB) error) error {
	return tx.WriteTx(ctx, func(db DB) error {
		return f(PrefixedDB{Prefix: tx.Prefix, DB: db})
	})
}

func (tx PrefixedTxDB) ReadTx(ctx context.Context, f func(db DB) error) error {
	return tx.ReadTx(ctx, func(db DB) error {
		return f(PrefixedDB{Prefix: tx.Prefix, DB: db})
	})
}

type MemDB struct {
	buckets map[string]*MemKV
}

func (db *MemDB) Bucket(p string) KV {
	if db.buckets == nil {
		db.buckets = make(map[string]*MemKV)
	}
	if _, exists := db.buckets[p]; !exists {
		db.buckets[p] = &MemKV{}
	}
	return db.buckets[p]
}
