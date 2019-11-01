package blobcache

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

type boltKV struct {
	db     *bolt.DB
	bucket []byte
}

func newBoltKV(db *bolt.DB, bucket []byte) (*boltKV, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	_, err = tx.CreateBucketIfNotExists(bucket)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &boltKV{db: db, bucket: bucket}, nil
}

func (kv *boltKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	tx, err := kv.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	buc := tx.Bucket(kv.bucket)
	return buc.Get(key), nil
}

func (kv *boltKV) Put(ctx context.Context, key, value []byte) error {
	tx, err := kv.db.Begin(true)
	if err != nil {
		return err
	}
	buc := tx.Bucket(kv.bucket)
	if err := buc.Put(key, value); err != nil {
		return err
	}
	return tx.Commit()
}

func (kv *boltKV) Delete(ctx context.Context, key, value []byte) error {
	tx, err := kv.db.Begin(true)
	if err != nil {
		return err
	}
	buc := tx.Bucket(kv.bucket)
	if err := buc.Delete(key); err != nil {
		return err
	}
	return tx.Commit()
}
