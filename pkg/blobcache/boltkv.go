package blobcache

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

type boltKV struct {
	db       *bolt.DB
	bucket   []byte
	capacity uint64
}

func NewBoltKV(db *bolt.DB, bucket []byte, capacity uint64) (*boltKV, error) {
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
	return &boltKV{db: db, bucket: bucket, capacity: capacity}, nil
}

func (kv *boltKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	var data []byte
	err := kv.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(kv.bucket)
		value := b.Get(key)
		data = append([]byte{}, value...)
		return nil
	})
	return data, err
}

func (kv *boltKV) Put(ctx context.Context, key, value []byte) error {
	err := kv.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(kv.bucket)
		if uint64(b.Stats().KeyN) >= kv.capacity {
			return ErrCacheFull
		}
		return b.Put(key, value)
	})
	return err
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

func (kv *boltKV) SizeTotal() uint64 {
	return kv.capacity
}

func (kv *boltKV) SizeUsed() uint64 {
	var size uint64
	err := kv.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(kv.bucket)
		size = uint64(b.Stats().KeyN)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return size
}
