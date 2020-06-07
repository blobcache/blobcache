package bcstate

import (
	"bytes"
	"math"

	bolt "go.etcd.io/bbolt"
)

var _ DB = &BoltDB{}

type BoltDB struct {
	db *bolt.DB
}

func NewBoltDB(db *bolt.DB) *BoltDB {
	return &BoltDB{db: db}
}

func (db *BoltDB) Bucket(p string) KV {
	return newBoltKV(db.db, p)
}

var _ KV = &BoltKV{}

type BoltKV struct {
	db         *bolt.DB
	bucketName []byte
}

func newBoltKV(db *bolt.DB, bucketName string) *BoltKV {
	return &BoltKV{
		db:         db,
		bucketName: []byte(bucketName),
	}
}

func (kv *BoltKV) GetF(key []byte, f func([]byte) error) error {
	return kv.db.View(func(tx *bolt.Tx) error {
		b := kv.selectBucket(tx)
		if b == nil {
			return nil
		}
		value := b.Get(key)
		return f(value)
	})
}

func (kv *BoltKV) Put(key, value []byte) error {
	err := kv.db.Update(func(tx *bolt.Tx) error {
		b := kv.selectBucket(tx)
		return b.Put(key, value)
	})
	return err
}

func (kv *BoltKV) Delete(key []byte) error {
	err := kv.db.Update(func(tx *bolt.Tx) error {
		b := kv.selectBucket(tx)
		return b.Delete(key)
	})
	return err
}

func (kv *BoltKV) ForEach(start, end []byte, fn func(k, v []byte) error) error {
	err := kv.db.View(func(tx *bolt.Tx) error {
		b := kv.selectBucket(tx)

		c := b.Cursor()
		for k, v := c.Seek(start); k != nil; k, v = c.Next() {
			if bytes.Compare(k, end) >= 0 {
				break
			}
			if err := fn(k, v); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (kv *BoltKV) SizeTotal() uint64 {
	return uint64(math.MaxInt64)
}

func (kv *BoltKV) SizeUsed() uint64 {
	var size uint64
	err := kv.db.View(func(tx *bolt.Tx) error {
		b := kv.selectBucket(tx)
		if b != nil {
			size = uint64(b.Stats().KeyN)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return size
}

func (kv *BoltKV) selectBucket(tx *bolt.Tx) *bolt.Bucket {
	type hasBucket interface {
		CreateBucketIfNotExists([]byte) (*bolt.Bucket, error)
		Bucket([]byte) *bolt.Bucket
	}
	var b hasBucket = tx

	if tx.Writable() {
		b2, err := b.CreateBucketIfNotExists(kv.bucketName)
		if err != nil {
			panic(err)
		}
		b = b2
	} else {
		b2 := b.Bucket(kv.bucketName)
		if b2 == nil {
			return nil
		}
		b = b2
	}
	return b.(*bolt.Bucket)
}
