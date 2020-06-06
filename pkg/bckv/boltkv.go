package bckv

import (
	"bytes"

	bolt "go.etcd.io/bbolt"
)

var _ KV = &BoltKV{}

type BoltKV struct {
	db         *bolt.DB
	bucketPath [][]byte
	capacity   uint64
}

func NewBoltKV(db *bolt.DB, capacity uint64) *BoltKV {
	return &BoltKV{
		db:       db,
		capacity: capacity,
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
		if uint64(b.Stats().KeyN) >= kv.capacity {
			return ErrFull
		}
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

func (kv *BoltKV) Bucket(p string) KV {
	kv2 := &BoltKV{
		db:         kv.db,
		bucketPath: append(kv.bucketPath, []byte(p)),
	}
	return kv2
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
	return kv.capacity
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
		for _, bname := range kv.bucketPath {
			b2, err := b.CreateBucketIfNotExists(bname)
			if err != nil {
				panic(err)
			}
			b = b2
		}
	} else {
		for _, bname := range kv.bucketPath {
			b2 := b.Bucket(bname)
			if b2 == nil {
				return nil
			}
			b = b2
		}
	}
	return b.(*bolt.Bucket)
}
