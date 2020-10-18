package bcstate

import (
	"bytes"
	"context"
	"math"

	bolt "go.etcd.io/bbolt"
)

var _ DB = &BoltDB{}

type BoltDB struct {
	db  *bolt.DB
	cap uint64
}

func NewBoltDB(db *bolt.DB, capacity uint64) *BoltDB {
	return &BoltDB{db: db, cap: capacity}
}

func (db *BoltDB) Bucket(p string) KV {
	return &boltKV{
		update: func(f func(tx *bolt.Tx) error) error {
			return db.db.Update(f)
		},
		view: func(f func(tx *bolt.Tx) error) error {
			return db.db.Update(f)
		},
		bucketName: []byte(p),
	}
}

func (kv *BoltDB) WriteTx(ctx context.Context, f func(db DB) error) error {
	return kv.db.Update(func(tx *bolt.Tx) error {
		return f(boltTx{tx})
	})
}

func (kv *BoltDB) ReadTx(ctx context.Context, f func(db DB) error) error {
	return kv.db.Update(func(tx *bolt.Tx) error {
		return f(boltTx{tx})
	})
}

type boltTx struct {
	tx *bolt.Tx
}

func (btx boltTx) Bucket(name string) KV {
	return &boltKV{
		update: func(f func(tx *bolt.Tx) error) error {
			return f(btx.tx)
		},
		view: func(f func(tx *bolt.Tx) error) error {
			return f(btx.tx)
		},
		bucketName: []byte(name),
	}
}

var _ KV = &boltKV{}

type boltKV struct {
	update     func(func(tx *bolt.Tx) error) error
	view       func(func(tx *bolt.Tx) error) error
	bucketName []byte
}

func (kv *boltKV) GetF(key []byte, f func([]byte) error) error {
	return kv.view(func(tx *bolt.Tx) error {
		b := kv.selectBucket(tx)
		if b == nil {
			return nil
		}
		value := b.Get(key)
		return f(value)
	})
}

func (kv *boltKV) Put(key, value []byte) error {
	err := kv.update(func(tx *bolt.Tx) error {
		b := kv.selectBucket(tx)
		return b.Put(key, value)
	})
	return err
}

func (kv *boltKV) Delete(key []byte) error {
	err := kv.update(func(tx *bolt.Tx) error {
		b := kv.selectBucket(tx)
		return b.Delete(key)
	})
	return err
}

func (kv *boltKV) ForEach(start, end []byte, fn func(k, v []byte) error) error {
	err := kv.view(func(tx *bolt.Tx) error {
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

func (kv *boltKV) NextSequence() (uint64, error) {
	var seq uint64
	err := kv.update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(kv.bucketName)
		if err != nil {
			return err
		}
		seq, err = b.NextSequence()
		return err
	})
	return seq, err
}

func (kv *boltKV) MaxCount() uint64 {
	return uint64(math.MaxInt64)
}

func (kv *boltKV) Count() uint64 {
	var size uint64
	err := kv.view(func(tx *bolt.Tx) error {
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

func (kv *boltKV) selectBucket(tx *bolt.Tx) *bolt.Bucket {
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
