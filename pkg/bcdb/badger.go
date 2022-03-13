package bcdb

import (
	"context"

	"github.com/brendoncarroll/go-state"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

type Badger struct {
	db *badger.DB
}

func NewBadgerMemory() DB {
	opts := badger.DefaultOptions("").
		WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return Badger{db: db}
}

func NewBadger(dir string) (DB, error) {
	opts := badger.LSMOnlyOptions(dir).
		WithCompression(options.None).
		WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return Badger{db: db}, nil
}

func (db Badger) Update(_ context.Context, fn func(Tx) error) error {
	return db.db.Update(func(tx *badger.Txn) error {
		return fn(badgerTx{tx})
	})
}

func (db Badger) View(_ context.Context, fn func(Tx) error) error {
	return db.db.Update(func(tx *badger.Txn) error {
		return fn(badgerTx{tx})
	})
}

type badgerTx struct {
	tx *badger.Txn
}

func (tx badgerTx) Put(key, value []byte) error {
	return tx.tx.Set(key, value)
}

func (tx badgerTx) Get(key []byte) ([]byte, error) {
	item, err := tx.tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy([]byte{})
}

func (tx badgerTx) Delete(key []byte) error {
	return tx.tx.Delete(key)
}

func (tx badgerTx) ForEach(span state.ByteSpan, fn func(key, value []byte) error) error {
	iter := tx.tx.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(span.Begin); iter.Valid(); iter.Next() {
		item := iter.Item()
		if span.AllLt(item.Key()) {
			break
		}
		if err := item.Value(func(value []byte) error {
			return fn(item.Key(), value)
		}); err != nil {
			return err
		}
	}
	return nil
}
