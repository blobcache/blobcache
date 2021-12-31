package bcdb

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-state"
	"github.com/pkg/errors"
)

type prefixed struct {
	inner  DB
	prefix string
}

func NewPrefixed(db DB, prefix string) DB {
	return prefixed{
		inner:  db,
		prefix: prefix,
	}
}

func (db prefixed) Update(ctx context.Context, fn func(Tx) error) error {
	return db.inner.Update(ctx, func(tx Tx) error {
		return fn(prefixTx{inner: tx, prefix: db.prefix})
	})
}

func (db prefixed) View(ctx context.Context, fn func(Tx) error) error {
	return db.inner.Update(ctx, func(tx Tx) error {
		return fn(prefixTx{inner: tx, prefix: db.prefix})
	})
}

type prefixTx struct {
	inner  Tx
	prefix string
}

func (tx prefixTx) Put(key, value []byte) error {
	return tx.inner.Put(tx.getKey(key), value)
}

func (tx prefixTx) Get(key []byte) ([]byte, error) {
	return tx.inner.Get(tx.getKey(key))
}

func (tx prefixTx) Delete(key []byte) error {
	return tx.inner.Delete(tx.getKey(key))
}

func (tx prefixTx) ForEach(span state.ByteRange, fn func(key, value []byte) error) error {
	span2 := prefixSpan(span, []byte(tx.prefix))
	return tx.inner.ForEach(span2, func(key, value []byte) error {
		key2, err := removePrefix(key, []byte(tx.prefix))
		if err != nil {
			panic(err)
		}
		return fn(key2, value)
	})
}

func (tx prefixTx) getKey(x []byte) []byte {
	return append([]byte(tx.prefix), x...)
}

func prefixSpan(x state.ByteRange, prefix []byte) state.ByteRange {
	begin := append([]byte{}, prefix...)
	begin = append(begin, x.Begin...)

	end := append([]byte{}, prefix...)
	if x.End != nil {
		end = append(end, x.End...)
	}
	return state.ByteRange{
		Begin: begin,
		End:   end,
	}
}

func removePrefix(x, prefix []byte) ([]byte, error) {
	if !bytes.HasPrefix(x, prefix) {
		return nil, errors.Errorf("%q does not have prefix %q", x, prefix)
	}
	return x[len(prefix):], nil
}
