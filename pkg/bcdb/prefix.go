package bcdb

type prefixed struct {
	prefix string
	inner  DB
}

func NewPrefixed(db DB, prefix string) DB {
	return prefixed{
		db:     db,
		prefix: prefix,
	}
}

func (db prefixed) Update(fn func(Tx) error) error {
	return db.inner.Update(func(tx Tx) error {
		return fn(prefixTx{inner: tx, prefix: db.prefix})
	})
}

func (db prefixed) View(fn func(Tx) error) error {
	return db.inner.Update(func(tx Tx) error {
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
	return tx.inner.Get(key)
}

func (tx prefixTx) getKey(x []byte) []byte {
	return append([]byte(tx.inner), x...)
}
