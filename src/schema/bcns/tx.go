package bcns

import (
	"context"

	"blobcache.io/blobcache/src/bcsdk"
)

// Tx is a transaction on a namespace
type Tx struct {
	schema Namespace
	cell   []byte
	sro    bcsdk.RO
	srw    bcsdk.RW
}

func NewFromTx(ctx context.Context, schema Namespace, tx *bcsdk.Tx) (Tx, error) {
	var cell []byte
	if err := tx.Load(ctx, &cell); err != nil {
		return Tx{}, err
	}
	return Tx{
		schema: schema,
		cell:   cell,
		sro:    tx,
		srw:    tx,
	}, nil
}

func NewTxRO(schema Namespace, s bcsdk.RO, cell []byte) Tx {
	return Tx{
		schema: schema,
		cell:   cell,
		sro:    s,
	}
}

func NewTx(schema Namespace, s bcsdk.RWD, cell []byte) Tx {
	return Tx{
		schema: schema,
		cell:   cell,
		sro:    s,
		srw:    s,
	}
}

func (tx Tx) List(ctx context.Context) ([]Entry, error) {
	return tx.schema.NSList(ctx, tx.sro, tx.cell)
}

func (tx Tx) Get(ctx context.Context, name string, dst *Entry) (bool, error) {
	return tx.schema.NSGet(ctx, tx.sro, tx.cell, name, dst)
}
