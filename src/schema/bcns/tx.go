package bcns

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
)

// Tx is a transaction on a namespace
type Tx struct {
	schema Namespace
	cell   []byte
	sro    bcsdk.RO
	srw    bcsdk.RW
	lnk    Linker
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
		lnk:    tx,
	}, nil
}

func NewTxRO(schema Namespace, s bcsdk.RO, cell []byte) Tx {
	return Tx{
		schema: schema,
		cell:   cell,
		sro:    s,
	}
}

func NewTx(schema Namespace, s bcsdk.RWD, lnk Linker, cell []byte) Tx {
	return Tx{
		schema: schema,
		cell:   cell,
		sro:    s,
		srw:    s,
	}
}

func (tx *Tx) List(ctx context.Context) ([]Entry, error) {
	return tx.schema.NSList(ctx, tx.sro, tx.cell)
}

func (tx *Tx) Get(ctx context.Context, name string, dst *Entry) (bool, error) {
	return tx.schema.NSGet(ctx, tx.sro, tx.cell, name, dst)
}

func (tx *Tx) Put(ctx context.Context, ent Entry) error {
	if err := CheckName(ent.Name); err != nil {
		return err
	}
	next, err := tx.schema.NSPut(ctx, tx.srw, tx.cell, ent)
	if err != nil {
		return err
	}
	tx.cell = append(tx.cell[:0], next...)
	return nil
}

// CreateAt creates a new entry at name only if there is no exising entry
func (tx *Tx) CreateAt(ctx context.Context, name string, target blobcache.Handle, mask blobcache.ActionSet) (Entry, error) {
	var entry Entry
	exists, err := tx.Get(ctx, name, &entry)
	if err != nil {
		return Entry{}, err
	}
	if exists {
		// TODO: error already exists
		return Entry{}, fmt.Errorf("ns: entry already exists at %s", name)
	}
	lt, err := tx.lnk.Link(ctx, target, mask)
	if err != nil {
		return Entry{}, err
	}
	ent := Entry{
		Name:   name,
		Target: target.OID,
		Rights: lt.Rights,
		Secret: lt.Secret,
	}
	if err := tx.Put(ctx, ent); err != nil {
		return ent, err
	}
	return ent, nil
}

type Linker interface {
	bcsdk.Linker
	bcsdk.Unlinker
}
