package bcns

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

// View calls fn with a read-only transaction
func View(ctx context.Context, svc blobcache.Service, sch Namespace, volh blobcache.Handle, fn func(*Tx) error) error {
	tx, err := bcsdk.BeginTx(ctx, svc, volh, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	nstx, err := NewFromTx(ctx, sch, tx)
	if err != nil {
		return err
	}
	return fn(&nstx)
}

// Modify calls fn with a read-write transaction
func Modify(ctx context.Context, svc blobcache.Service, sch Namespace, volh blobcache.Handle, fn func(*Tx) error) error {
	tx, err := bcsdk.BeginTx(ctx, svc, volh, blobcache.TxParams{Modify: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	nstx, err := NewFromTx(ctx, sch, tx)
	if err != nil {
		return err
	}
	if err := fn(&nstx); err != nil {
		return err
	}
	if err := tx.Save(ctx, nstx.cell); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Tx is a transaction on a namespace
type Tx struct {
	schema Namespace
	cell   []byte
	sro    bcsdk.RO
	srw    bcsdk.RW
	lnk    Linker
	ha     blobcache.HashAlgo
}

func NewFromTx(ctx context.Context, schema Namespace, tx *bcsdk.Tx) (Tx, error) {
	var cell []byte
	if err := tx.Load(ctx, &cell); err != nil {
		return Tx{}, err
	}
	ha := tx.HashAlgo()
	return Tx{
		schema: schema,
		cell:   cell,
		sro:    tx,
		srw:    tx,
		lnk:    tx,
		ha:     ha,
	}, nil
}

func NewTxRO(schema Namespace, s bcsdk.RO, cell []byte) Tx {
	return Tx{
		schema: schema,
		cell:   cell,
		sro:    s,
	}
}

func NewTx(schema Namespace, ha blobcache.HashAlgo, s bcsdk.RWD, lnk Linker, cell []byte) Tx {
	return Tx{
		schema: schema,
		cell:   cell,
		sro:    s,
		srw:    s,
		lnk:    lnk,
		ha:     ha,
	}
}

func (tx *Tx) setCell(x []byte) {
	tx.cell = append(tx.cell[:0], x...)
}

func (tx *Tx) roCtx(ctx context.Context) schema.ROCtx {
	return schema.ROCtx{Context: ctx, Store: tx.sro, Cell: tx.cell}
}

func (tx *Tx) rwCtx(ctx context.Context) schema.RWCtx {
	return schema.RWCtx{Context: ctx, Store: tx.srw, Cell: tx.cell}
}

func (tx *Tx) Init(ctx context.Context) error {
	sch, ok := tx.schema.(schema.Initializer)
	if !ok {
		return fmt.Errorf("protocol does not support initialization")
	}
	if len(tx.cell) != 0 {
		return fmt.Errorf("cannot initialize namespace, there is already something in the volume")
	}
	data, err := sch.Init(ctx, tx.srw)
	if err != nil {
		return err
	}
	tx.setCell(data)
	return nil
}

func (tx *Tx) List(ctx context.Context) ([]Entry, error) {
	return tx.schema.NSList(tx.roCtx(ctx))
}

func (tx *Tx) Get(ctx context.Context, name string, dst *Entry) (bool, error) {
	return tx.schema.NSGet(tx.roCtx(ctx), name, dst)
}

func (tx *Tx) Put(ctx context.Context, name string, target blobcache.Handle, mask blobcache.ActionSet) error {
	if err := CheckName(name); err != nil {
		return err
	}
	lt, err := tx.lnk.Link(ctx, target, mask)
	if err != nil {
		return err
	}
	ent := Entry{
		Name:   name,
		Target: lt.Target,
		Rights: lt.Rights,
		Secret: lt.Secret,
	}
	next, err := tx.schema.NSPut(tx.rwCtx(ctx), ent)
	if err != nil {
		return err
	}
	tx.setCell(next)
	return nil
}

func (tx *Tx) Delete(ctx context.Context, name string) error {
	var ent Entry
	if found, err := tx.Get(ctx, name, &ent); err != nil {
		return err
	} else if !found {
		// no change needed
		return nil
	}
	root, err := tx.schema.NSDelete(tx.rwCtx(ctx), name)
	if err != nil {
		return err
	}
	ents, err := tx.schema.NSList(tx.roCtx(ctx))
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(ents, func(x Entry) bool {
		return x.Target == ent.Target
	}) {
		// if the target is not referenced by any other entry, unlink it
		ltokID := ent.LinkToken().GetID(tx.ha)
		if err := tx.lnk.Unlink(ctx, []blobcache.LinkID{ltokID}); err != nil {
			return err
		}
	}
	tx.setCell(root)
	return nil
}

// Create creates a new entry at name only if there is no exising entry
func (tx *Tx) Create(ctx context.Context, name string, target blobcache.Handle, mask blobcache.ActionSet) error {
	var entry Entry
	exists, err := tx.Get(ctx, name, &entry)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("ns: entry already exists at %s", name)
	}
	return tx.Put(ctx, name, target, mask)
}

// Match performs greedy prefix matching using / as possible split points.
// Match is the unit operation of path resolution.
// If no match is found ErrNoMatch is returned
// Match prefers entries with longer matching prefixes.
func (tx *Tx) Match(ctx context.Context, p string) (Entry, string, error) {
	p = strings.Trim(p, "/")
	name := p
	for name != "" {
		var ent Entry
		found, err := tx.Get(ctx, name, &ent)
		if err != nil {
			return Entry{}, "", err
		}
		if found {
			rem := strings.TrimPrefix(p, name)
			rem = strings.Trim(rem, "/")
			return ent, rem, nil
		}
		idx := strings.LastIndex(name, "/")
		if idx < 0 {
			return Entry{}, "", &ErrNoMatch{Name: name}
		}
		name = strings.Trim(name[:idx], "/")
	}
	return Entry{}, "", &ErrNoMatch{Name: name}
}

type ErrNoMatch struct {
	// Name is the name which could not be matched
	Name string
}

func (e *ErrNoMatch) Error() string {
	return fmt.Sprintf("lookup incomplete. could not find prefix of %s", e.Name)
}

func (tx *Tx) Move(ctx context.Context, oldName, newName string) error {
	var ent Entry
	found, err := tx.Get(ctx, oldName, &ent)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("ns: no entry found at %s", oldName)
	}
	existsAtNew, err := tx.Get(ctx, newName, new(Entry))
	if err != nil {
		return err
	}
	if existsAtNew {
		return fmt.Errorf("ns: entry already exists at %s", newName)
	}
	ent.Name = newName
	next, err := tx.schema.NSPut(tx.rwCtx(ctx), ent)
	if err != nil {
		return err
	}
	tx.setCell(next)
	return tx.Delete(ctx, oldName)
}

type Linker interface {
	bcsdk.Linker
	bcsdk.Unlinker
}
