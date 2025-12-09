// Package jsonns provides a simple namespace implementation.
// All entries are stored in the root of the volume.
package jsonns

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/state/cadata"
)

const SchemaName blobcache.SchemaName = "blobcache/jsonns"

func init() {
	schemareg.AddDefaultSchema(SchemaName, Constructor)
}

type Entry = schema.NSEntry

var (
	_ schema.Schema    = &Schema{}
	_ schema.Namespace = &Schema{}
)

type Schema struct{}

func Constructor(_ json.RawMessage, _ schema.Factory) (schema.Schema, error) {
	return &Schema{}, nil
}

func (sch Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	_, err := sch.NSList(ctx, change.Next.Store, change.Next.Cell)
	if err != nil {
		return err
	}
	return nil
}

func (sch Schema) NSList(ctx context.Context, s schema.RO, root []byte) ([]Entry, error) {
	if len(root) == 0 {
		return nil, nil
	}
	if len(root) != blobcache.CIDSize {
		return nil, fmt.Errorf("root must be %d bytes. HAVE: %d", blobcache.CIDSize, len(root))
	}
	cid := cadata.IDFromBytes(root)
	buf := make([]byte, s.MaxSize())
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		return nil, err
	}
	ents, err := decodeNamespace(buf[:n])
	if err != nil {
		return nil, err
	}
	return ents, nil
}

func (sch Schema) NSGet(ctx context.Context, s schema.RO, root []byte, name string, dst *Entry) (bool, error) {
	ents, err := sch.NSList(ctx, s, root)
	if err != nil {
		return false, err
	}
	for _, ent := range ents {
		if ent.Name == name {
			*dst = ent
			return true, nil
		}
	}
	return false, nil
}

func (sch Schema) NSPut(ctx context.Context, s schema.RW, root []byte, ent Entry) ([]byte, error) {
	ents, err := sch.NSList(ctx, s, root)
	if err != nil {
		return nil, err
	}
	ents = append(ents, ent)
	return saveEnts(ctx, s, ents)
}

func (sch Schema) NSDelete(ctx context.Context, s schema.RW, root []byte, name string) ([]byte, error) {
	ents, err := sch.NSList(ctx, s, root)
	if err != nil {
		return nil, err
	}
	ents = slices.DeleteFunc(ents, func(ent Entry) bool { return ent.Name == name })
	return saveEnts(ctx, s, ents)
}

func saveEnts(ctx context.Context, s schema.WO, ents []Entry) ([]byte, error) {
	nsData, err := encodeNamespace(ents)
	if err != nil {
		return nil, err
	}
	cid, err := s.Post(ctx, nsData)
	if err != nil {
		return nil, err
	}
	return cid[:], nil
}

func (sch Schema) OpenAs(ctx context.Context, s schema.RO, root []byte, peer blobcache.PeerID) (blobcache.ActionSet, error) {
	// Don't modify the volume's permissions for any particular user.
	return blobcache.Action_ALL, nil
}

// Tx wraps a Tx to provide a namespace view.
type Tx struct {
	Tx *bcsdk.Tx
	// Root is the current root, set by calls to PutEntry and DeleteEntry
	Root   []byte
	Schema Schema
}

func (ns *Tx) loadEntries(ctx context.Context) ([]Entry, error) {
	if err := ns.Tx.Load(ctx, &ns.Root); err != nil {
		return nil, err
	}
	ents, err := ns.Schema.NSList(ctx, ns.Tx, ns.Root)
	if err != nil {
		return nil, err
	}
	return ents, nil
}

func (ns *Tx) saveEntries(ctx context.Context, ents []Entry) error {
	root, err := saveEnts(ctx, ns.Tx, ents)
	if err != nil {
		return err
	}
	ns.Root = root[:]
	return nil
}

func (ns *Tx) NSGet(ctx context.Context, name string) (*Entry, error) {
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return nil, err
	}
	idx, found := slices.BinarySearchFunc(ents, name, func(e Entry, name string) int {
		return strings.Compare(e.Name, name)
	})
	if !found {
		return nil, nil
	}
	return &ents[idx], nil
}

func (ns *Tx) NSPut(ctx context.Context, name string, target blobcache.OID, rights blobcache.ActionSet) error {
	ent := Entry{Name: name, Target: target, Rights: rights}
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return err
	}
	idx, found := slices.BinarySearchFunc(ents, ent.Name, func(e Entry, name string) int {
		return strings.Compare(e.Name, name)
	})
	if found {
		ents[idx] = ent
	} else {
		ents = slices.Insert(ents, idx, ent)
	}
	return ns.saveEntries(ctx, ents)
}

func (ns *Tx) NSDelete(ctx context.Context, name string) error {
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return err
	}
	idx, found := slices.BinarySearchFunc(ents, name, func(e Entry, name string) int {
		return strings.Compare(e.Name, name)
	})
	if !found {
		return nil
	}
	ents = slices.Delete(ents, idx, idx+1)
	return ns.saveEntries(ctx, ents)
}

func (ns *Tx) ListEntries(ctx context.Context) ([]Entry, error) {
	return ns.loadEntries(ctx)
}

func (ns *Tx) ListNames(ctx context.Context) ([]string, error) {
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return nil, err
	}
	names := slices2.Map(ents, func(e Entry) string {
		return e.Name
	})
	return names, nil
}

func (ns *Tx) Commit(ctx context.Context) error {
	if err := ns.Tx.Save(ctx, ns.Root); err != nil {
		return err
	}
	return ns.Tx.Commit(ctx)
}

// VisitAll visits the root blob and all the links to other volumes.
// If the underlying Tx is not a GC transaction, it will return an error (on the first call to Visit).
func (ns *Tx) VisitAll(ctx context.Context) error {
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return err
	}
	cid := cadata.IDFromBytes(ns.Root)
	if err := ns.Tx.Visit(ctx, []blobcache.CID{cid}); err != nil {
		return err
	}
	for _, ent := range ents {
		if err := ns.Tx.VisitLinks(ctx, []blobcache.OID{ent.Target}); err != nil {
			return err
		}
	}
	return nil
}

// GC performs garbage collection on the namespace.
func GC(ctx context.Context, svc blobcache.Service, volh blobcache.Handle) error {
	tx, err := bcsdk.BeginTx(ctx, svc, volh, blobcache.TxParams{GC: true, Modify: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	nstx := Tx{Tx: tx}

	if err := nstx.VisitAll(ctx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func encodeNamespace(ents []Entry) ([]byte, error) {
	slices.SortFunc(ents, func(a, b Entry) int {
		return strings.Compare(a.Name, b.Name)
	})
	ents = slices.CompactFunc(ents, func(a, b Entry) bool {
		return a.Name == b.Name
	})
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	for _, ent := range ents {
		if err := enc.Encode(ent); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeNamespace(buf []byte) ([]Entry, error) {
	dec := json.NewDecoder(bytes.NewReader(buf))
	var ents []Entry
	for {
		var ent Entry
		if err := dec.Decode(&ent); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		ents = append(ents, ent)
	}
	return ents, nil
}
