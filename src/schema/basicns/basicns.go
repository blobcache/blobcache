// Package basicns provides a simple namespace implementation.
// All entries are stored in the root of the volume.
package basicns

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/exp/slices2"
	"go.brendoncarroll.net/state/cadata"
)

func init() {
	bclocal.AddDefaultSchema(blobcache.Schema_BasicNS, Constructor)
}

type Entry struct {
	Name   string
	Target blobcache.OID
	Rights blobcache.ActionSet
}

func (ent *Entry) Link() schema.Link {
	return schema.Link{
		Target: ent.Target,
		Rights: ent.Rights,
	}
}

var _ schema.Container = &Schema{}

type Schema struct{}

func Constructor(_ json.RawMessage, _ schema.Factory) (schema.Schema, error) {
	return &Schema{}, nil
}

func (sch Schema) ValidateChange(ctx context.Context, s schema.RO, _, next []byte) error {
	_, err := sch.ListEntries(ctx, s.(cadata.Getter), next)
	if err != nil {
		return err
	}
	return nil
}

func (sch Schema) ListEntries(ctx context.Context, s cadata.Getter, root []byte) ([]Entry, error) {
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

func (sch Schema) ReadLinks(ctx context.Context, s schema.RO, root []byte, dst map[blobcache.OID]blobcache.ActionSet) error {
	ents, err := sch.ListEntries(ctx, s.(cadata.Getter), root)
	if err != nil {
		return err
	}
	for _, ent := range ents {
		dst[ent.Target] |= ent.Rights
	}
	return nil
}

// Tx wraps a Tx to provide a namespace view.
type Tx struct {
	Tx *blobcache.Tx
	// Root is the current root, set by calls to PutEntry and DeleteEntry
	Root   []byte
	Schema Schema
}

func (ns *Tx) loadEntries(ctx context.Context) ([]Entry, error) {
	if err := ns.Tx.Load(ctx, &ns.Root); err != nil {
		return nil, err
	}
	ents, err := ns.Schema.ListEntries(ctx, ns.Tx, ns.Root)
	if err != nil {
		return nil, err
	}
	return ents, nil
}

func (ns *Tx) saveEntries(ctx context.Context, ents []Entry) error {
	nsData, err := encodeNamespace(ents)
	if err != nil {
		return err
	}
	cid, err := ns.Tx.Post(ctx, nsData)
	if err != nil {
		return err
	}
	ns.Root = cid[:]
	return nil
}

func (ns *Tx) GetEntry(ctx context.Context, name string) (*Entry, error) {
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

func (ns *Tx) PutEntry(ctx context.Context, name string, target blobcache.OID, rights blobcache.ActionSet) error {
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

func (ns *Tx) DeleteEntry(ctx context.Context, name string) error {
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

func (ns Tx) ListEntries(ctx context.Context) ([]Entry, error) {
	return ns.loadEntries(ctx)
}

func (ns Tx) ListNames(ctx context.Context) ([]string, error) {
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return nil, err
	}
	names := slices2.Map(ents, func(e Entry) string {
		return e.Name
	})
	return names, nil
}

func (ns Tx) Commit(ctx context.Context) error {
	if err := ns.Tx.Save(ctx, ns.Root); err != nil {
		return err
	}
	return ns.Tx.Commit(ctx)
}

func encodeNamespace(ents []Entry) ([]byte, error) {
	slices.SortFunc(ents, func(a, b Entry) int {
		return strings.Compare(a.Name, b.Name)
	})
	for i := 0; i < len(ents)-1; i++ {
		if ents[i].Name == ents[i+1].Name {
			return nil, fmt.Errorf("duplicate name: %s", ents[i].Name)
		}
	}
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
