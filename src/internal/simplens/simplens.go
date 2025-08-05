// Package simplens provides a simple namespace implementation.
// All entries are stored in the root of the volume.
package simplens

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
)

type Namespace struct {
	Volume volumes.Volume
}

func (ns Namespace) GetEntry(ctx context.Context, name string) (*blobcache.Entry, error) {
	nsVol, err := ns.Volume.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer nsVol.Abort(ctx)
	nstx := Tx{Tx: nsVol}
	return nstx.GetEntry(ctx, name)
}

func (ns Namespace) PutEntry(ctx context.Context, x blobcache.Entry) error {
	tx, err := ns.Volume.BeginTx(ctx, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	nstx := Tx{Tx: tx}
	return nstx.PutEntry(ctx, x)
}

func (ns Namespace) ListEntries(ctx context.Context) ([]blobcache.Entry, error) {
	tx, err := ns.Volume.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	nstx := Tx{Tx: tx}
	return nstx.ListEntries(ctx)
}

func (ns Namespace) DeleteEntry(ctx context.Context, name string) error {
	tx, err := ns.Volume.BeginTx(ctx, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	nstx := Tx{Tx: tx}
	return nstx.DeleteEntry(ctx, name)
}

// Tx wraps a Tx to provide a namespace view.
type Tx struct {
	Tx volumes.Tx
}

func (ns Tx) loadEntries(ctx context.Context) ([]blobcache.Entry, error) {
	var root []byte
	if err := ns.Tx.Load(ctx, &root); err != nil {
		return nil, err
	}
	ents, err := decodeNamespace(root)
	if err != nil {
		return nil, err
	}
	return ents, nil
}

func (ns Tx) saveEntries(ctx context.Context, ents []blobcache.Entry) error {
	root, err := encodeNamespace(ents)
	if err != nil {
		return err
	}
	return ns.Tx.Commit(ctx, root)
}

func (ns Tx) GetEntry(ctx context.Context, name string) (*blobcache.Entry, error) {
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return nil, err
	}
	idx, found := slices.BinarySearchFunc(ents, name, func(e blobcache.Entry, name string) int {
		return strings.Compare(e.Name, name)
	})
	if !found {
		return nil, nil
	}
	return &ents[idx], nil
}

func (ns Tx) PutEntry(ctx context.Context, ent blobcache.Entry) error {
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return err
	}
	idx, found := slices.BinarySearchFunc(ents, ent.Name, func(e blobcache.Entry, name string) int {
		return strings.Compare(e.Name, name)
	})
	if found {
		ents[idx] = ent
	} else {
		ents = slices.Insert(ents, idx, ent)
	}
	return ns.saveEntries(ctx, ents)
}

func (ns Tx) DeleteEntry(ctx context.Context, name string) error {
	var root []byte
	if err := ns.Tx.Load(ctx, &root); err != nil {
		return err
	}
	ents, err := decodeNamespace(root)
	if err != nil {
		return err
	}
	ents = slices.DeleteFunc(ents, func(e blobcache.Entry) bool {
		return e.Name == name
	})
	root, err = encodeNamespace(ents)
	if err != nil {
		return err
	}
	return ns.Tx.Commit(ctx, root)
}

func (ns Tx) ListEntries(ctx context.Context) ([]blobcache.Entry, error) {
	return ns.loadEntries(ctx)
}

func (ns Tx) ListNames(ctx context.Context) ([]string, error) {
	ents, err := ns.loadEntries(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(ents))
	for i, ent := range ents {
		names[i] = ent.Name
	}
	return names, nil
}

func encodeNamespace(ents []blobcache.Entry) ([]byte, error) {
	slices.SortFunc(ents, func(a, b blobcache.Entry) int {
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

func decodeNamespace(buf []byte) ([]blobcache.Entry, error) {
	dec := json.NewDecoder(bytes.NewReader(buf))
	var ents []blobcache.Entry
	for {
		var ent blobcache.Entry
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
