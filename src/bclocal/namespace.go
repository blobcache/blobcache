package bclocal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
)

func nsLoad(ctx context.Context, vol volumes.Volume[[]byte]) ([]blobcache.Entry, error) {
	tx, err := vol.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	var buf []byte
	if err := tx.Load(ctx, &buf); err != nil {
		return nil, err
	}
	return decodeNamespace(buf)
}

func nsModify(ctx context.Context, vol volumes.Volume[[]byte], fn func(ents []blobcache.Entry) ([]blobcache.Entry, error)) error {
	tx, err := vol.BeginTx(ctx, blobcache.TxParams{Mutate: true})
	if err != nil {
		return err
	}
	var buf []byte
	if err := tx.Load(ctx, &buf); err != nil {
		return err
	}
	ents, err := decodeNamespace(buf)
	if err != nil {
		return err
	}
	ents, err = fn(ents)
	if err != nil {
		return err
	}
	buf, err = encodeNamespace(ents)
	if err != nil {
		return err
	}
	return tx.Commit(ctx, buf)
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

func allReferences(ents []blobcache.Entry) iter.Seq[blobcache.OID] {
	return func(yield func(blobcache.OID) bool) {
		for _, ent := range ents {
			if !yield(ent.Target) {
				return
			}
		}
	}
}
