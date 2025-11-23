// Package hydra implements a Mergeable Schema
package hydra

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

type Root []blobcache.CID

func (r *Root) Sort() {
	slices.SortFunc(*r, func(a, b blobcache.CID) int {
		return a.Compare(b)
	})
	*r = slices.CompactFunc(*r, func(a, b blobcache.CID) bool {
		return a == b
	})
}

func (r Root) Marshal(out []byte) []byte {
	slices.SortFunc(r, func(a, b blobcache.CID) int {
		return a.Compare(b)
	})
	for _, cid := range r {
		out = append(out, cid[:]...)
	}
	return out
}

func (r *Root) Unmarshal(data []byte) error {
	if len(data)%blobcache.CIDSize != 0 {
		return fmt.Errorf("root must be a multiple of the CID size. HAVE: %d", len(data))
	}
	var r2 []blobcache.CID
	for len(data) > 0 {
		cid := blobcache.CID(data[:blobcache.CIDSize])
		data = data[blobcache.CIDSize:]
		r2 = append(r2, cid)
	}
	*r = r2
	return nil
}

func ParseRoot(x []byte) (*Root, error) {
	var r Root
	if err := r.Unmarshal(x); err != nil {
		return nil, err
	}
	return &r, nil
}

var (
	_ schema.Schema      = &Schema{}
	_ schema.Constructor = Constructor
)

type Schema struct {
	X schema.Schema
}

type Params struct {
	X schema.Spec `json:"x"`
}

func Constructor(paramsData json.RawMessage, mkSchema schema.Factory) (schema.Schema, error) {
	var params Params
	if len(paramsData) > 0 {
		if err := params.X.Unmarshal(paramsData); err != nil {
			return nil, err
		}
	}
	x, err := mkSchema(params.X)
	if err != nil {
		return nil, err
	}
	return &Schema{
		X: x,
	}, nil
}

func (sch *Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	var root Root
	if err := root.Unmarshal(change.NextCell); err != nil {
		return err
	}
	var prevCID blobcache.CID
	for i, cid := range root {
		var exists [1]bool
		if err := change.NextStore.Exists(ctx, []blobcache.CID{cid}, exists[:]); err != nil {
			return err
		}
		if !exists[0] {
			return fmt.Errorf("hydra: head %d does not exist", i)
		}
		if prevCID.Compare(cid) >= 0 {
			return fmt.Errorf("heads are not sorted")
		}
		prevCID = cid
	}
	return nil
}

func (sch *Schema) Merge(ctx context.Context, stores []schema.RO, srcs [][]byte, dst *[]byte) error {
	var ret Root
	for _, src := range srcs {
		var r2 Root
		if err := r2.Unmarshal(src); err != nil {
			return err
		}
		ret = append(ret, r2...)
	}
	*dst = ret.Marshal((*dst)[:0])
	return nil
}

const SchemaName = "hydra"

func init() {
	bclocal.AddDefaultSchema(SchemaName, Constructor)
}
