package ledger

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

const SchemaName = "blobcache/ledger"

func init() {
	bclocal.AddDefaultSchema(SchemaName, Constructor)
}

var _ schema.Schema = &Schema[opaque]{}

type Schema[T Marshaler] struct {
	Mach Machine[T]
}

type Spec struct {
	// HashAlgo is the hash algorithm to use for the ledger.
	HashAlgo blobcache.HashAlgo `json:"hash_algo"`
	// X is the inner schema.
	X blobcache.SchemaSpec `json:"x"`
}

var _ schema.Constructor = Constructor

func Constructor(params json.RawMessage, mkSchema schema.Factory) (schema.Schema, error) {
	var spec Spec
	if err := json.Unmarshal(params, &spec); err != nil {
		return nil, err
	}
	inner, err := mkSchema(spec.X)
	if err != nil {
		return nil, err
	}
	return &Schema[opaque]{
		Mach: Machine[opaque]{
			HashAlgo:   spec.HashAlgo,
			ParseState: parseOpaque,
			Verify: func(ctx context.Context, s schema.RO, prev, next opaque) error {
				return inner.ValidateChange(ctx, schema.Change{
					PrevCell:  prev,
					NextCell:  next,
					PrevStore: s,
					NextStore: s,
				})
			},
		},
	}, nil
}

func (sch *Schema[T]) ValidateChange(ctx context.Context, change schema.Change) error {
	prev, err := sch.Mach.Parse(change.PrevCell)
	if err != nil {
		return err
	}
	next, err := sch.Mach.Parse(change.NextCell)
	if err != nil {
		return err
	}
	return sch.Mach.Validate(ctx, change.NextStore, prev, next)
}

type opaque []byte

func (o opaque) Marshal(out []byte) []byte {
	return append(out, o...)
}

func parseOpaque(data []byte) (opaque, error) {
	return opaque(data), nil
}
