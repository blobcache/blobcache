package statetrace

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/schema"
)

const SchemaName = "blobcache/statetrace"

func init() {
	schemareg.AddDefaultSchema(SchemaName, Constructor)
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
	if len(params) > 0 {
		if err := json.Unmarshal(params, &spec); err != nil {
			return nil, err
		}
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
					Prev: schema.Value{
						Cell:  prev,
						Store: s,
					},
					Next: schema.Value{
						Cell:  next,
						Store: s,
					},
				})
			},
		},
	}, nil
}

func (sch *Schema[T]) ValidateChange(ctx context.Context, change schema.Change) error {
	next, err := sch.Mach.Parse(change.Next.Cell)
	if err != nil {
		return err
	}
	if len(change.Prev.Cell) > 0 {
		prev, err := sch.Mach.Parse(change.Prev.Cell)
		if err != nil {
			return err
		}
		return sch.Mach.Validate(ctx, change.Next.Store, prev, next)
	}
	return sch.ValidateState(ctx, change.Next.Store, next)
}

func (sch *Schema[T]) ValidateState(ctx context.Context, s schema.RO, root Root[T]) error {
	return nil
}

type opaque []byte

func (o opaque) Marshal(out []byte) []byte {
	return append(out, o...)
}

func parseOpaque(data []byte) (opaque, error) {
	return opaque(data), nil
}
