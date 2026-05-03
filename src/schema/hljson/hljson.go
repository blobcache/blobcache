package hljson

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/schema"
	hljson "blobcache.io/hljson"
)

const SchemaName = "blobcache/hljson"

func Unmarshal[T any](data []byte, dst *T) error {
	return hljson.Unmarshal(data, dst)
}

func Marshal[T any](x T) ([]byte, error) {
	return hljson.Marshal(x)
}

func isValid(x []byte) bool {
	var dst map[string]any
	err := hljson.Unmarshal(x, &dst)
	return err == nil
}

var _ schema.Schema = &Schema{}

type Schema struct {
}

func (sch *Schema) ValidateChange(ctx context.Context, ch schema.Change) error {
	if !isValid(ch.Next.Cell) {
		return fmt.Errorf("volume cell must contain valid HLJSON. HAVE: %q", ch.Next.Cell)
	}
	return checkIntegrity(ctx, ch.Prev.Store, ch.Next.Store, ch.Next.Cell, true)
}

func (sch *Schema) VisitAll(ctx context.Context, s schema.RO, data []byte, fn func(cid blobcache.CID, link blobcache.LinkToken)) error

// checkIntegrity recursively checks that the data structure is valid.
// Everything in prevs is assumed to be valid.
func checkIntegrity(ctx context.Context, prevs, nexts schema.RO, data []byte, allowNonJSON bool) error {
	ciditer, err := hljson.AllLinksBytes(data)
	if err != nil {
		if allowNonJSON {
			return nil
		} else {
			return err
		}
	}
	buf := make([]byte, nexts.MaxSize())
	for cid := range ciditer {
		if ok, err := schema.ExistsUnit(ctx, prevs, cid); err != nil {
			return err
		} else if ok {
			continue // if it exists in prev, then it has been validated.
		}
		n, err := nexts.Get(ctx, cid, buf)
		if err != nil {
			return err
		}
		if err := checkIntegrity(ctx, prevs, nexts, buf[:n], allowNonJSON); err != nil {
			return err
		}
	}
	return nil
}

func Constructor(params json.RawMessage, _ schema.Factory) (schema.Schema, error) {
	return &Schema{}, nil
}

func init() {
	schemareg.AddDefaultSchema(SchemaName, Constructor)
}
