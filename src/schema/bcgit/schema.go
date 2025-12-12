package bcgit

import (
	"context"

	"blobcache.io/blobcache/src/internal/tries"
	"blobcache.io/blobcache/src/schema"
)

var _ schema.Schema = &Schema{}

type Schema struct{}

func (sch *Schema) ValidateChange(ctx context.Context, ch schema.Change) error {
	return nil
}

func (sch *Schema) Sync(ctx context.Context, rs schema.RO, ws schema.WO, rootData []byte) error {
	_, err := ParseRoot(rootData)
	if err != nil {
		return err
	}
	return nil
}

// Root is stored in the Volume's cell.
type Root struct {
	Refs tries.Root
}

func ParseRoot(data []byte) (*Root, error) {
	tr, err := tries.ParseRoot(data)
	if err != nil {
		return nil, err
	}
	return &Root{Refs: *tr}, nil
}
