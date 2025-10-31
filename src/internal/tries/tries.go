package tries

import (
	"context"

	"blobcache.io/blobcache/src/schema"
	"github.com/pkg/errors"
	"go.brendoncarroll.net/state"
)

var (
	ErrCannotCollapse = errors.Errorf("cannot collapse parent into child")
	ErrCannotSplit    = errors.Errorf("cannot split, < 2 entries")
)

// ErrNotFound is returned when a key cannot be found.
type ErrNotFound = state.ErrNotFound[[]byte]

func IsErrNotFound(err error) bool {
	return state.IsErrNotFound[[]byte](err)
}

type Span = state.ByteSpan

type Root Index

func (x *Root) Marshal(out []byte) []byte {
	idx := Index(*x)
	return idx.Marshal(out)
}

func ParseRoot(x []byte) (*Root, error) {
	var idx Index
	if err := idx.Unmarshal(x); err != nil {
		return nil, err
	}
	return (*Root)(&idx), nil
}

func (o *Machine) Validate(ctx context.Context, s schema.RO, x Index) error {
	// getEntries includes validation
	ents, err := o.getNode(ctx, s, x, false)
	if err != nil {
		return err
	}
	if x.IsParent {
		for _, ent := range ents {
			var idx Index
			if err := idx.FromEntry(*ent); err != nil {
				return err
			}
			if err := o.Validate(ctx, s, idx); err != nil {
				return err
			}
		}
	}
	return nil
}
