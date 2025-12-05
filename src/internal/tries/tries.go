package tries

import (
	"context"

	"blobcache.io/blobcache/src/internal/tries/triescnp"
	"blobcache.io/blobcache/src/schema"
	"github.com/pkg/errors"
	"go.brendoncarroll.net/state"
)

var (
	ErrCannotCollapse = errors.Errorf("cannot collapse parent into child")
	ErrCannotSplit    = errors.Errorf("cannot split, < 2 entries")
)

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
	node, err := o.getNode(ctx, s, x)
	if err != nil {
		return err
	}
	ents, err := node.Entries()
	if err != nil {
		return err
	}
	if err := checkEntries(ctx, s, x, ents); err != nil {
		return err
	}
	for i := 0; i < ents.Len(); i++ {
		ent := ents.At(i)
		switch ent.Which() {
		case triescnp.Entry_Which_value:
			continue
		}
		return errors.Errorf("invalid entry: %s", ent.Which())
	}
	return nil
}
