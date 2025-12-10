package tries

import (
	"context"

	"blobcache.io/blobcache/src/bcsdk"
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

// LoadRoot loads the root node from the given loader.
// LoadRoot returns (nil, nil) if the root is empty.
func LoadRoot(ctx context.Context, ldr bcsdk.Loader) (*Root, error) {
	var rootData []byte
	if err := ldr.Load(ctx, &rootData); err != nil {
		return nil, err
	}
	if len(rootData) == 0 {
		return nil, nil
	}
	return ParseRoot(rootData)
}

func SaveRoot(ctx context.Context, tx bcsdk.Saver, root Root) error {
	return tx.Save(ctx, root.Marshal(nil))
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
	return nil
}
