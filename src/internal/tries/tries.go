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

func (x *Root) Unmarshal(data []byte) error {
	idx := (*Index)(x)
	return idx.Unmarshal(data)
}

func ParseRoot(x []byte) (*Root, error) {
	var root Root
	if err := root.Unmarshal(x); err != nil {
		return nil, err
	}
	return &root, nil
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

func (o *Machine) Validate(ctx context.Context, s schema.RO, x Root) error {
	// getEntries includes validation
	node, err := o.getNode(ctx, s, Index(x))
	if err != nil {
		return err
	}
	ents, err := node.Entries()
	if err != nil {
		return err
	}
	if err := checkEntries(ctx, s, Index(x), ents); err != nil {
		return err
	}
	return nil
}
