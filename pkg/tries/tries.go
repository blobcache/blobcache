package tries

import (
	"context"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNotExist = errors.Errorf("no entry for key")

	ErrCannotCollapse = errors.Errorf("cannot collapse parent into child")
	ErrCannotSplit    = errors.Errorf("cannot split, < 2 entries")
)

type Span = state.ByteSpan

type Root struct {
	Ref      Ref
	IsParent bool   `json:"is_parent"`
	Count    uint64 `json:"count"`
	Prefix   []byte `json:"prefix"`
}

func rootFromEntry(ent *Entry) (*Root, error) {
	var idx Index
	if err := proto.Unmarshal(ent.Value, &idx); err != nil {
		return nil, err
	}
	ref, err := parseRef(idx.Ref)
	if err != nil {
		return nil, err
	}
	return &Root{
		Ref: *ref,

		IsParent: idx.IsParent,
		Count:    idx.Count,

		Prefix: ent.Key,
	}, nil
}

func entryFromRoot(x Root) *Entry {
	idx := &Index{
		Ref:      marshalRef(x.Ref),
		IsParent: x.IsParent,
		Count:    x.Count,
	}
	data, err := proto.Marshal(idx)
	if err != nil {
		panic(err)
	}
	return &Entry{
		Key:   x.Prefix,
		Value: data,
	}
}

func (o *Operator) Validate(ctx context.Context, s cadata.Store, x Root) error {
	// getEntries includes validation
	ents, err := o.getNode(ctx, s, x, false)
	if err != nil {
		return err
	}
	if x.IsParent {
		for _, ent := range ents {
			root, err := rootFromEntry(ent)
			if err != nil {
				return err
			}
			if err := o.Validate(ctx, s, *root); err != nil {
				return err
			}
		}
	}
	return nil
}
