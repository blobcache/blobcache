package tries

import (
	"bytes"
	"context"
	"io"

	"blobcache.io/blobcache/src/schema"
)

type Iterator struct {
	op      *Machine
	s       schema.RO
	root    Root
	span    Span
	lastKey []byte
}

func (o *Machine) NewIterator(s schema.RO, root Root, span Span) *Iterator {
	return &Iterator{op: o, s: s, root: root, span: span}
}

func (it *Iterator) Next(ctx context.Context, dst *Entry) error {
	var gteq []byte
	if it.lastKey != nil {
		gteq = append(it.lastKey, 0x00)
	} else {
		gteq = append([]byte{}, it.span.Begin...)
	}
	ent, err := it.op.MinEntry(ctx, it.s, it.root, gteq)
	if err != nil {
		if IsErrNotFound(err) {
			err = io.EOF
		}
		return err
	}

	it.lastKey = append(it.lastKey[:0], ent.Key...)
	dst.Key = append(dst.Key[:0], ent.Key...)
	dst.Value = append(dst.Value[:0], ent.Value...)
	return nil
}

// MinEntry returns the first entry >= gteq
func (o *Machine) MinEntry(ctx context.Context, s schema.RO, root Root, gteq []byte) (*Entry, error) {
	ents, err := o.getNode(ctx, s, root, false)
	if err != nil {
		return nil, err
	}
	gteq = compressKey(root.Prefix, gteq)
	if root.IsParent {
		for _, ent := range ents {
			if len(ent.Key) == 0 {
				if bytes.Equal(gteq, ent.Key) {
					return expandEntry(root.Prefix, ent), nil
				}
				continue
			}
			if bytes.Compare(ent.Key, gteq) >= 0 {
				root2, err := rootFromEntry(ent)
				if err != nil {
					return nil, err
				}
				minEnt, err := o.MinEntry(ctx, s, *root2, gteq)
				if err != nil {
					return nil, err
				}
				return expandEntry(root.Prefix, minEnt), nil
			}
		}
	} else {
		for _, ent := range ents {
			if bytes.Compare(ent.Key, gteq) >= 0 {
				return expandEntry(root.Prefix, ent), nil
			}
		}
	}
	return nil, ErrNotFound{Key: gteq}
}
