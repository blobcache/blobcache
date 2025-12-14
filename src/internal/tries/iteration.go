package tries

import (
	"bytes"
	"context"
	"fmt"

	"blobcache.io/blobcache/src/internal/tries/triescnp"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/exp/streams"
)

type Iterator struct {
	m    *Machine
	s    schema.RO
	root Root

	span    Span
	lastKey []byte
}

func (mach *Machine) NewIterator(s schema.RO, root Root, span Span) *Iterator {
	return &Iterator{m: mach, s: s, root: root, span: span}
}

func (it *Iterator) Next(ctx context.Context, dst *Entry) error {
	var gteq []byte
	if it.lastKey != nil {
		gteq = append(it.lastKey, 0x00)
	} else {
		gteq = append([]byte{}, it.span.Begin...)
	}
	ent, err := it.m.MinEntry(ctx, it.s, it.root, gteq)
	if err != nil {
		return err
	} else if ent == nil {
		return streams.EOS()
	}

	it.lastKey = append(it.lastKey[:0], ent.Key...)
	dst.Key = append(dst.Key[:0], ent.Key...)
	dst.Value = append(dst.Value[:0], ent.Value...)
	return nil
}

// MinEntry returns the first entry >= gteq
func (mach *Machine) MinEntry(ctx context.Context, s schema.RO, root Root, gteq []byte) (*Entry, error) {
	node, err := mach.getNode(ctx, s, Index(root))
	if err != nil {
		return nil, err
	}
	gteq = compressKey(root.Prefix, gteq)
	ent, err := mach.minEntry(ctx, s, *node, gteq)
	if err != nil {
		return nil, err
	}
	if ent != nil {
		ent.Key = expandKey(root.Prefix, ent.Key)
	}
	return ent, nil
}

func (mach *Machine) minEntry(ctx context.Context, s schema.RO, node triescnp.Node, gteq []byte) (*Entry, error) {
	el, err := node.Entries()
	if err != nil {
		return nil, err
	}
	for i := 0; i < el.Len(); i++ {
		xent := el.At(i)
		k, err := xent.Key()
		if err != nil {
			return nil, err
		}

		switch xent.Which() {
		case triescnp.Entry_Which_value:
			if bytes.Compare(k, gteq) >= 0 {
				var ent Entry
				if err := ent.fromCNP(xent); err != nil {
					return nil, err
				}
				return &ent, nil
			}

		case triescnp.Entry_Which_index:
			var ient Index
			if err := ient.fromCNP(xent); err != nil {
				return nil, err
			}
			var gteq2 []byte
			if bytes.Compare(gteq, k) <= 0 {
				gteq2 = compressKey(gteq, k)
			}
			ent, err := mach.MinEntry(ctx, s, Root(ient), gteq2)
			if err != nil {
				return nil, err
			}
			ent.Key = expandKey(ent.Key, k)
			return ent, nil
		default:
			return nil, fmt.Errorf("unknown entry type: %v", xent.Which())
		}
	}
	// no entry >= gteq
	return nil, nil
}
