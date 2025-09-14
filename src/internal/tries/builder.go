package tries

import (
	"bytes"
	"context"

	"blobcache.io/blobcache/src/blobcache"
)

type writeStore interface {
	Post(ctx context.Context, data []byte) (blobcache.CID, error)
	Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error)
	Delete(ctx context.Context, cid blobcache.CID) error
	Exists(ctx context.Context, cid blobcache.CID) (bool, error)
	MaxSize() int
	Hash(data []byte) blobcache.CID
}

type Builder struct {
	op        *Machine
	s         writeStore
	batchSize int

	root *Root
	ents []*Entry
}

func (o *Machine) NewBuilder(s writeStore, batchSize int) *Builder {
	return &Builder{
		op:        o,
		s:         s,
		batchSize: batchSize,
	}
}

func (b *Builder) Put(ctx context.Context, k, v []byte) error {
	ent := &Entry{
		Key:   append([]byte{}, k...),
		Value: append([]byte{}, v...),
	}
	b.ents = append(b.ents, ent)
	if len(b.ents) < b.batchSize {
		return nil
	}
	return b.flush(ctx)
}

func (b *Builder) flush(ctx context.Context) error {
	sortEntries(b.ents)
	b.ents = dedup(b.ents)
	var root *Root
	var err error
	if b.root == nil {
		root, err = b.op.PostSlice(ctx, b.s, b.ents)
	} else {
		root, err = b.op.PutBatch(ctx, b.s, *b.root, b.ents)
	}
	if err != nil {
		return err
	}
	b.root = root
	b.ents = b.ents[:0]
	return nil
}

func (b *Builder) Finish(ctx context.Context) (*Root, error) {
	if err := b.flush(ctx); err != nil {
		return nil, err
	}
	return b.root, nil
}

// dedup removes duplicates from a sorted slice
func dedup(ents []*Entry) []*Entry {
	var deleted int
	for i := 0; i < len(ents); i++ {
		if i >= len(ents)-1 {
			ents[i-deleted] = ents[i]
			continue
		}
		cmp := bytes.Compare(ents[i].Key, ents[i+1].Key)
		switch {
		case cmp > 0:
			panic("dedup called with unsorted entries")
		case i < len(ents)-1 && cmp == 0:
			deleted++
		default:
			ents[i-deleted] = ents[i]
		}
	}
	return ents[:len(ents)-deleted]
}
