package merklelog

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/exp/streams"
)

var _ schema.Schema = &Schema{}

type Params struct {
	X           blobcache.SchemaSpec `json:"x"`
	MaxRootSize int                  `json:"maxRootSize"`
}

type Schema struct {
	X           schema.Schema
	MaxRootSize int
}

// Constructor is a schema constructor
func Constructor(params json.RawMessage, mkSchema schema.Factory) (schema.Schema, error) {
	var spec Params
	if err := json.Unmarshal(params, &spec); err != nil {
		return nil, err
	}
	x, err := mkSchema(spec.X)
	if err != nil {
		return nil, err
	}
	return &Schema{
		X:           x,
		MaxRootSize: spec.MaxRootSize,
	}, nil
}

func (sch Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	var states [2]State
	if err := states[0].Unmarshal(change.PrevCell); err != nil {
		return err
	}
	if err := states[1].Unmarshal(change.NextCell); err != nil {
		return err
	}
	if includes, err := Includes(ctx, change.NextStore, states[1], states[0]); err != nil {
		return err
	} else if !includes {
		return fmt.Errorf("merklelog: next state does not include prev state")
	}
	it := NewIterator(states[1], change.NextStore, states[0].Len(), states[1].Len())
	spit := newSlidingPairIterator(it)
	buf1 := make([]byte, sch.MaxRootSize)
	buf2 := make([]byte, sch.MaxRootSize)
	return streams.ForEach(ctx, spit, func(cids [2]CID) error {
		n1, err := change.NextStore.Get(ctx, cids[0], buf1)
		if err != nil {
			return err
		}
		data1 := buf1[:n1]
		n2, err := change.NextStore.Get(ctx, cids[1], buf2)
		if err != nil {
			return err
		}
		data2 := buf2[:n2]

		return sch.X.ValidateChange(ctx, schema.Change{
			PrevCell:  data1,
			NextCell:  data2,
			PrevStore: change.NextStore,
			NextStore: change.NextStore,
		})
	})
}

func init() {
	bclocal.AddDefaultSchema(blobcache.Schema_MerkleLog, Constructor)
}
