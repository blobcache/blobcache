// Package basiccont implements a simple container for volumes.
package basiccont

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/state/cadata"
)

var (
	_ schema.Schema    = &Schema{}
	_ schema.Container = &Schema{}
)

func init() {
	bclocal.AddDefaultSchema(blobcache.Schema_BasicContainer, Constructor)
}

// Schema
type Schema struct{}

func Constructor(_ json.RawMessage, _ schema.Factory) (schema.Schema, error) {
	return &Schema{}, nil
}

func (sch Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	var prev blobcache.OID
	return sch.WalkOIDs(ctx, change.NextStore, cadata.IDFromBytes(change.NextCell), func(oid blobcache.OID) error {
		if oid.Compare(prev) < 0 {
			return fmt.Errorf("OID %s is less than previous OID %s", oid, prev)
		}
		prev = oid
		return nil
	})
}

func (sch Schema) WalkOIDs(ctx context.Context, s schema.RO, cid blobcache.CID, fn func(blobcache.OID) error) error {
	buf := make([]byte, blobcache.CIDSize*2)
	n, err := s.Get(ctx, cid, buf)
	if err != nil {
		return err
	}
	switch n {
	case 16:
		return nil // base case, this is a Volume's OID
	case 2 * blobcache.CIDSize:
		// this is a Merkle tree node, so we need to validate the children
		left := cadata.IDFromBytes(buf[:blobcache.CIDSize])
		right := cadata.IDFromBytes(buf[blobcache.CIDSize:])
		for _, cid2 := range []cadata.ID{left, right} {
			if err := sch.WalkOIDs(ctx, s, cid2, fn); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("invalid root: %d bytes", n)
	}
}

func (sch Schema) ListOIDs(ctx context.Context, s schema.RO, root []byte) ([]blobcache.OID, error) {
	var ret []blobcache.OID
	err := sch.WalkOIDs(ctx, s, cadata.IDFromBytes(root), func(oid blobcache.OID) error {
		ret = append(ret, oid)
		return nil
	})
	return ret, err
}

func (sch Schema) ReadLinks(ctx context.Context, s schema.RO, root []byte, dst map[blobcache.OID]blobcache.ActionSet) error {
	err := sch.WalkOIDs(ctx, s, cadata.IDFromBytes(root), func(oid blobcache.OID) error {
		dst[oid] = blobcache.Action_ALL
		return nil
	})
	return err
}
