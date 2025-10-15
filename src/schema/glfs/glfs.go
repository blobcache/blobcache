package glfs

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/glfs"
	"go.brendoncarroll.net/state/cadata"
)

var _ schema.Schema = &Schema{}

type Schema struct {
	Mach *glfs.Machine
}

func (sch *Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	if len(change.PrevCell) == 0 {
		nextRef, err := ParseRef(change.NextCell)
		if err != nil {
			return err
		}
		return sch.Mach.WalkRefs(ctx, change.NextStore, *nextRef, func(ref glfs.Ref) error {
			return nil
		})
	}
	prevRef, err := ParseRef(change.PrevCell)
	if err != nil {
		return err
	}
	nextRef, err := ParseRef(change.NextCell)
	if err != nil {
		return err
	}
	return DiffRefs(ctx, change.NextStore, *prevRef, *nextRef, func(left, right *glfs.Ref) error { return nil })
}

func ParseRef(root []byte) (*glfs.Ref, error) {
	var ref glfs.Ref
	if err := json.Unmarshal(root, &ref); err != nil {
		return nil, err
	}
	return &ref, nil
}

func MarshalRef(ref *glfs.Ref) []byte {
	return jsonMarshal(ref)
}

func jsonMarshal(x any) []byte {
	buf, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return buf
}

// DiffRefs calls fn for ecah ref that is only in the left filsystem or only in the right filesystem.
// Each call to fn, will only set one of left or right to non-nil.
func DiffRefs(ctx context.Context, src cadata.Getter, left, right glfs.Ref, fn func(left, right *glfs.Ref) error) error {
	// TODO: implement this.
	return nil
}

func Load(ctx context.Context, txn *blobcache.Tx) (*glfs.Ref, error) {
	var root []byte
	if err := txn.Load(ctx, &root); err != nil {
		return nil, err
	}
	return ParseRef(root)
}

func SyncVolume(ctx context.Context, svc blobcache.Service, srcVol, dstVol blobcache.Handle) error {
	srcTx, err := blobcache.BeginTx(ctx, svc, srcVol, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer srcTx.Abort(ctx)
	dstTx, err := blobcache.BeginTx(ctx, svc, dstVol, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer dstTx.Abort(ctx)
	return SyncTx(ctx, srcTx, dstTx)
}

// SyncTx efficiently copies all the data from src to dst.
// SyncTx will call Commit on dstTx with the new root.
func SyncTx(ctx context.Context, srcTx, dstTx *blobcache.Tx) error {
	srcRoot, err := Load(ctx, srcTx)
	if err != nil {
		return err
	}
	dstRoot, err := Load(ctx, dstTx)
	if err != nil {
		return err
	}
	if srcRoot.Equals(*dstRoot) {
		return nil
	}
	if err := glfs.Sync(ctx, dstTx, srcTx, *srcRoot); err != nil {
		return err
	}
	if err := dstTx.Save(ctx, MarshalRef(srcRoot)); err != nil {
		return err
	}
	return dstTx.Commit(ctx)
}
