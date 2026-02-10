package bcglfs

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/glfs"
)

const SchemaName blobcache.SchemaName = "glfs"

func init() {
	schemareg.AddDefaultSchema(SchemaName, Constructor)
}

var (
	_ schema.Schema = &Schema{}

	_ schema.Constructor = Constructor
)

type Spec struct{}

func Constructor(params json.RawMessage, mkSchema schema.Factory) (schema.Schema, error) {
	var spec Spec
	if err := json.Unmarshal(params, &spec); err != nil {
		return nil, err
	}
	return &Schema{Mach: glfs.NewMachine()}, nil
}

type Schema struct {
	Mach *glfs.Machine
}

func (sch *Schema) ValidateChange(ctx context.Context, change schema.Change) error {
	if len(change.Prev.Cell) == 0 {
		nextRef, err := ParseRef(change.Next.Cell)
		if err != nil {
			return err
		}
		return sch.Mach.WalkRefs(ctx, change.Next.Store, *nextRef, func(ref glfs.Ref) error {
			return nil
		})
	}
	prevRef, err := ParseRef(change.Prev.Cell)
	if err != nil {
		return err
	}
	nextRef, err := ParseRef(change.Next.Cell)
	if err != nil {
		return err
	}
	return DiffRefs(ctx, change.Next.Store, *prevRef, *nextRef, func(left, right *glfs.Ref) error { return nil })
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
func DiffRefs(ctx context.Context, src schema.RO, left, right glfs.Ref, fn func(left, right *glfs.Ref) error) error {
	// TODO: implement this.
	return nil
}

func Load(ctx context.Context, txn *bcsdk.Tx) (*glfs.Ref, error) {
	var root []byte
	if err := txn.Load(ctx, &root); err != nil {
		return nil, err
	}
	return ParseRef(root)
}

func SyncVolume(ctx context.Context, svc blobcache.Service, srcVol, dstVol blobcache.Handle) error {
	srcTx, err := bcsdk.BeginTx(ctx, svc, srcVol, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer srcTx.Abort(ctx)
	dstTx, err := bcsdk.BeginTx(ctx, svc, dstVol, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer dstTx.Abort(ctx)
	return SyncTx(ctx, srcTx, dstTx)
}

// SyncTx efficiently copies all the data from src to dst.
// SyncTx will call Commit on dstTx with the new root.
func SyncTx(ctx context.Context, srcTx, dstTx *bcsdk.Tx) error {
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
