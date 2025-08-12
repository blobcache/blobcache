package glfs

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/schema"
	"blobcache.io/glfs"
	"go.brendoncarroll.net/state/cadata"
)

var _ schema.Schema = &Schema{}

type Schema struct {
	Mach *glfs.Machine
}

func (sch *Schema) Validate(ctx context.Context, src cadata.Getter, prev, next []byte) error {
	if len(prev) == 0 {
		nextRef, err := ParseRef(next)
		if err != nil {
			return err
		}
		return sch.Mach.WalkRefs(ctx, src, *nextRef, func(ref glfs.Ref) error {
			return nil
		})
	}
	prevRef, err := ParseRef(prev)
	if err != nil {
		return err
	}
	nextRef, err := ParseRef(next)
	if err != nil {
		return err
	}
	return DiffRefs(ctx, src, *prevRef, *nextRef, func(left, right *glfs.Ref) error { return nil })
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

type Store interface {
	Get(ctx context.Context, cid cadata.ID, buf []byte) (int, error)
	Post(ctx context.Context, salt *cadata.ID, data []byte) (cadata.ID, error)
	Exists(ctx context.Context, cid cadata.ID) (bool, error)
}

// DiffRefs calls fn for ecah ref that is only in the left filsystem or only in the right filesystem.
// Each call to fn, will only set one of left or right to non-nil.
func DiffRefs(ctx context.Context, src cadata.Getter, left, right glfs.Ref, fn func(left, right *glfs.Ref) error) error {
	// TODO: implement this.
	return nil
}
