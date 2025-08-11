package glfs

import (
	"context"
	"encoding/json"

	"blobcache.io/glfs"
	"go.brendoncarroll.net/state/cadata"
)

type Schema struct {
	Mach *glfs.Machine
}

func (sch *Schema) Validate(ctx context.Context, src cadata.Getter, root []byte) error {
	_, err := ParseRef(root)
	return err
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
