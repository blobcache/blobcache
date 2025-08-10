package glfsns

import (
	"bytes"
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/simplens"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/glfs"
	"go.brendoncarroll.net/exp/streams"
)

type Namespace struct {
	Volume  volumes.Volume
	Machine *glfs.Machine
}

func (ns *Namespace) GetEntry(ctx context.Context, name string) (*simplens.Entry, error) {
	panic("not implemented")
}

func (ns *Namespace) PutEntry(ctx context.Context, x simplens.Entry) error {
	tx, err := ns.Volume.BeginTx(ctx, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	panic("not implemented")
}

func (ns *Namespace) DeleteEntry(ctx context.Context, name string) error {
	panic("not implemented")
}

func (ns *Namespace) ListEntries(ctx context.Context) ([]simplens.Entry, error) {
	panic("not implemented")
}

type Tx struct {
	Machine *glfs.Machine
	Tx      volumes.Tx
}

func (tx *Tx) loadRef(ctx context.Context) (*glfs.Ref, error) {
	var root []byte
	if err := tx.Tx.Load(ctx, &root); err != nil {
		return nil, err
	}
	return ParseRef(root)
}

func (tx *Tx) GetEntry(ctx context.Context, name string) (*simplens.Entry, error) {
	ref, err := tx.loadRef(ctx)
	if err != nil {
		return nil, err
	}
	s := volumes.NewUnsaltedStore(tx.Tx)

	ref2, err := tx.Machine.GetAtPath(ctx, s, *ref, name)
	if err != nil {
		return nil, err
	}
	r, err := tx.Machine.GetBlob(ctx, s, *ref2)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(r)
	var ret simplens.Entry
	if err := dec.Decode(&ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func (tx *Tx) PutEntry(ctx context.Context, x simplens.Entry) error {
	ref, err := tx.loadRef(ctx)
	if err != nil {
		return err
	}
	s := volumes.NewUnsaltedStore(tx.Tx)
	entRef, err := tx.Machine.PostBlob(ctx, s, bytes.NewReader(jsonMarshal(x)))
	if err != nil {
		return err
	}
	newLayer, err := tx.Machine.PostTreeSlice(ctx, s, []glfs.TreeEntry{
		{Name: x.Name, Ref: *entRef},
	})
	if err != nil {
		return err
	}
	nextRoot, err := glfs.Merge(ctx, s, s, *ref, *newLayer)
	if err != nil {
		return err
	}
	return tx.Tx.Commit(ctx, MarshalRef(nextRoot))
}

func (tx *Tx) ListEntries(ctx context.Context) ([]simplens.Entry, error) {
	ref, err := tx.loadRef(ctx)
	if err != nil {
		return nil, err
	}
	src := volumes.NewUnsaltedStore(tx.Tx)
	tr, err := tx.Machine.NewTreeReader(src, *ref)
	if err != nil {
		return nil, err
	}
	var ret []simplens.Entry
	if err := streams.ForEach(ctx, tr, func(entry glfs.TreeEntry) error {
		br, err := tx.Machine.GetBlob(ctx, src, entry.Ref)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(br)
		var x simplens.Entry
		if err := dec.Decode(&x); err != nil {
			return err
		}
		ret = append(ret, x)
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (tx *Tx) DeleteEntry(ctx context.Context, name string) error {
	panic("not implemented")
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
