package bcgit

import (
	"context"
	"log"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/tries"
	"blobcache.io/blobcache/src/schema/bcgit/gitrh"
	"go.brendoncarroll.net/exp/streams"
)

func Hash(x []byte) blobcache.CID {
	return blobcache.HashAlgo_SHA2_256.HashFunc()(nil, x)
}

func DefaultVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			HashAlgo: blobcache.HashAlgo_SHA2_256,
			MaxSize:  1e7,
		},
	}
}

type GitRef = gitrh.Ref

// Remote is a Git Remote backed by a Blobcache Volume
type Remote struct {
	svc  blobcache.Service
	volh blobcache.Handle

	tmach *tries.Machine
}

func NewRemote(svc blobcache.Service, volh blobcache.Handle) *Remote {
	return &Remote{
		svc:  svc,
		volh: volh,

		tmach: tries.NewMachine(nil, blobcache.HashAlgo_SHA2_256.HashFunc()),
	}
}

func (rem *Remote) putRefs(ctx context.Context, refs []GitRef) error {
	tx, err := bcsdk.BeginTx(ctx, rem.svc, rem.volh, blobcache.TxParams{Modify: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	root, err := tries.LoadRoot(ctx, tx)
	if err != nil {
		return err
	}
	if root == nil {
		root, err = rem.tmach.NewEmpty(ctx, tx)
		if err != nil {
			return err
		}
	}
	for _, gr := range refs {
		root, err = rem.tmach.Put(ctx, tx, *root, []byte(gr.Name), gr.Target[:])
		if err != nil {
			return err
		}
	}
	if err := tries.SaveRoot(ctx, tx, *root); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func beginTTx(ctx context.Context, tmach *tries.Machine, tx *bcsdk.Tx) (*tries.Tx, error) {
	root, err := tries.LoadRoot(ctx, tx)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return tmach.NewTxOnEmpty(), nil
	} else {
		return tmach.NewTx(*root), nil
	}
}

func (rem *Remote) Push(ctx context.Context, src *gitrh.Store, refs []GitRef) error {
	tx, err := bcsdk.BeginTx(ctx, rem.svc, rem.volh, blobcache.TxParams{Modify: true})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	ttx, err := beginTTx(ctx, rem.tmach, tx)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		log.Println("setting", ref.Name, "to", ref.Target)
		if err := Sync(ctx, src, tx, ref.Target); err != nil {
			return err
		}
		if err := ttx.Put(ctx, tx, []byte(ref.Name), ref.Target[:]); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (gr *Remote) Iterate(ctx context.Context) (streams.Iterator[GitRef], error) {
	tx, err := bcsdk.BeginTx(ctx, gr.svc, gr.volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	root, err := tries.LoadRoot(ctx, tx)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return streams.NewSlice[GitRef](nil, nil), nil
	}
	return &RefIterator{
		tx: tx,
		it: gr.tmach.NewIterator(tx, *root, tries.Span{}),
	}, nil
}

func (gr *Remote) GetRef(ctx context.Context, name string) (*GitRef, error) {
	tx, err := bcsdk.BeginTx(ctx, gr.svc, gr.volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	root, err := tries.LoadRoot(ctx, tx)
	if err != nil {
		return nil, err
	}
	var val []byte
	if found, err := gr.tmach.Get(ctx, tx, *root, []byte(name), &val); err != nil {
		return nil, err
	} else if !found {
		return nil, nil
	}
	return gitRefFromEntry(tries.Entry{
		Key:   []byte(name),
		Value: val,
	}), nil
}

func (gr *Remote) Close() error {
	// TODO: we may need to renew the handle in the background
	// adding this now means callers will get in the habit of calling it.
	return nil
}

var _ streams.Iterator[GitRef] = &RefIterator{}

type RefIterator struct {
	tx *bcsdk.Tx
	it *tries.Iterator
}

func (ri *RefIterator) Next(ctx context.Context, dst *GitRef) error {
	var ent tries.Entry
	if err := ri.it.Next(ctx, &ent); err != nil {
		return err
	}
	dst.Name = string(ent.Key)
	dst.Target = [32]byte{}
	copy(dst.Target[:], ent.Value)
	return nil
}

func (ri *RefIterator) Close() error {
	return ri.tx.Abort(context.TODO())
}

func gitRefFromEntry(ent tries.Entry) *GitRef {
	var target [32]byte
	copy(target[:], ent.Value)
	return &GitRef{
		Name:   string(ent.Key),
		Target: target,
	}
}
