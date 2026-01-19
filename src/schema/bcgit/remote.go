package bcgit

import (
	"context"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/tries"
	"blobcache.io/blobcache/src/schema/bcgit/gitrh"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/stdctx/logctx"
)

const HashAlgo = blobcache.HashAlgo_SHA2_256

func Hash(x []byte) blobcache.CID {
	return HashAlgo.HashFunc()(nil, x)
}

func DefaultVolumeSpec() blobcache.VolumeSpec {
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			HashAlgo: blobcache.HashAlgo_SHA2_256,
			MaxSize:  gitrh.MaxSize,
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
	root, err := LoadRoot(ctx, tx)
	if err != nil {
		return err
	}
	if root == nil {
		troot, err := rem.tmach.NewEmpty(ctx, tx)
		if err != nil {
			return err
		}
		root = &Root{Refs: *troot}
	}
	for _, gr := range refs {
		troot, err := rem.tmach.Put(ctx, tx, root.Refs, []byte(gr.Name), gr.Target[:])
		if err != nil {
			return err
		}
		root.Refs = *troot
	}
	if err := SaveRoot(ctx, tx, *root); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func beginTTx(ctx context.Context, tmach *tries.Machine, tx *bcsdk.Tx) (*tries.Tx, error) {
	root, err := LoadRoot(ctx, tx)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return tmach.NewTxOnEmpty(tx), nil
	} else {
		return tmach.NewTx(tx, root.Refs), nil
	}
}

func (rem *Remote) Push(ctx context.Context, src bcsdk.RO, refs []GitRef) error {
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
		if err := SyncGit(ctx, src, tx, ref.Target); err != nil {
			return err
		}
		if err := ttx.Put(ctx, []byte(ref.Name), ref.Target[:]); err != nil {
			return err
		}
	}
	root, err := ttx.Flush(ctx)
	if err != nil {
		return err
	}
	if err := SaveRoot(ctx, tx, Root{Refs: *root}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (rem *Remote) Fetch(ctx context.Context, ws bcsdk.WO, refs map[string]blobcache.CID, dst map[string]blobcache.CID) error {
	tx, err := bcsdk.BeginTx(ctx, rem.svc, rem.volh, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	it, err := rem.openRefIterator(ctx, tx)
	if err != nil {
		return err
	}
	if err := streams.ForEach(ctx, it, func(gr GitRef) error {
		if want, exists := refs[gr.Name]; !exists {
			return nil
		} else if gr.Target != want {
			logctx.Infof(ctx, "skipping %v because the ref has changed", gr.Name)
			return nil
		}
		logctx.Infof(ctx, "syncing %v", gr.Name)
		if err := SyncGit(ctx, tx, ws, gr.Target); err != nil {
			return err
		}
		dst[gr.Name] = gr.Target
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// OpenIterator returns an iterator impelementing steams.Iterator
// If a non-nil RetIterator is returned (only happens when err == nil)
// then it is the callers responsibility to close it or the underlying transaction
// will remain open.
func (rem *Remote) OpenIterator(ctx context.Context) (*RefIterator, error) {
	tx, err := bcsdk.BeginTx(ctx, rem.svc, rem.volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	return rem.openRefIterator(ctx, tx)
}

func (rem *Remote) openRefIterator(ctx context.Context, tx *bcsdk.Tx) (*RefIterator, error) {
	root, err := LoadRoot(ctx, tx)
	if err != nil {
		return nil, err
	}
	if root == nil {
		// omitting the root gives an empty iterator
		return &RefIterator{tx: tx}, nil
	}
	return &RefIterator{
		tx: tx,
		it: rem.tmach.NewIterator(tx, root.Refs, tries.Span{}),
	}, nil
}

func (rem *Remote) GetRef(ctx context.Context, name string) (*GitRef, error) {
	tx, err := bcsdk.BeginTx(ctx, rem.svc, rem.volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	defer tx.Abort(ctx)
	root, err := LoadRoot(ctx, tx)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, nil
	}
	var val []byte
	if found, err := rem.tmach.Get(ctx, tx, root.Refs, []byte(name), &val); err != nil {
		return nil, err
	} else if !found {
		return nil, nil
	}
	return gitRefFromEntry(tries.Entry{
		Key:   []byte(name),
		Value: val,
	}), nil
}

func (rem *Remote) Close() error {
	// TODO: we may need to renew the handle in the background
	// adding this now means callers will get in the habit of calling it.
	return nil
}

var _ streams.Iterator[GitRef] = &RefIterator{}

type RefIterator struct {
	tx *bcsdk.Tx
	it *tries.Iterator
}

func (ri *RefIterator) Next(ctx context.Context, dst []GitRef) (int, error) {
	if ri.it == nil {
		return 0, streams.EOS()
	}
	var ent tries.Entry
	if err := streams.NextUnit(ctx, ri.it, &ent); err != nil {
		return 0, err
	}
	dst[0].Name = string(ent.Key)
	dst[0].Target = [32]byte{}
	copy(dst[0].Target[:], ent.Value)
	return 1, nil
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
