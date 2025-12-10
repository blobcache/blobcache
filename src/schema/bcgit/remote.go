package bcgit

import (
	"context"
	"iter"
	"log"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/tries"
	"blobcache.io/blobcache/src/schema/bcgit/gitrh"
	"go.brendoncarroll.net/exp/streams"
)

func Hash(x []byte) blobcache.CID {
	return blobcache.HashAlgo_SHA2_256.HashFunc()(nil, x)
}

type GitRef = gitrh.Ref

// FmtURL formats a URL
func FmtURL(u blobcache.URL) string {
	return "bc::" + strings.TrimPrefix(u.String(), "bc://")
}

func NewRemoteHelper(rem *Remote) gitrh.Server {
	return gitrh.Server{
		List: func(ctx context.Context) iter.Seq2[GitRef, error] {
			return func(yield func(GitRef, error) bool) {
				it, err := rem.Iterate(ctx)
				if err != nil {
					yield(GitRef{}, err)
					return
				}
				var gr GitRef
				for {
					if err := it.Next(ctx, &gr); err != nil {
						if !streams.IsEOS(err) {
							yield(GitRef{}, err)
						}
						return
					}
					if !yield(gr, nil) {
						return
					}
				}
			}
		},
	}
}

func OpenRemoteHelper(ctx context.Context, bc blobcache.Service, u blobcache.URL) (*Remote, error) {
	volh, err := bcsdk.OpenURL(ctx, bc, u)
	if err != nil {
		return nil, err
	}
	return NewRemote(bc, *volh), nil
}

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

func (gr *Remote) Iterate(ctx context.Context) (streams.Iterator[GitRef], error) {
	tx, err := bcsdk.BeginTx(ctx, gr.svc, gr.volh, blobcache.TxParams{})
	if err != nil {
		return nil, err
	}
	var rootData []byte
	if err := tx.Load(ctx, &rootData); err != nil {
		tx.Abort(ctx)
		return nil, err
	}
	root, err := tries.ParseRoot(rootData)
	if err != nil {
		tx.Abort(ctx)
		return nil, err
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
	log.Println("ent", string(ent.Key))
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
