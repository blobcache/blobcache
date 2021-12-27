package blobnet

import (
	"context"
	"io"

	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jonboulle/clockwork"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
)

type Params struct {
	PeerStore peers.PeerStore
	Mux       p2pmux.StringSecureAskMux
	DB        bcstate.DB
	Local     cadata.Getter
	Clock     clockwork.Clock
}

var _ cadata.Getter = &Blobnet{}

type Blobnet struct {
	mux p2pmux.StringSecureAskMux
}

func NewBlobNet(params Params) *Blobnet {
	mux := params.Mux
	bn := &Blobnet{
		mux: mux,
	}
	return bn
}

func (bn *Blobnet) Close() error {
	return nil
}

func (bn *Blobnet) HaveLocally(ctx context.Context, id cadata.ID) error {
	return nil
}

func (bn *Blobnet) GoneLocally(ctx context.Context, id cadata.ID) error {
	return nil
}

func (bn *Blobnet) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	panic("not implemented")
}

func (bn *Blobnet) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	_, err := bn.Get(ctx, id, nil)
	if err == io.ErrShortBuffer {
		err = nil
	}
	if err == bcstate.ErrNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (bn *Blobnet) MaxSize() int {
	return 1 << 22
}
