package bcnet

import (
	"bytes"
	"context"

	"github.com/inet256/inet256/pkg/inet256"
	"go.brendoncarroll.net/p2p"
	"go.brendoncarroll.net/state/cadata"
)

type BlobPullServer struct {
	open func(PeerID) cadata.Store
}

func (s *BlobPullServer) HandleAsk(ctx context.Context, resp []byte, req p2p.Message[PeerID]) int {
	id := cadata.IDFromBytes(req.Payload)
	n, err := s.open(req.Src).Get(ctx, id, resp)
	if cadata.IsNotFound(err) {
		return copy(resp, id[:])
	}
	if err != nil {
		return copy(resp, []byte(err.Error()))
	}
	return n
}

type BlobPullClient struct {
	swarm p2p.SecureAskSwarm[PeerID, inet256.PublicKey]
}

func (c BlobPullClient) Pull(ctx context.Context, dst PeerID, id cadata.ID, buf []byte) (int, error) {
	n, err := c.swarm.Ask(ctx, buf, dst, p2p.IOVec{id[:]})
	if err != nil {
		return 0, err
	}
	if bytes.Equal(buf, id[:]) {
		return 0, cadata.ErrNotFound{Key: id}
	}
	actual := Hash(buf[:n])
	if actual != id {
		return 0, cadata.ErrBadData
	}
	return n, nil
}
