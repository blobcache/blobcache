package bcnet

import (
	"context"
	"encoding/json"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
)

var (
	_ volumes.Volume[[]byte] = (*Volume)(nil)
	_ volumes.Tx[[]byte]     = (*Tx)(nil)
)

type HandleMapping struct {
	Upwards   func(blobcache.Handle) blobcache.Handle
	Downwards func(blobcache.Handle) blobcache.Handle
}

func (hm HandleMapping) IsZero() bool {
	return hm.Upwards == nil && hm.Downwards == nil
}

// Volume is a remote volume.
type Volume struct {
	n            *Node
	ep           blobcache.Endpoint
	h            blobcache.Handle
	handleMapper HandleMapping
}

func NewVolume(n *Node, ep blobcache.Endpoint, h blobcache.Handle, handleMapper HandleMapping) *Volume {
	if handleMapper.IsZero() {
		handleMapper = HandleMapping{
			Upwards:   func(h blobcache.Handle) blobcache.Handle { return h },
			Downwards: func(h blobcache.Handle) blobcache.Handle { return h },
		}
	}
	return &Volume{
		n:            n,
		ep:           ep,
		h:            h,
		handleMapper: handleMapper,
	}
}

func (v *Volume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	_, err := doJSON[AwaitReq, AwaitResp](ctx, v.n, v.ep, MT_VOLUME_AWAIT, AwaitReq{
		Cond: blobcache.Conditions{},
	})
	if err != nil {
		return err
	}
	loadResp, err := doJSON[LoadReq, LoadResp](ctx, v.n, v.ep, MT_TX_LOAD, LoadReq{
		Tx: v.h,
	})
	if err != nil {
		return err
	}
	*next = loadResp.Root
	return nil
}

func (v *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (volumes.Tx[[]byte], error) {
	resp, err := doJSON[BeginTxReq, BeginTxResp](ctx, v.n, v.ep, MT_VOLUME_BEGIN_TX, BeginTxReq{
		Volume:   v.handleMapper.Downwards(v.h),
		TxParams: spec,
	})
	if err != nil {
		return nil, err
	}
	return &Tx{
		n:  v.n,
		ep: v.ep,
		h:  v.handleMapper.Upwards(resp.Handle),
	}, nil
}

// Tx is a transaction on a remote volume.
type Tx struct {
	n  *Node
	ep blobcache.Endpoint
	h  blobcache.Handle
}

func (tx *Tx) Commit(ctx context.Context, root []byte) error {
	_, err := doJSON[CommitReq, CommitResp](ctx, tx.n, tx.ep, MT_TX_COMMIT, CommitReq{
		Tx:   tx.h,
		Root: root,
	})
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Abort(ctx context.Context) error {
	_, err := doJSON[AbortReq, AbortResp](ctx, tx.n, tx.ep, MT_TX_ABORT, AbortReq{
		Tx: tx.h,
	})
	return err
}

func (tx *Tx) Load(ctx context.Context, dst *[]byte) error {
	_, err := doJSON[LoadReq, LoadResp](ctx, tx.n, tx.ep, MT_TX_LOAD, LoadReq{
		Tx: tx.h,
	})
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	return blobcache.CID{}, nil
}

func (tx *Tx) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	return 0, nil
}

func (tx *Tx) Delete(ctx context.Context, cid blobcache.CID) error {
	_, err := doJSON[DeleteReq, DeleteResp](ctx, tx.n, tx.ep, MT_TX_DELETE, DeleteReq{
		Tx:  tx.h,
		CID: cid,
	})
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	resp, err := doJSON[ExistsReq, ExistsResp](ctx, tx.n, tx.ep, MT_TX_EXISTS, ExistsReq{
		Tx:   tx.h,
		CIDs: []blobcache.CID{cid},
	})
	if err != nil {
		return false, err
	}
	return resp.Exists[0], nil
}

func (tx *Tx) MaxSize() int {
	return 0
}

func (tx *Tx) Hash(data []byte) blobcache.CID {
	return blobcache.CID{}
}

func doJSON[Req, Resp any](ctx context.Context, node *Node, remote blobcache.Endpoint, code MessageType, req Req) (*Resp, error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var reqMsg Message
	reqMsg.SetCode(code)
	reqMsg.SetBody(reqData)
	var respMsg Message
	if err := node.Ask(ctx, remote, reqMsg, &respMsg); err != nil {
		return nil, err
	}
	if respMsg.Header().Code() == MT_ERROR {
		return nil, ParseWireError(respMsg.Body())
	}
	var resp Resp
	if err := json.Unmarshal(respMsg.Body(), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func OpenVolume(ctx context.Context, n *Node, ep blobcache.Endpoint, id blobcache.OID) (volumes.Volume[[]byte], error) {
	resp, err := doJSON[OpenReq, OpenResp](ctx, n, ep, MT_OPEN, OpenReq{
		OID: id,
	})
	if err != nil {
		return nil, err
	}
	return NewVolume(n, ep, resp.Handle, HandleMapping{}), nil
}
