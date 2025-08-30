package bcnet

import (
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
)

var (
	_ volumes.Volume = (*Volume)(nil)
	_ volumes.Tx     = (*Tx)(nil)
)

// Volume is a remote volume.
type Volume struct {
	n    *Node
	ep   blobcache.Endpoint
	h    blobcache.Handle
	info *blobcache.VolumeInfo
}

func NewVolume(n *Node, ep blobcache.Endpoint, h blobcache.Handle, info *blobcache.VolumeInfo) *Volume {
	return &Volume{
		n:    n,
		ep:   ep,
		h:    h,
		info: info,
	}
}

func (v *Volume) Endpoint() blobcache.Endpoint {
	return v.ep
}

func (v *Volume) Handle() blobcache.Handle {
	return v.h
}

func (v *Volume) Info() *blobcache.VolumeInfo {
	return v.info
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

func (v *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (volumes.Tx, error) {
	resp, err := doJSON[BeginTxReq, BeginTxResp](ctx, v.n, v.ep, MT_VOLUME_BEGIN_TX, BeginTxReq{
		Volume: v.h,
		Params: spec,
	})
	if err != nil {
		return nil, err
	}
	return &Tx{
		n:       v.n,
		ep:      v.ep,
		params:  spec,
		h:       resp.Tx,
		volInfo: v.info,
	}, nil
}

// Tx is a transaction on a remote volume.
type Tx struct {
	n       *Node
	ep      blobcache.Endpoint
	h       blobcache.Handle
	params  blobcache.TxParams
	volInfo *blobcache.VolumeInfo

	root []byte
}

func (tx *Tx) Volume() volumes.Volume {
	return &Volume{
		n:    tx.n,
		ep:   tx.ep,
		h:    tx.h,
		info: tx.volInfo,
	}
}

func (tx *Tx) Commit(ctx context.Context) error {
	if !tx.params.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	var root *[]byte
	if tx.root != nil {
		root = &tx.root
	}
	_, err := doBinary(ctx, tx.n, tx.ep, MT_TX_COMMIT, CommitReq{
		Tx:   tx.h,
		Root: root,
	}, &CommitResp{})
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Abort(ctx context.Context) error {
	_, err := doBinary(ctx, tx.n, tx.ep, MT_TX_ABORT, AbortReq{
		Tx: tx.h,
	}, &AbortResp{})
	return err
}

func (tx *Tx) Load(ctx context.Context, dst *[]byte) error {
	resp, err := doBinary(ctx, tx.n, tx.ep, MT_TX_LOAD, LoadReq{
		Tx: tx.h,
	}, &LoadResp{})
	if err != nil {
		return err
	}
	*dst = append((*dst)[:0], resp.Root...)
	return nil
}

func (tx *Tx) Save(ctx context.Context, src []byte) error {
	if !tx.params.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	tx.root = append(tx.root[:0], src...)
	// TODO: we could also send this to the server, but it's probably
	// better to just wait until Commit time.
	return nil
}

func (tx *Tx) Post(ctx context.Context, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	var body []byte
	body = append(body, tx.h.OID[:]...)
	body = append(body, tx.h.Secret[:]...)
	reqMsg := Message{}
	if salt != nil {
		// if there is a salt, write that befor the data.  The salt size is known.
		reqMsg.SetCode(MT_TX_POST_SALT)
		body = append(body, salt[:]...)
	} else {
		// just write the data.
		reqMsg.SetCode(MT_TX_POST)
	}
	body = append(body, data[:]...)
	reqMsg.SetBody(body)

	// do request/response
	var respMsg Message
	if err := tx.n.Ask(ctx, tx.ep, reqMsg, &respMsg); err != nil {
		return blobcache.CID{}, err
	}

	if respMsg.Header().Code().IsError() {
		return blobcache.CID{}, ParseWireError(respMsg.Header().Code(), respMsg.Body())
	}
	if !respMsg.Header().Code().IsOK() {
		return blobcache.CID{}, fmt.Errorf("reply message has non-OK code: %d", respMsg.Header().Code())
	}

	// request is ok at this point.
	respBody := respMsg.Body()
	if len(respBody) != blobcache.CIDSize {
		return blobcache.CID{}, fmt.Errorf("invalid response body length: %d", len(respBody))
	}
	var theirCID blobcache.CID
	copy(theirCID[:], respBody)
	ourCID := tx.Hash(salt, data)
	if theirCID != ourCID {
		return blobcache.CID{}, fmt.Errorf("hash mismatch: ourCID=%s, theirCID=%s", ourCID, theirCID)
	}
	return theirCID, nil
}

func (tx *Tx) Get(ctx context.Context, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	var body []byte
	body = append(body, tx.h.OID[:]...)
	body = append(body, tx.h.Secret[:]...)
	body = append(body, cid[:]...)
	var reqMsg Message
	reqMsg.SetCode(MT_TX_GET)
	reqMsg.SetBody(body)

	var respMsg Message
	if err := tx.n.Ask(ctx, tx.ep, reqMsg, &respMsg); err != nil {
		return 0, err
	}
	if respMsg.Header().Code().IsError() {
		return 0, ParseWireError(respMsg.Header().Code(), respMsg.Body())
	}
	if !respMsg.Header().Code().IsOK() {
		return 0, fmt.Errorf("reply message has non-OK code: %d", respMsg.Header().Code())
	}
	respBody := respMsg.Body()
	if err := blobcache.CheckBlob(tx.Hash, salt, &cid, respBody); err != nil {
		return 0, err
	}
	if len(respMsg.Body()) > len(buf) {
		return 0, fmt.Errorf("buffer too short")
	}
	copy(buf, respMsg.Body())
	return len(respMsg.Body()), nil
}

func (tx *Tx) Delete(ctx context.Context, cid blobcache.CID) error {
	_, err := doBinary(ctx, tx.n, tx.ep, MT_TX_DELETE, DeleteReq{
		Tx:  tx.h,
		CID: cid,
	}, &DeleteResp{})
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Exists(ctx context.Context, cid blobcache.CID) (bool, error) {
	resp, err := doBinary(ctx, tx.n, tx.ep, MT_TX_EXISTS, ExistsReq{
		Tx:   tx.h,
		CIDs: []blobcache.CID{cid},
	}, &ExistsResp{})
	if err != nil {
		return false, err
	}
	return resp.Exists[0], nil
}

func (tx *Tx) MaxSize() int {
	return int(tx.volInfo.MaxSize)
}

func (tx *Tx) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	hf := tx.volInfo.HashAlgo.HashFunc()
	return hf(salt, data)
}

func (tx *Tx) AllowLink(ctx context.Context, subvol blobcache.Handle) error {
	_, err := doBinary(ctx, tx.n, tx.ep, MT_TX_ALLOW_LINK, AllowLinkReq{
		Tx:     tx.h,
		Subvol: subvol,
	}, &AllowLinkResp{})
	return err
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
	if respMsg.Header().Code().IsError() {
		return nil, ParseWireError(respMsg.Header().Code(), respMsg.Body())
	}
	if !respMsg.Header().Code().IsOK() {
		return nil, fmt.Errorf("reply message has non-OK code: %d", respMsg.Header().Code())
	}
	var resp Resp
	if err := json.Unmarshal(respMsg.Body(), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func doBinary[Req interface{ Marshal(out []byte) []byte }, Resp interface{ Unmarshal(data []byte) error }](ctx context.Context, node *Node, remote blobcache.Endpoint, code MessageType, req Req, zeroResp Resp) (Resp, error) {
	reqData := req.Marshal(nil)
	var reqMsg Message
	reqMsg.SetCode(code)
	reqMsg.SetBody(reqData)
	var respMsg Message
	if err := node.Ask(ctx, remote, reqMsg, &respMsg); err != nil {
		var zero Resp
		return zero, err
	}
	if respMsg.Header().Code().IsError() {
		var zero Resp
		return zero, ParseWireError(respMsg.Header().Code(), respMsg.Body())
	}
	if !respMsg.Header().Code().IsOK() {
		var zero Resp
		return zero, fmt.Errorf("reply message has non-OK code: %d", respMsg.Header().Code())
	}
	resp := zeroResp
	if err := resp.Unmarshal(respMsg.Body()); err != nil {
		var zero Resp
		return zero, err
	}
	return resp, nil
}

func OpenVolumeFrom(ctx context.Context, n *Node, ep blobcache.Endpoint, base blobcache.Handle, target blobcache.OID, mask blobcache.ActionSet) (*Volume, error) {
	resp, err := doJSON[OpenFromReq, OpenFromResp](ctx, n, ep, MT_OPEN_FROM, OpenFromReq{
		Base:   base,
		Target: target,
		Mask:   mask,
	})
	if err != nil {
		return nil, err
	}
	if err := resp.Info.HashAlgo.Validate(); err != nil {
		return nil, err
	}
	return NewVolume(n, ep, resp.Handle, &resp.Info), nil
}

func OpenVolumeAs(ctx context.Context, n *Node, ep blobcache.Endpoint, target blobcache.OID, mask blobcache.ActionSet) (*Volume, error) {
	resp, err := doJSON[OpenAsReq, OpenAsResp](ctx, n, ep, MT_OPEN_AS, OpenAsReq{
		Target: target,
		Mask:   mask,
	})
	if err != nil {
		return nil, err
	}
	if err := resp.Info.HashAlgo.Validate(); err != nil {
		return nil, err
	}
	return NewVolume(n, ep, resp.Handle, &resp.Info), nil
}

func InspectVolume(ctx context.Context, n *Node, ep blobcache.Endpoint, vol blobcache.Handle) (*blobcache.VolumeInfo, error) {
	resp, err := doJSON[InspectVolumeReq, InspectVolumeResp](ctx, n, ep, MT_VOLUME_INSPECT, InspectVolumeReq{
		Volume: vol,
	})
	if err != nil {
		return nil, err
	}
	return &resp.Info, nil
}
