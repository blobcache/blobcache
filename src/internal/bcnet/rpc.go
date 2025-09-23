package bcnet

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

type Transport interface {
	Tell(ctx context.Context, ep blobcache.Endpoint, req *Message) error
	Ask(ctx context.Context, ep blobcache.Endpoint, req Message, resp *Message) error
}

func Drop(ctx context.Context, tp Transport, ep blobcache.Endpoint, h blobcache.Handle) error {
	var resp DropResp
	if _, err := doAsk(ctx, tp, ep, MT_HANDLE_DROP, DropReq{Handle: h}, &resp); err != nil {
		return err
	}
	return nil
}

func KeepAlive(ctx context.Context, tp Transport, ep blobcache.Endpoint, hs []blobcache.Handle) error {
	var resp KeepAliveResp
	if _, err := doAsk(ctx, tp, ep, MT_HANDLE_KEEP_ALIVE, KeepAliveReq{Handles: hs}, &resp); err != nil {
		return err
	}
	return nil
}

func InspectHandle(ctx context.Context, tp Transport, ep blobcache.Endpoint, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	var resp InspectHandleResp
	if _, err := doAsk(ctx, tp, ep, MT_HANDLE_INSPECT, InspectHandleReq{Handle: h}, &resp); err != nil {
		return nil, err
	}
	return &resp.Info, nil
}

func OpenAs(ctx context.Context, tp Transport, ep blobcache.Endpoint, target blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	var resp OpenAsResp
	if _, err := doAsk(ctx, tp, ep, MT_OPEN_AS, OpenAsReq{Target: target, Mask: mask}, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func OpenFrom(ctx context.Context, tp Transport, ep blobcache.Endpoint, base blobcache.Handle, target blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	var resp OpenFromResp
	if _, err := doAsk(ctx, tp, ep, MT_OPEN_FROM, OpenFromReq{Base: base, Target: target, Mask: mask}, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func Await(ctx context.Context, tp Transport, ep blobcache.Endpoint, cond blobcache.Conditions) error {
	var resp AwaitResp
	if _, err := doAsk(ctx, tp, ep, MT_VOLUME_AWAIT, AwaitReq{Cond: cond}, &resp); err != nil {
		return err
	}
	return nil
}

func CreateVolume(ctx context.Context, tp Transport, ep blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	var resp CreateVolumeResp
	if _, err := doAsk(ctx, tp, ep, MT_CREATE_VOLUME, CreateVolumeReq{Spec: vspec}, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func CloneVolume(ctx context.Context, tp Transport, ep blobcache.Endpoint, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	var resp CloneVolumeResp
	if _, err := doAsk(ctx, tp, ep, MT_VOLUME_CLONE, CloneVolumeReq{Volume: volh}, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func BeginTx(ctx context.Context, tp Transport, ep blobcache.Endpoint, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	var resp BeginTxResp
	if _, err := doAsk(ctx, tp, ep, MT_VOLUME_BEGIN_TX, BeginTxReq{Volume: volh, Params: txp}, &resp); err != nil {
		return nil, err
	}
	return &resp.Tx, nil
}

func InspectTx(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	var resp InspectTxResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_INSPECT, InspectTxReq{Tx: tx}, &resp); err != nil {
		return nil, err
	}
	return &resp.Info, nil
}

func Commit(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, root *[]byte) error {
	var resp CommitResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_COMMIT, CommitReq{Tx: tx, Root: root}, &resp); err != nil {
		return err
	}
	return nil
}

func Abort(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle) error {
	var resp AbortResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_ABORT, AbortReq{Tx: tx}, &resp); err != nil {
		return err
	}
	return nil
}

func Load(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, dst *[]byte) error {
	var resp LoadResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_LOAD, LoadReq{Tx: tx}, &resp); err != nil {
		return err
	}
	*dst = append((*dst)[:0], resp.Root...)
	return nil
}

func Save(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, src []byte) error {
	var resp SaveResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_SAVE, SaveReq{Tx: tx, Root: src}, &resp); err != nil {
		return err
	}
	return nil
}

func Post(ctx context.Context, tp Transport, ep blobcache.Endpoint, txh blobcache.Handle, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
	var body []byte
	body = append(body, txh.OID[:]...)
	body = append(body, txh.Secret[:]...)
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
	if err := tp.Ask(ctx, ep, reqMsg, &respMsg); err != nil {
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
	return theirCID, nil
}

func Get(ctx context.Context, tp Transport, ep blobcache.Endpoint, txh blobcache.Handle, hf blobcache.HashFunc, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
	var body []byte
	body = append(body, txh.OID[:]...)
	body = append(body, txh.Secret[:]...)
	body = append(body, cid[:]...)
	var reqMsg Message
	reqMsg.SetCode(MT_TX_GET)
	reqMsg.SetBody(body)

	var respMsg Message
	if err := tp.Ask(ctx, ep, reqMsg, &respMsg); err != nil {
		return 0, err
	}
	if respMsg.Header().Code().IsError() {
		return 0, ParseWireError(respMsg.Header().Code(), respMsg.Body())
	}
	if !respMsg.Header().Code().IsOK() {
		return 0, fmt.Errorf("reply message has non-OK code: %d", respMsg.Header().Code())
	}
	respBody := respMsg.Body()
	if err := blobcache.CheckBlob(hf, salt, &cid, respBody); err != nil {
		return 0, err
	}
	if len(respMsg.Body()) > len(buf) {
		return 0, fmt.Errorf("buffer too short")
	}
	copy(buf, respMsg.Body())
	return len(respMsg.Body()), nil
}

func Exists(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	var resp ExistsResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_EXISTS, ExistsReq{Tx: tx, CIDs: cids}, &resp); err != nil {
		return err
	}
	copy(dst, resp.Exists)
	return nil
}

func Delete(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID) error {
	var resp DeleteResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_DELETE, DeleteReq{Tx: tx, CIDs: cids}, &resp); err != nil {
		return err
	}
	return nil
}

func AddFrom(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID, srcTxns []blobcache.Handle, success []bool) error {
	var resp AddFromResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_ADD_FROM, AddFromReq{Tx: tx, CIDs: cids, Srcs: srcTxns}, &resp); err != nil {
		return err
	}
	copy(success, resp.Added)
	return nil
}

func AllowLink(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, subvol blobcache.Handle) error {
	var resp AllowLinkResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_ALLOW_LINK, AllowLinkReq{Tx: tx, Subvol: subvol}, &resp); err != nil {
		return err
	}
	return nil
}

func Visit(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID) error {
	var resp VisitResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_VISIT, VisitReq{Tx: tx, CIDs: cids}, &resp); err != nil {
		return err
	}
	return nil
}

func IsVisited(ctx context.Context, tp Transport, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	var resp IsVisitedResp
	if _, err := doAsk(ctx, tp, ep, MT_TX_IS_VISITED, IsVisitedReq{Tx: tx, CIDs: cids}, &resp); err != nil {
		return err
	}
	copy(dst, resp.Visited)
	return nil
}

func doAsk[Req interface{ Marshal(out []byte) []byte }, Resp interface{ Unmarshal(data []byte) error }](ctx context.Context, node Transport, remote blobcache.Endpoint, code MessageType, req Req, zeroResp Resp) (Resp, error) {
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

func InspectVolume(ctx context.Context, tp Transport, ep blobcache.Endpoint, vol blobcache.Handle) (*blobcache.VolumeInfo, error) {
	resp, err := doAsk(ctx, tp, ep, MT_VOLUME_INSPECT, InspectVolumeReq{
		Volume: vol,
	}, &InspectVolumeResp{})
	if err != nil {
		return nil, err
	}
	return &resp.Info, nil
}
