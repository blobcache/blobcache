// Package bcp implements the Blobcache Protocol (BCP).
package bcp

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

type Asker interface {
	Ask(ctx context.Context, remote blobcache.Endpoint, req Message, resp *Message) error
}

type Teller interface {
	Tell(ctx context.Context, remote blobcache.Endpoint, req Message) error
}

func Ping(ctx context.Context, tp Asker, ep blobcache.Endpoint) error {
	var resp Message
	var req Message
	req.SetCode(MT_PING)
	if err := tp.Ask(ctx, ep, req, &resp); err != nil {
		return err
	}
	return nil
}

func Endpoint(ctx context.Context, tp Asker) (blobcache.Endpoint, error) {
	var resp EndpointResp
	ep := blobcache.Endpoint{}
	if err := doAsk(ctx, tp, ep, MT_ENDPOINT, EndpointReq{}, &resp); err != nil {
		return blobcache.Endpoint{}, err
	}
	return resp.Endpoint, nil
}

func Drop(ctx context.Context, tp Asker, ep blobcache.Endpoint, h blobcache.Handle) error {
	var resp DropResp
	if err := doAsk(ctx, tp, ep, MT_HANDLE_DROP, DropReq{Handle: h}, &resp); err != nil {
		return err
	}
	return nil
}

func KeepAlive(ctx context.Context, tp Asker, ep blobcache.Endpoint, hs []blobcache.Handle) error {
	var resp KeepAliveResp
	if err := doAsk(ctx, tp, ep, MT_HANDLE_KEEP_ALIVE, KeepAliveReq{Handles: hs}, &resp); err != nil {
		return err
	}
	return nil
}

func Share(ctx context.Context, tp Asker, ep blobcache.Endpoint, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	var resp ShareResp
	if err := doAsk(ctx, tp, ep, MT_HANDLE_SHARE, ShareReq{Handle: h, Peer: to, Mask: mask}, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func InspectHandle(ctx context.Context, tp Asker, ep blobcache.Endpoint, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	var resp InspectHandleResp
	if err := doAsk(ctx, tp, ep, MT_HANDLE_INSPECT, InspectHandleReq{Handle: h}, &resp); err != nil {
		return nil, err
	}
	return &resp.Info, nil
}

func OpenFiat(ctx context.Context, tp Asker, ep blobcache.Endpoint, target blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, *blobcache.VolumeInfo, error) {
	var resp OpenFiatResp
	if err := doAsk(ctx, tp, ep, MT_OPEN_FIAT, OpenFiatReq{Target: target, Mask: mask}, &resp); err != nil {
		return nil, nil, err
	}
	if err := resp.Info.HashAlgo.Validate(); err != nil {
		return nil, nil, err
	}
	return &resp.Handle, &resp.Info, nil
}

func OpenFrom(ctx context.Context, tp Asker, ep blobcache.Endpoint, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, *blobcache.VolumeInfo, error) {
	var resp OpenFromResp
	if err := doAsk(ctx, tp, ep, MT_OPEN_FROM, OpenFromReq{Base: base, Token: ltok, Mask: mask}, &resp); err != nil {
		return nil, nil, err
	}
	if err := resp.Info.HashAlgo.Validate(); err != nil {
		return nil, nil, err
	}
	return &resp.Handle, &resp.Info, nil
}

func CreateVolume(ctx context.Context, tp Asker, ep blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	var resp CreateVolumeResp
	if err := doAsk(ctx, tp, ep, MT_CREATE_VOLUME, CreateVolumeReq{Spec: vspec}, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func CloneVolume(ctx context.Context, tp Asker, ep blobcache.Endpoint, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	var resp CloneVolumeResp
	if err := doAsk(ctx, tp, ep, MT_VOLUME_CLONE, CloneVolumeReq{Volume: volh}, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func BeginTx(ctx context.Context, tp Asker, ep blobcache.Endpoint, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, *blobcache.TxInfo, error) {
	var resp BeginTxResp
	if err := doAsk(ctx, tp, ep, MT_VOLUME_BEGIN_TX, BeginTxReq{Volume: volh, Params: txp}, &resp); err != nil {
		return nil, nil, err
	}
	return &resp.Tx, &resp.Info, nil
}

func InspectTx(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	var resp InspectTxResp
	if err := doAsk(ctx, tp, ep, MT_TX_INSPECT, InspectTxReq{Tx: tx}, &resp); err != nil {
		return nil, err
	}
	return &resp.Info, nil
}

func Commit(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, root *[]byte) error {
	var resp CommitResp
	if err := doAsk(ctx, tp, ep, MT_TX_COMMIT, CommitReq{Tx: tx, Root: root}, &resp); err != nil {
		return err
	}
	return nil
}

func Abort(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle) error {
	var resp AbortResp
	if err := doAsk(ctx, tp, ep, MT_TX_ABORT, AbortReq{Tx: tx}, &resp); err != nil {
		return err
	}
	return nil
}

func Load(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, dst *[]byte) error {
	var resp LoadResp
	if err := doAsk(ctx, tp, ep, MT_TX_LOAD, LoadReq{Tx: tx}, &resp); err != nil {
		return err
	}
	*dst = append((*dst)[:0], resp.Root...)
	return nil
}

func Save(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, src []byte) error {
	var resp SaveResp
	if err := doAsk(ctx, tp, ep, MT_TX_SAVE, SaveReq{Tx: tx, Root: src}, &resp); err != nil {
		return err
	}
	return nil
}

func Post(ctx context.Context, tp Asker, ep blobcache.Endpoint, txh blobcache.Handle, salt *blobcache.CID, data []byte) (blobcache.CID, error) {
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
		return blobcache.CID{}, parseWireError(respMsg.Header().Code(), respMsg.Body())
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

func Get(ctx context.Context, tp Asker, ep blobcache.Endpoint, txh blobcache.Handle, hf blobcache.HashFunc, cid blobcache.CID, salt *blobcache.CID, buf []byte) (int, error) {
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
		return 0, parseWireError(respMsg.Header().Code(), respMsg.Body())
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

func Exists(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	var resp ExistsResp
	if err := doAsk(ctx, tp, ep, MT_TX_EXISTS, ExistsReq{Tx: tx, CIDs: cids}, &resp); err != nil {
		return err
	}
	copy(dst, resp.Exists)
	return nil
}

func Delete(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID) error {
	var resp DeleteResp
	if err := doAsk(ctx, tp, ep, MT_TX_DELETE, DeleteReq{Tx: tx, CIDs: cids}, &resp); err != nil {
		return err
	}
	return nil
}

func AddFrom(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID, srcTxns []blobcache.Handle, success []bool) error {
	var resp AddFromResp
	if err := doAsk(ctx, tp, ep, MT_TX_ADD_FROM, AddFromReq{Tx: tx, CIDs: cids, Srcs: srcTxns}, &resp); err != nil {
		return err
	}
	copy(success, resp.Added)
	return nil
}

func Visit(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID) error {
	var resp VisitResp
	if err := doAsk(ctx, tp, ep, MT_TX_VISIT, VisitReq{Tx: tx, CIDs: cids}, &resp); err != nil {
		return err
	}
	return nil
}

func IsVisited(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	var resp IsVisitedResp
	if err := doAsk(ctx, tp, ep, MT_TX_IS_VISITED, IsVisitedReq{Tx: tx, CIDs: cids}, &resp); err != nil {
		return err
	}
	copy(dst, resp.Visited)
	return nil
}

func Link(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, subvol blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	var resp LinkResp
	if err := doAsk(ctx, tp, ep, MT_TX_LINK, LinkReq{Tx: tx, Subvol: subvol, Mask: mask}, &resp); err != nil {
		return nil, err
	}
	return &resp.Token, nil
}

func Unlink(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	var resp UnlinkResp
	if err := doAsk(ctx, tp, ep, MT_TX_UNLINK, UnlinkReq{Tx: tx, Targets: targets}, &resp); err != nil {
		return err
	}
	return nil
}

func VisitLinks(ctx context.Context, tp Asker, ep blobcache.Endpoint, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	var resp VisitLinksResp
	if err := doAsk(ctx, tp, ep, MT_TX_VISIT_LINKS, VisitLinksReq{Tx: tx, Targets: targets}, &resp); err != nil {
		return err
	}
	return nil
}

func InspectVolume(ctx context.Context, tp Asker, ep blobcache.Endpoint, vol blobcache.Handle) (*blobcache.VolumeInfo, error) {
	resp := &InspectVolumeResp{}
	if err := doAsk(ctx, tp, ep, MT_VOLUME_INSPECT, InspectVolumeReq{
		Volume: vol,
	}, resp); err != nil {
		return nil, err
	}
	return &resp.Info, nil
}

func CreateQueue(ctx context.Context, tp Asker, ep blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	var resp CreateQueueResp
	if err := doAsk(ctx, tp, ep, MT_QUEUE_CREATE, CreateQueueReq{Spec: qspec}, &resp); err != nil {
		return nil, err
	}
	return &resp.Handle, nil
}

func Next(ctx context.Context, tp Asker, ep blobcache.Endpoint, qh blobcache.Handle, buf []blobcache.Message, opts blobcache.NextOpts) (int, error) {
	var resp NextResp
	if err := doAsk(ctx, tp, ep, MT_QUEUE_NEXT, NextReq{Opts: opts, Max: len(buf)}, &resp); err != nil {
		return 0, err
	}
	return len(resp.Messages), nil
}

func Insert(ctx context.Context, tp Asker, ep blobcache.Endpoint, from *blobcache.Endpoint, qh blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	var resp InsertResp
	if err := doAsk(ctx, tp, ep, MT_QUEUE_INSERT, InsertReq{Messages: msgs}, &resp); err != nil {
		return nil, err
	}
	return &blobcache.InsertResp{Success: resp.Success}, nil
}

func SubToVolume(ctx context.Context, tp Asker, ep blobcache.Endpoint, qh blobcache.Handle, volh blobcache.Handle) error {
	var resp SubToVolumeResp
	if err := doAsk(ctx, tp, ep, MT_QUEUE_SUB_TO_VOLUME, SubToVolumeReq{Queue: qh, Volume: volh}, &resp); err != nil {
		return err
	}
	return nil
}

func doAsk[Req Sendable, Resp interface{ Unmarshal(data []byte) error }](ctx context.Context, node Asker, remote blobcache.Endpoint, code MessageType, req Req, resp Resp) error {
	reqData := req.Marshal(nil)
	var reqMsg Message
	reqMsg.SetCode(code)
	reqMsg.SetBody(reqData)
	var respMsg Message
	if err := node.Ask(ctx, remote, reqMsg, &respMsg); err != nil {
		return err
	}
	if respMsg.Header().Code().IsError() {
		return parseWireError(respMsg.Header().Code(), respMsg.Body())
	}
	if !respMsg.Header().Code().IsOK() {
		return fmt.Errorf("reply message has non-OK code: %d", respMsg.Header().Code())
	}
	if err := resp.Unmarshal(respMsg.Body()); err != nil {
		return err
	}
	return nil
}

func doTell(ctx context.Context, tp Teller, ep blobcache.Endpoint, code MessageType, x Sendable) error {
	var x2 Message
	x2.SetCode(code)
	x2.SetBody(x.Marshal(nil))
	return tp.Tell(ctx, ep, x2)
}
