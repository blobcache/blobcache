package bcnet

import (
	"context"
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
	n    Transport
	ep   blobcache.Endpoint
	h    blobcache.Handle
	info *blobcache.VolumeInfo
}

func NewVolume(node Transport, ep blobcache.Endpoint, h blobcache.Handle, info *blobcache.VolumeInfo) *Volume {
	return &Volume{
		n:    node,
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
	_, err := doAsk(ctx, v.n, v.ep, MT_VOLUME_AWAIT, AwaitReq{
		Cond: blobcache.Conditions{},
	}, &AwaitResp{})
	if err != nil {
		return err
	}
	loadResp, err := doAsk(ctx, v.n, v.ep, MT_TX_LOAD, LoadReq{
		Tx: v.h,
	}, &LoadResp{})
	if err != nil {
		return err
	}
	*next = loadResp.Root
	return nil
}

func (v *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (volumes.Tx, error) {
	resp, err := doAsk(ctx, v.n, v.ep, MT_VOLUME_BEGIN_TX, BeginTxReq{
		Volume: v.h,
		Params: spec,
	}, &BeginTxResp{})
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
	n       Transport
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
	_, err := doAsk(ctx, tx.n, tx.ep, MT_TX_COMMIT, CommitReq{
		Tx:   tx.h,
		Root: root,
	}, &CommitResp{})
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Abort(ctx context.Context) error {
	_, err := doAsk(ctx, tx.n, tx.ep, MT_TX_ABORT, AbortReq{
		Tx: tx.h,
	}, &AbortResp{})
	return err
}

func (tx *Tx) Load(ctx context.Context, dst *[]byte) error {
	resp, err := doAsk(ctx, tx.n, tx.ep, MT_TX_LOAD, LoadReq{
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

func (tx *Tx) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	theirCID, err := Post(ctx, tx.n, tx.ep, tx.h, opts.Salt, data)
	if err != nil {
		return blobcache.CID{}, err
	}
	ourCID := tx.Hash(opts.Salt, data)
	if theirCID != ourCID {
		return blobcache.CID{}, fmt.Errorf("hash mismatch: ourCID=%s, theirCID=%s", ourCID, theirCID)
	}
	return theirCID, nil
}

func (tx *Tx) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return Get(ctx, tx.n, tx.ep, tx.h, tx.Hash, cid, opts.Salt, buf)
}

func (tx *Tx) Delete(ctx context.Context, cids []blobcache.CID) error {
	_, err := doAsk(ctx, tx.n, tx.ep, MT_TX_DELETE, DeleteReq{
		Tx:   tx.h,
		CIDs: cids,
	}, &DeleteResp{})
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	resp, err := doAsk(ctx, tx.n, tx.ep, MT_TX_EXISTS, ExistsReq{
		Tx:   tx.h,
		CIDs: cids,
	}, &ExistsResp{})
	if err != nil {
		return err
	}
	copy(dst, resp.Exists)
	return nil
}

func (tx *Tx) MaxSize() int {
	return int(tx.volInfo.MaxSize)
}

func (tx *Tx) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	hf := tx.volInfo.HashAlgo.HashFunc()
	return hf(salt, data)
}

func (tx *Tx) AllowLink(ctx context.Context, subvol blobcache.Handle) error {
	_, err := doAsk(ctx, tx.n, tx.ep, MT_TX_ALLOW_LINK, AllowLinkReq{
		Tx:     tx.h,
		Subvol: subvol,
	}, &AllowLinkResp{})
	return err
}

func (tx *Tx) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	resp, err := doAsk(ctx, tx.n, tx.ep, MT_TX_IS_VISITED, IsVisitedReq{
		Tx:   tx.h,
		CIDs: cids,
	}, &IsVisitedResp{})
	copy(dst, resp.Visited)
	return err
}

func (tx *Tx) Visit(ctx context.Context, cids []blobcache.CID) error {
	_, err := doAsk(ctx, tx.n, tx.ep, MT_TX_VISIT, VisitReq{
		Tx:   tx.h,
		CIDs: cids,
	}, &VisitResp{})
	return err
}

func OpenVolumeFrom(ctx context.Context, tp Transport, ep blobcache.Endpoint, base blobcache.Handle, target blobcache.OID, mask blobcache.ActionSet) (*Volume, error) {
	resp, err := doAsk(ctx, tp, ep, MT_OPEN_FROM, OpenFromReq{
		Base:   base,
		Target: target,
		Mask:   mask,
	}, &OpenFromResp{})
	if err != nil {
		return nil, err
	}
	if err := resp.Info.HashAlgo.Validate(); err != nil {
		return nil, err
	}
	return NewVolume(tp, ep, resp.Handle, &resp.Info), nil
}

func OpenVolumeAs(ctx context.Context, tp Transport, ep blobcache.Endpoint, target blobcache.OID, mask blobcache.ActionSet) (*Volume, error) {
	resp, err := doAsk(ctx, tp, ep, MT_OPEN_AS, OpenAsReq{
		Target: target,
		Mask:   mask,
	}, &OpenAsResp{})
	if err != nil {
		return nil, err
	}
	if err := resp.Info.HashAlgo.Validate(); err != nil {
		return nil, err
	}
	return NewVolume(tp, ep, resp.Handle, &resp.Info), nil
}
