package remotebe

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/internal/bcp"
)

var (
	_ backend.Volume = (*Volume)(nil)
	_ backend.Tx     = (*Tx)(nil)
)

// Volume is a remote volume.
type Volume struct {
	sys  *System
	n    bcp.Asker
	ep   blobcache.Endpoint
	h    blobcache.Handle
	info *blobcache.VolumeInfo
}

func NewVolume(sys *System, node bcp.Asker, ep blobcache.Endpoint, h blobcache.Handle, info *blobcache.VolumeInfo) *Volume {
	return &Volume{
		sys:  sys,
		n:    node,
		ep:   ep,
		h:    h,
		info: info,
	}
}

func (v *Volume) GetBackend() blobcache.VolumeBackend[blobcache.OID] {
	return blobcache.VolumeBackend[blobcache.OID]{
		Remote: &blobcache.VolumeBackend_Remote{
			Endpoint: v.ep,
			Volume:   v.h.OID,
		},
	}
}

func (v *Volume) Endpoint() blobcache.Endpoint {
	return v.ep
}

func (v *Volume) Handle() blobcache.Handle {
	return v.h
}

func (v *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (backend.Tx, error) {
	txh, info, err := bcp.BeginTx(ctx, v.n, v.ep, v.h, spec)
	if err != nil {
		return nil, err
	}
	return &Tx{
		vol:    v,
		params: spec,
		h:      *txh,
		info:   info,
	}, nil
}

func (v *Volume) VolumeDown(ctx context.Context) error {
	return v.sys.VolumeDown(ctx, v)
}

func (v *Volume) AccessSubVolume(ctx context.Context, ltok blobcache.LinkToken) (blobcache.ActionSet, error) {
	h, _, err := bcp.OpenFrom(ctx, v.n, v.ep, v.h, ltok, blobcache.Action_ALL)
	if err != nil {
		return 0, err
	}
	hinfo, err := bcp.InspectHandle(ctx, v.n, v.ep, *h)
	if err != nil {
		return 0, err
	}
	return hinfo.Rights, nil
}

func (v *Volume) GetParams() blobcache.VolumeConfig {
	return v.info.VolumeConfig
}

// Tx is a transaction on a remote volume.
type Tx struct {
	vol    *Volume
	h      blobcache.Handle
	params blobcache.TxParams
	info   *blobcache.TxInfo

	root []byte
}

func (tx *Tx) Params() blobcache.TxParams {
	return tx.params
}

func (tx *Tx) Volume() backend.Volume {
	return tx.vol
}

func (tx *Tx) Commit(ctx context.Context) error {
	if !tx.params.Modify {
		return blobcache.ErrTxReadOnly{}
	}
	var root *[]byte
	if tx.root != nil {
		root = &tx.root
	}
	return bcp.Commit(ctx, tx.vol.n, tx.vol.ep, tx.h, root)
}

func (tx *Tx) Abort(ctx context.Context) error {
	return bcp.Abort(ctx, tx.vol.n, tx.vol.ep, tx.h)
}

func (tx *Tx) Load(ctx context.Context, dst *[]byte) error {
	return bcp.Load(ctx, tx.vol.n, tx.vol.ep, tx.h, dst)
}

func (tx *Tx) Save(ctx context.Context, src []byte) error {
	if !tx.params.Modify {
		return blobcache.ErrTxReadOnly{}
	}
	tx.root = append(tx.root[:0], src...)
	// TODO: we could also send this to the server, but it's probably
	// better to just wait until Commit time.
	return nil
}

func (tx *Tx) Post(ctx context.Context, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	theirCID, err := bcp.Post(ctx, tx.vol.n, tx.vol.ep, tx.h, opts.Salt, data)
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
	return bcp.Get(ctx, tx.vol.n, tx.vol.ep, tx.h, tx.Hash, cid, opts.Salt, buf)
}

func (tx *Tx) Delete(ctx context.Context, cids []blobcache.CID) error {
	return bcp.Delete(ctx, tx.vol.n, tx.vol.ep, tx.h, cids)
}

func (tx *Tx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return bcp.Exists(ctx, tx.vol.n, tx.vol.ep, tx.h, cids, dst)
}

func (tx *Tx) MaxSize() int {
	return int(tx.info.MaxSize)
}

func (tx *Tx) Hash(salt *blobcache.CID, data []byte) blobcache.CID {
	hf := tx.info.HashAlgo.HashFunc()
	return hf(salt, data)
}

func (tx *Tx) IsVisited(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	if len(cids) != len(dst) {
		return fmt.Errorf("cids and dst must have the same length")
	}
	return bcp.IsVisited(ctx, tx.vol.n, tx.vol.ep, tx.h, cids, dst)
}

func (tx *Tx) Visit(ctx context.Context, cids []blobcache.CID) error {
	return bcp.Visit(ctx, tx.vol.n, tx.vol.ep, tx.h, cids)
}

func (tx *Tx) Link(ctx context.Context, svoid blobcache.OID, rights blobcache.ActionSet, targetVol backend.Volume) (*blobcache.LinkToken, error) {
	if !tx.params.Modify {
		return nil, blobcache.ErrTxReadOnly{}
	}
	rvol, ok := targetVol.(*Volume)
	if !ok {
		return nil, fmt.Errorf("remotebe: can only link to remote volumes")
	}
	if rvol.ep.Peer != tx.vol.ep.Peer {
		return nil, fmt.Errorf("remotebe: can only link to volumes on the same peer")
	}
	return bcp.Link(ctx, tx.vol.n, tx.vol.ep, tx.h, rvol.h, rights)
}

func (tx *Tx) Unlink(ctx context.Context, targets []blobcache.LinkToken) error {
	if !tx.params.Modify {
		return blobcache.ErrTxReadOnly{}
	}
	return bcp.Unlink(ctx, tx.vol.n, tx.vol.ep, tx.h, targets)
}

func (tx *Tx) VisitLinks(ctx context.Context, targets []blobcache.LinkToken) error {
	if !tx.params.Modify {
		return blobcache.ErrTxReadOnly{}
	}
	if !tx.params.GCBlobs {
		return blobcache.ErrTxNotGC{Op: "VisitLinks"}
	}
	return bcp.VisitLinks(ctx, tx.vol.n, tx.vol.ep, tx.h, targets)
}
