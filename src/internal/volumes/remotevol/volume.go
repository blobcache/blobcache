package remotevol

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/volumes"
)

var (
	_ volumes.Volume = (*Volume)(nil)
	_ volumes.Tx     = (*Tx)(nil)
)

// Volume is a remote volume.
type Volume struct {
	n    bcnet.Transport
	ep   blobcache.Endpoint
	h    blobcache.Handle
	info *blobcache.VolumeInfo
}

func NewVolume(node bcnet.Transport, ep blobcache.Endpoint, h blobcache.Handle, info *blobcache.VolumeInfo) *Volume {
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
	return bcnet.Await(ctx, v.n, v.ep, blobcache.Conditions{})
}

func (v *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (volumes.Tx, error) {
	txh, info, err := bcnet.BeginTx(ctx, v.n, v.ep, v.h, spec)
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

func (v *Volume) ReadLinks(ctx context.Context, dst volumes.LinkSet) error {
	return fmt.Errorf("remotevol: ReadLinks not implemented")
}

// Tx is a transaction on a remote volume.
type Tx struct {
	vol    *Volume
	h      blobcache.Handle
	params blobcache.TxParams
	info   *blobcache.TxInfo

	root []byte
}

func (tx *Tx) Volume() volumes.Volume {
	return tx.vol
}

func (tx *Tx) Commit(ctx context.Context) error {
	if !tx.params.Mutate {
		return blobcache.ErrTxReadOnly{}
	}
	var root *[]byte
	if tx.root != nil {
		root = &tx.root
	}
	return bcnet.Commit(ctx, tx.vol.n, tx.vol.ep, tx.h, root)
}

func (tx *Tx) Abort(ctx context.Context) error {
	return bcnet.Abort(ctx, tx.vol.n, tx.vol.ep, tx.h)
}

func (tx *Tx) Load(ctx context.Context, dst *[]byte) error {
	return bcnet.Load(ctx, tx.vol.n, tx.vol.ep, tx.h, dst)
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
	theirCID, err := bcnet.Post(ctx, tx.vol.n, tx.vol.ep, tx.h, opts.Salt, data)
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
	return bcnet.Get(ctx, tx.vol.n, tx.vol.ep, tx.h, tx.Hash, cid, opts.Salt, buf)
}

func (tx *Tx) Delete(ctx context.Context, cids []blobcache.CID) error {
	return bcnet.Delete(ctx, tx.vol.n, tx.vol.ep, tx.h, cids)
}

func (tx *Tx) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	return bcnet.Exists(ctx, tx.vol.n, tx.vol.ep, tx.h, cids, dst)
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
	return bcnet.IsVisited(ctx, tx.vol.n, tx.vol.ep, tx.h, cids, dst)
}

func (tx *Tx) Visit(ctx context.Context, cids []blobcache.CID) error {
	return bcnet.Visit(ctx, tx.vol.n, tx.vol.ep, tx.h, cids)
}

func (tx *Tx) Link(ctx context.Context, subvol blobcache.OID, mask blobcache.ActionSet) error {
	return fmt.Errorf("remotevol: Link not implemented")
}

func (tx *Tx) Unlink(ctx context.Context, targets []blobcache.OID) error {
	return fmt.Errorf("remotevol: Unlink not implemented")
}

func (tx *Tx) VisitLinks(ctx context.Context, targets []blobcache.OID) error {
	return fmt.Errorf("remotevol: VisitLinks not implemented")
}

func OpenVolumeFrom(ctx context.Context, tp bcnet.Transport, ep blobcache.Endpoint, base blobcache.Handle, target blobcache.OID, mask blobcache.ActionSet) (*Volume, error) {
	volh, info, err := bcnet.OpenFrom(ctx, tp, ep, base, target, mask)
	if err != nil {
		return nil, err
	}
	return NewVolume(tp, ep, *volh, info), nil
}

func OpenVolumeAs(ctx context.Context, tp bcnet.Transport, ep blobcache.Endpoint, target blobcache.OID, mask blobcache.ActionSet) (*Volume, error) {
	volh, info, err := bcnet.OpenFiat(ctx, tp, ep, target, mask)
	if err != nil {
		return nil, err
	}
	return NewVolume(tp, ep, *volh, info), nil
}
