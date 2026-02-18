package bcsys

import (
	"context"
	"crypto/aes"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bccrypto"
	"go.brendoncarroll.net/exp/slices2"
)

type peerView[LK any, LV LocalVolume[LK]] struct {
	svc       *Service[LK, LV]
	peer      blobcache.PeerID
	tmpSecret *[32]byte
}

func (pv *peerView[LK, LV]) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	return pv.svc.Endpoint(ctx)
}

func (pv *peerView[LK, LV]) Drop(ctx context.Context, h blobcache.Handle) error {
	return pv.svc.Drop(ctx, pv.incoming(h))
}

func (pv *peerView[LK, LV]) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	return pv.svc.KeepAlive(ctx, slices2.Map(hs, pv.incoming))
}

func (pv *peerView[LK, LV]) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	return pv.svc.InspectHandle(ctx, pv.incoming(h))
}

func (pv *peerView[LK, LV]) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	// We need to transform the incoming, but apply no transformation to the outgoing.
	// This returned handle will not work for the caller, unless they are sharing with themselves.
	return pv.svc.Share(ctx, pv.incoming(h), to, mask)
}

func (pv *peerView[LK, LV]) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	pol := pv.svc.env.Policy
	if !pol.CanCreate(pv.peer) {
		return nil, ErrNotAllowed{
			Peer:   pv.peer,
			Action: "CreateVolume",
		}
	}
	if host != nil {
		return nil, fmt.Errorf("peers cannot ask us to create volumes on remote nodes")
	}
	switch {
	case vspec.Local != nil:
	default:
		return nil, fmt.Errorf("volume %v cannot be created remotely", vspec)
	}
	h, err := pv.svc.CreateVolume(ctx, nil, vspec)
	if err != nil {
		return nil, err
	}
	h2 := pv.outgoing(*h)
	return &h2, nil
}

func (pv *peerView[LK, LV]) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return pv.svc.InspectVolume(ctx, pv.incoming(h))
}

func (pv *peerView[LK, LV]) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	pol := pv.svc.env.Policy
	if rights := pol.OpenFiat(pv.peer, x); rights == 0 {
		return nil, ErrNotAllowed{
			Peer:   pv.peer,
			Action: "OpenFiat",
			Target: x,
		}
	} else {
		h, err := pv.svc.OpenFiat(ctx, x, rights&mask)
		if err != nil {
			return nil, err
		}
		h2 := pv.outgoing(*h)
		return &h2, nil
	}
}

func (pv *peerView[LK, LV]) OpenFrom(ctx context.Context, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	h, err := pv.svc.OpenFrom(ctx, pv.incoming(base), ltok, mask)
	if err != nil {
		return nil, err
	}
	h2 := pv.outgoing(*h)
	return &h2, nil
}

func (pv *peerView[LK, LV]) BeginTx(ctx context.Context, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	h, err := pv.svc.BeginTx(ctx, pv.incoming(volh), txp)
	if err != nil {
		return nil, err
	}
	h2 := pv.outgoing(*h)
	return &h2, nil
}

func (pv *peerView[LK, LV]) CloneVolume(ctx context.Context, _ *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	// Ignore the caller parameter provided by upstream; this view is already bound to pv.peer.
	h, err := pv.svc.CloneVolume(ctx, &pv.peer, pv.incoming(volh))
	if err != nil {
		return nil, err
	}
	h2 := pv.outgoing(*h)
	return &h2, nil
}

func (pv *peerView[LK, LV]) InspectTx(ctx context.Context, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	return pv.svc.InspectTx(ctx, pv.incoming(tx))
}

func (pv *peerView[LK, LV]) Commit(ctx context.Context, tx blobcache.Handle) error {
	return pv.svc.Commit(ctx, pv.incoming(tx))
}

func (pv *peerView[LK, LV]) Abort(ctx context.Context, tx blobcache.Handle) error {
	return pv.svc.Abort(ctx, pv.incoming(tx))
}

func (pv *peerView[LK, LV]) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	return pv.svc.Load(ctx, pv.incoming(tx), dst)
}

func (pv *peerView[LK, LV]) Save(ctx context.Context, tx blobcache.Handle, src []byte) error {
	return pv.svc.Save(ctx, pv.incoming(tx), src)
}

func (pv *peerView[LK, LV]) Post(ctx context.Context, tx blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return pv.svc.Post(ctx, pv.incoming(tx), data, opts)
}

func (pv *peerView[LK, LV]) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return pv.svc.Get(ctx, pv.incoming(tx), cid, buf, opts)
}

func (pv *peerView[LK, LV]) Exists(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	return pv.svc.Exists(ctx, pv.incoming(tx), cids, dst)
}

func (pv *peerView[LK, LV]) Delete(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return pv.svc.Delete(ctx, pv.incoming(tx), cids)
}

func (pv *peerView[LK, LV]) Copy(ctx context.Context, tx blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, success []bool) error {
	decSrc := slices2.Map(srcTxns, pv.incoming)
	return pv.svc.Copy(ctx, pv.incoming(tx), decSrc, cids, success)
}

func (pv *peerView[LK, LV]) Visit(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return pv.svc.Visit(ctx, pv.incoming(tx), cids)
}

func (pv *peerView[LK, LV]) IsVisited(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, yesVisited []bool) error {
	return pv.svc.IsVisited(ctx, pv.incoming(tx), cids, yesVisited)
}

func (pv *peerView[LK, LV]) Link(ctx context.Context, tx blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	return pv.svc.Link(ctx, pv.incoming(tx), pv.incoming(target), mask)
}

func (pv *peerView[LK, LV]) Unlink(ctx context.Context, tx blobcache.Handle, ltoks []blobcache.LinkToken) error {
	return pv.svc.Unlink(ctx, pv.incoming(tx), ltoks)
}

func (pv *peerView[LK, LV]) VisitLinks(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkToken) error {
	return pv.svc.VisitLinks(ctx, pv.incoming(tx), targets)
}

func (pv *peerView[LK, LV]) CreateQueue(ctx context.Context, host *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	if host != nil {
		return nil, fmt.Errorf("peers cannot ask us to create queues on remote nodes")
	}
	h, err := pv.svc.CreateQueue(ctx, nil, qspec)
	if err != nil {
		return nil, err
	}
	h2 := pv.outgoing(*h)
	return &h2, nil
}

func (pv *peerView[LK, LV]) InspectQueue(ctx context.Context, q blobcache.Handle) (blobcache.QueueInfo, error) {
	return pv.svc.InspectQueue(ctx, pv.incoming(q))
}

func (pv *peerView[LK, LV]) Dequeue(ctx context.Context, q blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	return pv.svc.Dequeue(ctx, pv.incoming(q), buf, opts)
}

func (pv *peerView[LK, LV]) Enqueue(ctx context.Context, q blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	return pv.svc.Enqueue(ctx, pv.incoming(q), msgs)
}

func (pv *peerView[LK, LV]) SubToVolume(ctx context.Context, q blobcache.Handle, vol blobcache.Handle) error {
	return pv.svc.SubToVolume(ctx, pv.incoming(q), pv.incoming(vol))
}

// outgoing should be called on all outgoing handles intended for pv.peer.
func (pv *peerView[LK, LV]) outgoing(x blobcache.Handle) blobcache.Handle {
	key := derivePeerSecret(pv.tmpSecret, pv.peer)
	ciph, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	ciph.Encrypt(x.Secret[:], x.Secret[:])
	return x
}

// incoming should be called on all incoming handles received from pv.peer.
func (pv *peerView[LK, LV]) incoming(x blobcache.Handle) blobcache.Handle {
	key := derivePeerSecret(pv.tmpSecret, pv.peer)
	ciph, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	ciph.Decrypt(x.Secret[:], x.Secret[:])
	return x
}

func derivePeerSecret(secret *[32]byte, peer blobcache.PeerID) [32]byte {
	return deriveKey(secret, peer[:])
}

func deriveKey(entropy *[32]byte, additional []byte) [32]byte {
	return bccrypto.DeriveKey(blobcache.HashAlgo_CSHAKE256.HashFunc(), entropy, additional)
}
