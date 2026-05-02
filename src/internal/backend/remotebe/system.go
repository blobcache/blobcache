package remotebe

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/bcp"
	"go.brendoncarroll.net/exp/singleflight"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

var _ backend.VolumeSystem[Params, *Volume] = &System{}

type Params = blobcache.VolumeBackend_Remote

type System struct {
	node *atomic.Pointer[bcnet.Node]

	mu     sync.RWMutex
	remote map[Params]*Volume
	sf     singleflight.Group[Params, *Volume]
}

type ShareInResult struct {
	Volume *Volume
	Queue  *Queue
}

func New(node *atomic.Pointer[bcnet.Node]) System {
	return System{
		node:   node,
		remote: make(map[Params]*Volume),
	}
}

func (sys *System) VolumeUp(ctx context.Context, p Params) (*Volume, error) {
	node := sys.node.Load()
	if node == nil {
		return nil, fmt.Errorf("bcremote: cannot open remote volume, no node")
	}
	vol, err, _ := sys.sf.Do(p, func() (*Volume, error) {
		sys.mu.Lock()
		vol, exists := sys.remote[p]
		sys.mu.Unlock()
		if exists {
			return vol, nil
		}
		logctx.Info(ctx, "opening remote volume...", zap.Stringer("ip_port", p.Endpoint.IPPort))
		volh, info, err := bcp.OpenFiat(ctx, node, p.Endpoint, p.Volume, blobcache.Action_ALL)
		if err != nil {
			return nil, err
		}
		logctx.Info(ctx, "successfully opened remote volume", zap.Stringer("endpoint", p.Endpoint), zap.Stringer("volume", p.Volume))
		vol = NewVolume(sys, node, p.Endpoint, *volh, info)
		sys.mu.Lock()
		sys.remote[p] = vol
		sys.mu.Unlock()
		return vol, nil
	})
	return vol, err
}

func (sys *System) VolumeDown(ctx context.Context, vol *Volume) error {
	sys.mu.Lock()
	defer sys.mu.Unlock()
	delete(sys.remote, *vol.GetBackend().Remote)
	// zero the handle so it can't be used.
	vol.h.Secret = [16]byte{}
	return nil
}

func (sys *System) CreateVolume(ctx context.Context, ep blobcache.Endpoint, vspec blobcache.VolumeSpec) (*Volume, error) {
	hashAlgo := vspec.Config().HashAlgo
	node := sys.node.Load()
	if node == nil {
		return nil, fmt.Errorf("bcremote: cannot create remote volume, no node")
	}
	// The host cannot be set to non-zero when using bcnet.Node
	resp, err := bcp.CreateVolume(ctx, node, ep, bcp.CreateVolumeReq{Spec: vspec})
	if err != nil {
		return nil, err
	}
	h := resp.Handle
	vol := NewVolume(sys, node, ep, resp.Handle, &resp.Info)
	sys.mu.Lock()
	defer sys.mu.Unlock()
	p := Params{Endpoint: ep, Volume: h.OID, HashAlgo: hashAlgo}
	if _, exists := sys.remote[p]; exists {
		return nil, fmt.Errorf("peer %v reused OID %v  in reply to CreateVolume", ep.Node, h.OID)
	}
	sys.remote[p] = vol
	return vol, nil
}

func (sys *System) VolumeDestroy(ctx context.Context, vol *Volume) error {
	// no way for us to destroy a remote volume
	return nil
}

// CreateQueue creates a queue on the remote node and returns a local proxy.
func (sys *System) CreateQueue(ctx context.Context, ep blobcache.Endpoint, qspec blobcache.QueueSpec) (*Queue, *blobcache.QueueInfo, error) {
	node := sys.node.Load()
	if node == nil {
		return nil, nil, fmt.Errorf("bcremote: cannot create remote queue, no node")
	}
	qh, err := bcp.CreateQueue(ctx, node, ep, qspec)
	if err != nil {
		return nil, nil, err
	}
	info, err := bcp.InspectQueue(ctx, node, ep, *qh)
	if err != nil {
		return nil, nil, err
	}
	return NewQueue(sys, node, ep, *qh, info.Config), &info, nil
}

// QueueUp opens an existing remote queue and returns a local proxy.
func (sys *System) QueueUp(ctx context.Context, p *blobcache.QueueBackend_Remote) (*Queue, error) {
	node := sys.node.Load()
	if node == nil {
		return nil, fmt.Errorf("bcremote: cannot open remote queue, no node")
	}
	h, _, err := bcp.OpenFiat(ctx, node, p.Endpoint, p.OID, blobcache.Action_ALL)
	if err != nil {
		return nil, err
	}
	info, err := bcp.InspectQueue(ctx, node, p.Endpoint, *h)
	if err != nil {
		return nil, err
	}
	return NewQueue(sys, node, p.Endpoint, *h, info.Config), nil
}

func (sys *System) SubToVol(ctx context.Context, vol *Volume, q backend.Queue, spec blobcache.VolSubSpec) error {
	rq, ok := q.(*Queue)
	if !ok {
		return fmt.Errorf("bcremote: SubToVol requires a remote queue, got %T", q)
	}
	if rq.ep.Node != vol.ep.Node {
		return fmt.Errorf("bcremote: SubToVol requires the queue and volume to be on the same peer")
	}
	return bcp.SubToVolume(ctx, vol.n, vol.ep, rq.h, vol.h, spec)
}

func (sys *System) OpenFrom(ctx context.Context, base *Volume, token blobcache.LinkToken, mask blobcache.ActionSet) (blobcache.ActionSet, *Volume, error) {
	h, info, err := bcp.OpenFrom(ctx, base.n, base.ep, base.h, token, mask)
	if err != nil {
		return 0, nil, err
	}
	hinfo, err := bcp.InspectHandle(ctx, base.n, base.ep, *h)
	if err != nil {
		return 0, nil, err
	}
	return hinfo.Rights, NewVolume(sys, base.n, base.ep, *h, info), nil
}

func (sys *System) ShareIn(ctx context.Context, ep blobcache.Endpoint, h blobcache.Handle, info blobcache.Info) (*ShareInResult, error) {
	node := sys.node.Load()
	if node == nil {
		return nil, fmt.Errorf("bcremote: cannot share-in remote object, no node")
	}
	ret := &ShareInResult{}
	switch {
	case info.Volume != nil:
		ret.Volume = NewVolume(sys, node, ep, h, info.Volume)
		return ret, nil
	case info.Queue != nil:
		ret.Queue = NewQueue(sys, node, ep, h, info.Queue.Config)
		return ret, nil
	default:
		return nil, fmt.Errorf("bcremote: cannot share-in unknown object type")
	}
}
