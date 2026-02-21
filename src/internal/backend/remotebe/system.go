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
)

var _ backend.VolumeSystem[Params, *Volume] = &System{}

type Params = blobcache.VolumeBackend_Remote

type System struct {
	node *atomic.Pointer[bcnet.Node]

	mu     sync.RWMutex
	remote map[Params]*Volume
	sf     singleflight.Group[Params, *Volume]
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
		volh, info, err := bcp.OpenFiat(ctx, node, p.Endpoint, p.Volume, blobcache.Action_ALL)
		if err != nil {
			return nil, err
		}
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

func (sys *System) VolumeDestroy(ctx context.Context, vol *Volume) error {
	// no way for us to destroy a remote volume
	return nil
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
