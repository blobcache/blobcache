package remotevol

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/volumes"
)

var _ volumes.System[Params, *Volume] = &System{}

type Params = blobcache.VolumeBackend_Remote

type System struct {
	node *atomic.Pointer[bcnet.Node]

	mu     sync.RWMutex
	remote map[Params]*Volume
}

func New(node *atomic.Pointer[bcnet.Node]) System {
	return System{
		node:   node,
		remote: make(map[Params]*Volume),
	}
}

func (sys *System) Up(ctx context.Context, p Params) (*Volume, error) {
	node := sys.node.Load()
	if node == nil {
		return nil, fmt.Errorf("bcremote: cannot open remote volume, no node")
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	vol, exists := sys.remote[p]
	if exists {
		return vol, nil
	}
	// TODO: shouldn't use the network with the lock held.
	volh, info, err := bcp.OpenFiat(ctx, node, p.Endpoint, p.Volume, blobcache.Action_ALL)
	if err != nil {
		return nil, err
	}
	return NewVolume(node, p.Endpoint, *volh, info), nil
}

func (sys *System) Drop(ctx context.Context, vol *Volume) error {
	sys.mu.Lock()
	defer sys.mu.Unlock()
	delete(sys.remote, *vol.Info().Backend.Remote)
	return nil
}
