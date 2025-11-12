package consensusvol

import (
	"context"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/pubsub"
	"blobcache.io/blobcache/src/internal/volumes"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

type Params struct {
	Schema blobcache.SchemaSpec
	State  volumes.Volume
}

var _ volumes.System[Params, *Volume] = &System{}

type System struct {
	env Env

	mu   sync.RWMutex
	vols map[blobcache.TID]*Volume
}

type Env struct {
	Background context.Context
	// Hub is used to subscribe to events
	Hub  *pubsub.Hub
	Send func(blobcache.Message) error
}

func New(env Env) System {
	return System{
		env: env,
	}
}

func (sys *System) Up(ctx context.Context, params Params) (*Volume, error) {
	k := NewTID(params.Schema)
	if sys.vols == nil {
		sys.vols = make(map[blobcache.TID]*Volume)
	}
	sys.mu.Lock()
	defer sys.mu.Unlock()
	if vol, exists := sys.vols[k]; exists {
		return vol, nil
	}
	incoming := make(chan *blobcache.Message)
	vol := newVolume(k, incoming)
	sys.vols[k] = vol
	go func() {
		var outgoing []blobcache.Message
		for tmsg := range incoming {
			if err := vol.Handle(&outgoing, *tmsg); err != nil {
				logctx.Error(ctx, "handling topic message: %w", zap.Error(err))
				continue
			}
			for _, omsg := range outgoing {
				if err := sys.env.Send(omsg); err != nil {
					logctx.Error(ctx, "sending topic message: %w", zap.Error(err))
				}
			}
		}
	}()
	return &Volume{}, nil
}

func (sys *System) Drop(ctx context.Context, vol *Volume) error {
	sys.mu.Lock()
	defer sys.mu.Unlock()
	vol, exists := sys.vols[vol.id]
	if !exists {
		return nil
	}
	if sys.env.Hub.Unsubscribe(vol.id, vol.incoming) {
		close(vol.incoming)
	}
	delete(sys.vols, vol.id)
	return nil
}

// NewTID returns a TID from a SchemaSpec
func NewTID(sch blobcache.SchemaSpec) blobcache.TID {
	hf := blobcache.HashAlgo_CSHAKE256.HashFunc()
	nameSalt := hf(nil, []byte(sch.Name))
	return blobcache.TID(hf(&nameSalt, sch.Params))
}
