package globalvol

import (
	"context"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/pubsub"
	"blobcache.io/blobcache/src/internal/svcgroup"
	"blobcache.io/blobcache/src/internal/volumes"
	"blobcache.io/blobcache/src/schema"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

type Params struct {
	Spec blobcache.VolumeBackend_Global
	// Settled is the volume to keep the consensus state in.
	Settled volumes.Volume
	Schema  schema.Schema
}

// ConsensusSM is a consensus state machine
type ConsensusSM interface {
	Timeout(out []blobcache.Message, now time.Time) []blobcache.Message
	Handle(out []blobcache.Message, in blobcache.Message)
}

var _ volumes.System[Params, *Volume] = &System{}

type System struct {
	env Env

	mu   sync.RWMutex
	vols map[blobcache.CID]*Volume
	sg   svcgroup.Group
}

type Env struct {
	Background context.Context
	// Hub is used to subscribe to events
	Hub  *pubsub.Hub
	Send func(blobcache.Message) error
}

func New(env Env) *System {
	sys := &System{
		env: env,
		sg:  svcgroup.New(env.Background),
	}
	sys.sg.Always(sys.timerLoop)
	return sys
}

func (sys *System) Up(ctx context.Context, params Params) (*Volume, error) {
	k := params.Spec.Essence.ID()
	sys.mu.Lock()
	defer sys.mu.Unlock()
	if sys.vols == nil {
		sys.vols = make(map[blobcache.CID]*Volume)
	}
	if vol, exists := sys.vols[k]; exists {
		return vol, nil
	}

	incoming := make(chan *blobcache.Message)
	vol := newVolume(params.Spec, params.Settled, incoming)
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
	if sys.env.Hub.Unsubscribe(MkTID(vol.id), vol.incoming) {
		close(vol.incoming)
	}
	delete(sys.vols, vol.id)
	return nil
}

func (sys *System) Close() error {
	sys.sg.Shutdown()
	return nil
}

func (sys *System) timerLoop(ctx context.Context) error {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	now := time.Now()
	for {
		// Iterate through volumes and call timer method on their state machines.
		func() {
			sys.mu.Lock()
			defer sys.mu.Unlock()
			for _, vol := range sys.vols {
				outgoing := vol.sm.Timeout(nil, now)
				for _, msg := range outgoing {
					sys.env.Send(msg)
				}
			}
		}()
		select {
		case <-tick.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// MkTID returns a TID from a SchemaSpec
func MkTID(cid blobcache.CID) blobcache.TID {
	hf := blobcache.HashAlgo_CSHAKE256.HashFunc()
	return blobcache.TID(hf(nil, cid[:]))
}
