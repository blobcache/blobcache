package bclocal

import (
	"context"

	"blobcache.io/blobcache/src/bclocal/internal/localvol"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
	"blobcache.io/blobcache/src/internal/backend/memory"
)

var _ backend.System[localvol.Params, backend.Volume, blobcache.QueueBackend_Memory, *memory.Queue] = &system{}

type system struct {
	vols   *localvol.System
	queues *memory.System
}

func newSystem(vols *localvol.System, queues *memory.System) *system {
	return &system{
		vols:   vols,
		queues: queues,
	}
}

func (s *system) VolumeUp(ctx context.Context, spec localvol.Params) (backend.Volume, error) {
	return s.vols.VolumeUp(ctx, spec)
}

func (s *system) CreateQueue(ctx context.Context, spec blobcache.QueueBackend_Memory) (*memory.Queue, error) {
	return s.queues.CreateQueue(ctx, spec)
}

func (s *system) VolumeDestroy(ctx context.Context, vol backend.Volume) error {
	return s.vols.VolumeDestroy(ctx, vol.(*localvol.Volume))
}
