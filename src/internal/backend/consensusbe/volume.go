package consensusvol

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend"
)

var _ backend.Volume = &Volume{}

// Volume manages state for a volume.
type Volume struct {
	id       blobcache.TID
	incoming chan *blobcache.Message
}

func newVolume(tid blobcache.TID, incoming chan *blobcache.Message) *Volume {
	return &Volume{
		id:       tid,
		incoming: incoming,
	}
}

func (v *Volume) Handle(out *[]blobcache.Message, msg blobcache.Message) error {
	return nil
}

// Await implements backend.Volume.
func (v *Volume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	panic("unimplemented")
}

// BeginTx implements backend.Volume.
func (v *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (backend.Tx, error) {
	panic("unimplemented")
}

func (v *Volume) VolumeDown(ctx context.Context) error {
	return nil
}

func (v *Volume) AccessSubVolume(ctx context.Context, ltok blobcache.LinkToken) (blobcache.ActionSet, error) {
	panic("unimplemented")
}

func (v *Volume) GetBackend() blobcache.VolumeBackend[blobcache.OID] {
	panic("unimplemented")
}

func (v *Volume) GetParams() blobcache.VolumeConfig {
	panic("unimplemented")
}
