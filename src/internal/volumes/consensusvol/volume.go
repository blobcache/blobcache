package consensusvol

import (
	"context"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/volumes"
)

var _ volumes.Volume = &Volume{}

// Volume manages state for a volume.
type Volume struct {
	id       blobcache.TID
	incoming chan *blobcache.TopicMessage
}

func newVolume(tid blobcache.TID, incoming chan *blobcache.TopicMessage) *Volume {
	return &Volume{
		id:       tid,
		incoming: incoming,
	}
}

func (v *Volume) Handle(out *[]blobcache.TopicMessage, msg blobcache.TopicMessage) error {
	return nil
}

// Await implements volumes.Volume.
func (v *Volume) Await(ctx context.Context, prev []byte, next *[]byte) error {
	panic("unimplemented")
}

// BeginTx implements volumes.Volume.
func (v *Volume) BeginTx(ctx context.Context, spec blobcache.TxParams) (volumes.Tx, error) {
	panic("unimplemented")
}
