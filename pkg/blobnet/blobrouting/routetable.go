package blobrouting

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/pkg/errors"
)

type RouteTable interface {
	Put(context.Context, blobs.ID, p2p.PeerID, time.Time) error
	Lookup(context.Context, blobs.ID) ([]RTEntry, error)
	Delete(context.Context, blobs.ID, p2p.PeerID) error
	List(ctx context.Context, prefix []byte, entries []RTEntry) (int, error)
}

type RTEntry struct {
	BlobID    blobs.ID
	PeerID    p2p.PeerID
	SightedAt time.Time
}

func parseTime(x []byte) (*time.Time, error) {
	if len(x) != 8 {
		return nil, errors.Errorf("buffer is wrong size for uint64")
	}
	t := time.Unix(int64(binary.BigEndian.Uint64(x)), 0)
	return &t, nil
}
