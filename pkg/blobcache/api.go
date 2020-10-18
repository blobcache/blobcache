package blobcache

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/blobcache/blobcache/pkg/blobs"
)

type API interface {
	// PinSets
	CreatePinSet(ctx context.Context, name string) (PinSetID, error)
	DeletePinSet(ctx context.Context, pinset PinSetID) error
	GetPinSet(ctx context.Context, pinset PinSetID) (*PinSet, error)
	Pin(ctx context.Context, pinset PinSetID, id blobs.ID) error
	Unpin(ctx context.Context, pinset PinSetID, id blobs.ID) error

	// Blobs
	Post(ctx context.Context, pinset PinSetID, data []byte) (blobs.ID, error)
	GetF(ctx context.Context, id blobs.ID, f func([]byte) error) error

	MaxBlobSize() int
}

type Source interface {
	blobs.Getter
	blobs.Lister
}

type PeerStore = peers.PeerStore
