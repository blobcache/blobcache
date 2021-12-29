package blobcache

import (
	"context"

	"github.com/blobcache/blobcache/pkg/stores"
	"github.com/brendoncarroll/go-state/cadata"
)

// MaxSize is the maximum blob size
const MaxSize = 1 << 21

// Hash is the hash function used to compute cadata.IDs
func Hash(x []byte) cadata.ID {
	return stores.Hash(x)
}

type PinSet struct {
	ID          PinSetID  `json:"id"`
	Description string    `json:"name"`
	Root        cadata.ID `json:"root"`
	Count       uint64    `json:"count"`
}

type PinSetOptions struct {
}

type PinSetHandle struct {
	ID     PinSetID `json:"id"`
	Secret [16]byte `json:"secret"`
}

type PinSetID uint64

// Service is the API exposed by either a blobcache client, or inmemory node.
type Service interface {
	// CreatePinSet creates a PinSet with the provided options and returns a handle to it.
	CreatePinSet(ctx context.Context, opts PinSetOptions) (*PinSetHandle, error)
	// DeletePinSet deletes the PinSet referenced by the handle
	DeletePinSet(ctx context.Context, pinset PinSetHandle) error
	// GetPinSet returns information about the PinSet
	GetPinSet(ctx context.Context, pinset PinSetHandle) (*PinSet, error)

	// Add adds data to the PinSet by ID.
	Add(ctx context.Context, pinset PinSetHandle, id cadata.ID) error
	// Delete removes data from the PinSet by ID.
	Delete(ctx context.Context, pinset PinSetHandle, id cadata.ID) error
	// Post adds data to a PinSet and returns the ID.
	Post(ctx context.Context, pinset PinSetHandle, data []byte) (cadata.ID, error)
	// Get retrieves data from the PinSet and returns the ID.
	Get(ctx context.Context, pinset PinSetHandle, id cadata.ID, buf []byte) (int, error)
	// Exists returns whether the PinSet contains ID
	Exists(ctx context.Context, pinset PinSetHandle, id cadata.ID) (bool, error)
	// List lists the ids in the pinSet.
	List(ctx context.Context, pinSet PinSetHandle, first []byte, ids []cadata.ID) (n int, err error)

	// MaxBlobSize returns the maximum blob size
	MaxBlobSize() int
}

type Store = cadata.Store
