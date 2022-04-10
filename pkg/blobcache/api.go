package blobcache

import (
	"context"
	"errors"
	"strconv"

	"github.com/blobcache/blobcache/pkg/dirserv"
	"github.com/blobcache/blobcache/pkg/stores"
	"github.com/brendoncarroll/go-state/cadata"
)

// MaxSize is the maximum blob size
const MaxSize = stores.MaxSize

// Hash is the hash function used to compute cadata.IDs
func Hash(x []byte) cadata.ID {
	return stores.Hash(x)
}

const (
	StatusOK = "OK"
)

type PinSet struct {
	Status string `json:"status"`
	Count  uint64 `json:"count"`
}

type PinSetOptions struct {
}

type Handle = dirserv.Handle

type Entry = dirserv.Entry

type PinSetID uint64

func (id PinSetID) HexString() string {
	return strconv.FormatUint(uint64(id), 16)
}

var (
	ErrPinSetNotFound = errors.New("pinset not found")
	ErrDataNotFound   = cadata.ErrNotFound
)

// Service is the API exposed by either a blobcache client, or inmemory node.
type Service interface {
	// CreateDir creates a directory below handle
	CreateDir(ctx context.Context, h Handle, name string) (*Handle, error)
	// Open returns a handle to an object
	Open(ctx context.Context, h Handle, p []string) (*Handle, error)
	// ListEntries lists the entries in the directory which h points to.
	ListEntries(ctx context.Context, h Handle) ([]Entry, error)
	// DeleteEntry ensures that there is no entry at name
	DeleteEntry(ctx context.Context, h Handle, name string) error

	// CreatePinSet creates a PinSet with the provided options and returns a handle to it.
	CreatePinSet(ctx context.Context, h Handle, name string, opts PinSetOptions) (*Handle, error)
	// GetPinSet returns information about the PinSet
	GetPinSet(ctx context.Context, h Handle) (*PinSet, error)

	// Add adds data to the PinSet by ID.
	Add(ctx context.Context, pinset Handle, id cadata.ID) error
	// Delete removes data from the PinSet by ID.
	Delete(ctx context.Context, pinset Handle, id cadata.ID) error
	// Post adds data to a PinSet and returns the ID.
	Post(ctx context.Context, pinset Handle, data []byte) (cadata.ID, error)
	// Get retrieves data from the PinSet and returns the ID.
	Get(ctx context.Context, pinset Handle, id cadata.ID, buf []byte) (int, error)
	// Exists returns whether the PinSet contains ID
	Exists(ctx context.Context, pinset Handle, id cadata.ID) (bool, error)
	// List lists the ids in the pinSet.
	List(ctx context.Context, pinSet Handle, id cadata.ID, ids []cadata.ID) (n int, err error)
	// WaitOK waits until the pinSet is status is OK.
	// This means that all the blobs in the pinSet are correctly replicated according to the pinset's config.
	WaitOK(ctx context.Context, pinSet Handle) error

	// MaxSize returns the maximum blob size
	MaxSize() int
}

type Store = cadata.Store

type Source = interface {
	cadata.Exister
	cadata.Getter
}
