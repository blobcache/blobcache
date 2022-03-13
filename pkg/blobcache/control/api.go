package control

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

// ReadOnlySet is a read-only cadata.Set
type ReadOnlySet interface {
	cadata.Lister
	cadata.Exister
}

// Source provides blobs to be maintained by the controller
type Source struct {
	Set              ReadOnlySet
	ExpectedReplicas float64
}

// Sinks are places which blobs can be stored.
type Sink struct {
	Locus    [32]byte
	MaxCount int64

	Actual  ReadOnlySet
	Desired cadata.Set
	Notify  NotifyFunc
	Flush   FlushFunc
}

// NotifyFunc is called to notify about a change to an ID
type NotifyFunc = func(cadata.ID)

// FlushFunc is called to flush changes to a sync
type FlushFunc = func(ctx context.Context, set map[cadata.ID]struct{}) error
