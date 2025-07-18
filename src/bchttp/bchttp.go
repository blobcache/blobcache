// Package bchttp implements a Blobcache service over HTTP.
package bchttp

import (
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcnet"
)

// Handle messages.
type (
	InspectHandleReq  = bcnet.InspectHandleReq
	InspectHandleResp = bcnet.InspectHandleResp
	DropReq           = bcnet.DropReq
	DropResp          = bcnet.DropResp
	KeepAliveReq      = bcnet.KeepAliveReq
	KeepAliveResp     = bcnet.KeepAliveResp
	OpenReq           = bcnet.OpenReq
	OpenResp          = bcnet.OpenResp
)

// Namespace messages.
type (
	OpenAtReq       = bcnet.OpenAtReq
	OpenAtResp      = bcnet.OpenAtResp
	GetEntryReq     = bcnet.GetEntryReq
	GetEntryResp    = bcnet.GetEntryResp
	PutEntryReq     = bcnet.PutEntryReq
	PutEntryResp    = bcnet.PutEntryResp
	DeleteEntryReq  = bcnet.DeleteEntryReq
	DeleteEntryResp = bcnet.DeleteEntryResp
	ListNamesReq    = bcnet.ListNamesReq
	ListNamesResp   = bcnet.ListNamesResp
)

// Volume messages.

type (
	AwaitReq  = bcnet.AwaitReq
	AwaitResp = bcnet.AwaitResp
)

type CreateVolumeReq struct {
	Spec blobcache.VolumeSpec `json:"spec"`
}

type CreateVolumeResp struct {
	Handle blobcache.Handle `json:"handle"`
}

type BeginTxReq struct {
	Volume blobcache.Handle   `json:"volume"`
	Params blobcache.TxParams `json:"params"`
}

type BeginTxResp struct {
	Tx blobcache.Handle `json:"handle"`
}

// Tx messages.

type CommitReq struct {
	Root []byte `json:"root"`
}

type CommitResp struct{}

type AbortReq struct{}

type AbortResp struct{}

type LoadReq struct{}

type LoadResp struct {
	Root []byte `json:"root"`
}

type ExistsReq struct {
	CID blobcache.CID `json:"cid"`
}

type ExistsResp struct {
	Exists bool `json:"exists"`
}

type DeleteReq struct {
	CID blobcache.CID `json:"cid"`
}

type DeleteResp struct{}

type GetReq struct {
	CID  blobcache.CID  `json:"cid"`
	Salt *blobcache.CID `json:"salt,omitempty"`
}

// Miscellaneous messages.

type EndpointReq struct{}

type EndpointResp struct {
	Endpoint blobcache.Endpoint `json:"endpoint"`
}
