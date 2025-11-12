// Package bchttp implements a Blobcache service over HTTP.
package bchttp

import (
	"blobcache.io/blobcache/src/blobcache"
)

// Handle messages.
type InspectHandleReq struct {
	Handle blobcache.Handle `json:"handle"`
}

type InspectHandleResp struct {
	Info blobcache.HandleInfo `json:"info"`
}

type DropReq struct {
	Handle blobcache.Handle `json:"handle"`
}

type DropResp struct{}

type KeepAliveReq struct {
	Handles []blobcache.Handle `json:"handles"`
}

type KeepAliveResp struct{}

type ShareReq struct {
	Handle blobcache.Handle    `json:"handle"`
	Peer   blobcache.PeerID    `json:"peer"`
	Mask   blobcache.ActionSet `json:"mask"`
}

type ShareResp struct {
	Handle blobcache.Handle `json:"handle"`
}

type OpenFromReq struct {
	Base   blobcache.Handle    `json:"base"`
	Target blobcache.OID       `json:"target"`
	Mask   blobcache.ActionSet `json:"mask"`
}

type OpenFromResp struct {
	Handle blobcache.Handle     `json:"handle"`
	Info   blobcache.VolumeInfo `json:"info"`
}

type OpenFiatReq struct {
	Target blobcache.OID       `json:"target"`
	Mask   blobcache.ActionSet `json:"mask"`
}

type OpenFiatResp struct {
	Handle blobcache.Handle     `json:"handle"`
	Info   blobcache.VolumeInfo `json:"info"`
}

type InspectVolumeReq struct {
	Volume blobcache.Handle `json:"volume"`
}

type CreateVolumeReq struct {
	Host *blobcache.Endpoint  `json:"host,omitempty"`
	Spec blobcache.VolumeSpec `json:"spec"`
}

type CreateVolumeResp struct {
	Handle blobcache.Handle `json:"handle"`
}

type CloneVolumeReq struct {
	// Volume is the handle to the volume to clone.
	Volume blobcache.Handle `json:"volume"`
}

type CloneVolumeResp struct {
	// Clone is the handle to the cloned volume.
	Clone blobcache.Handle `json:"clone"`
}

type BeginTxReq struct {
	Volume blobcache.Handle   `json:"volume"`
	Params blobcache.TxParams `json:"params"`
}

type BeginTxResp struct {
	Tx blobcache.Handle `json:"handle"`
}

// Tx messages.

type CreateSubVolumeReq struct {
	Spec blobcache.VolumeSpec `json:"spec"`
}

type CreateSubVolumeResp struct {
	Volume blobcache.VolumeInfo `json:"volume"`
}

type InspectTxReq struct {
	Tx blobcache.Handle `json:"tx"`
}

type InspectTxResp struct {
	Info blobcache.TxInfo `json:"info"`
}

type CommitReq struct{}

type CommitResp struct{}

type AbortReq struct{}

type AbortResp struct{}

type SaveReq struct {
	Root []byte `json:"root"`
}

type SaveResp struct{}

type LoadReq struct{}

type LoadResp struct {
	Root []byte `json:"root"`
}

type ExistsReq struct {
	CIDs []blobcache.CID `json:"cids"`
}

type ExistsResp struct {
	Exists []bool `json:"exists"`
}

type DeleteReq struct {
	CIDs []blobcache.CID `json:"cids"`
}

type DeleteResp struct{}

type GetReq struct {
	CID  blobcache.CID  `json:"cid"`
	Salt *blobcache.CID `json:"salt,omitempty"`
}

type AddFromReq struct {
	CIDs []blobcache.CID    `json:"cids"`
	Srcs []blobcache.Handle `json:"srcs"`
}

type AddFromResp struct {
	Added []bool `json:"added"`
}

type VisitReq struct {
	CIDs []blobcache.CID `json:"cids"`
}

type VisitResp struct{}

type IsVisitedReq struct {
	CIDs []blobcache.CID `json:"cids"`
}

type IsVisitedResp struct {
	Visited []bool `json:"visited"`
}

// Miscellaneous messages.

type EndpointReq struct{}

type EndpointResp struct {
	Endpoint blobcache.Endpoint `json:"endpoint"`
}

type LinkReq struct {
	Target blobcache.Handle    `json:"target"`
	Mask   blobcache.ActionSet `json:"mask"`
}

type LinkResp struct{}

type UnlinkReq struct {
	Targets []blobcache.OID `json:"targets"`
}

type UnlinkResp struct{}

type VisitLinksReq struct {
	Targets []blobcache.OID `json:"targets"`
}

type VisitLinksResp struct{}
