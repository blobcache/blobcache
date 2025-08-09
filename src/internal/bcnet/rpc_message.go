package bcnet

import "blobcache.io/blobcache/src/blobcache"

type OpenReq struct {
	Base   blobcache.Handle    `json:"base"`
	Target blobcache.OID       `json:"target"`
	Mask   blobcache.ActionSet `json:"mask"`
}

type OpenResp struct {
	Handle blobcache.Handle     `json:"handle"`
	Info   blobcache.VolumeInfo `json:"info"`
}

type CreateAndLinkReq struct {
	Parent blobcache.Handle     `json:"parent"`
	Name   string               `json:"name"`
	Spec   blobcache.VolumeSpec `json:"spec"`
}

type CreateAndLinkResp struct {
	Handle blobcache.Handle `json:"handle"`
}

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

type InspectVolumeReq struct {
	Volume blobcache.Handle `json:"volume"`
}

type InspectVolumeResp struct {
	Info blobcache.VolumeInfo `json:"info"`
}

type AwaitReq struct {
	Cond blobcache.Conditions `json:"cond"`
}

type AwaitResp struct{}

type BeginTxReq struct {
	Volume blobcache.Handle   `json:"volume"`
	Params blobcache.TxParams `json:"params"`
}

type BeginTxResp struct {
	// Tx is the handle for the transaction.
	Tx blobcache.Handle `json:"tx"`
	// VolumeInfo is the volume info for the transaction.
	VolumeInfo blobcache.VolumeInfo `json:"volume_info"`
}

type InspectTxReq struct {
	Tx blobcache.Handle `json:"tx"`
}

type InspectTxResp struct {
	Info blobcache.TxInfo `json:"info"`
}

type CommitReq struct {
	Tx   blobcache.Handle `json:"tx"`
	Root []byte           `json:"root"`
}

type CommitResp struct{}

type AbortReq struct {
	Tx blobcache.Handle `json:"tx"`
}

type AbortResp struct{}

type LoadReq struct {
	Tx blobcache.Handle `json:"tx"`
}

type LoadResp struct {
	Root []byte `json:"root"`
}

type ExistsReq struct {
	Tx   blobcache.Handle `json:"tx"`
	CIDs []blobcache.CID  `json:"cids"`
}

type ExistsResp struct {
	Exists []bool `json:"exists"`
}

type DeleteReq struct {
	Tx  blobcache.Handle `json:"tx"`
	CID blobcache.CID    `json:"cid"`
}

type DeleteResp struct{}

type GetReq struct {
	Tx   blobcache.Handle `json:"tx"`
	CID  blobcache.CID    `json:"cid"`
	Salt *blobcache.CID   `json:"salt,omitempty"`
}

type GetResp struct {
	Data []byte `json:"data"`
}

type SetSubVolumesReq struct {
	Tx      blobcache.Handle   `json:"tx"`
	Subvols []blobcache.Handle `json:"subvols"`
}

type SetSubVolumesResp struct{}

type AllowLinkReq struct {
	Tx     blobcache.Handle `json:"tx"`
	Subvol blobcache.Handle `json:"subvol"`
}

type AllowLinkResp struct{}

type CreateSubVolumeReq struct {
	Tx   blobcache.Handle     `json:"tx"`
	Spec blobcache.VolumeSpec `json:"spec"`
}

type CreateSubVolumeResp struct {
	Volume blobcache.VolumeInfo `json:"volume"`
}
