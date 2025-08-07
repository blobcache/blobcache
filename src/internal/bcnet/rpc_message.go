package bcnet

import "blobcache.io/blobcache/src/blobcache"

type OpenReq struct {
	OID blobcache.OID `json:"target"`
}

type OpenResp struct {
	Handle blobcache.Handle     `json:"handle"`
	Info   blobcache.VolumeInfo `json:"info"`
}

type OpenAtReq struct {
	Namespace blobcache.Handle `json:"namespace"`
	Name      string           `json:"name"`
}

type OpenAtResp struct {
	Handle blobcache.Handle `json:"handle"`
}

type GetEntryReq struct {
	Namespace blobcache.Handle `json:"namespace"`
	Name      string           `json:"name"`
}

type GetEntryResp struct {
	Entry blobcache.Entry `json:"entry"`
}

type PutEntryReq struct {
	Namespace blobcache.Handle `json:"namespace"`
	Name      string           `json:"name"`
	Target    blobcache.Handle
}

type PutEntryResp struct{}

type DeleteEntryReq struct {
	Namespace blobcache.Handle `json:"namespace"`
	Name      string           `json:"name"`
}

type DeleteEntryResp struct{}

type ListNamesReq struct {
	Namespace blobcache.Handle `json:"namespace"`
}

type ListNamesResp struct {
	Names []string `json:"names"`
}

type CreateVolumeAtReq struct {
	Namespace blobcache.Handle     `json:"namespace"`
	Name      string               `json:"name"`
	Spec      blobcache.VolumeSpec `json:"spec"`
}

type CreateVolumeAtResp struct {
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
	Info *blobcache.VolumeInfo `json:"info"`
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
