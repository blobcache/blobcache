package bcnet

import "blobcache.io/blobcache/src/blobcache"

type OpenReq struct {
	Target blobcache.OID `json:"target"`
}

type OpenResp struct {
	Handle blobcache.Handle `json:"handle"`
}

type AnchorReq struct {
	Handle blobcache.Handle `json:"handle"`
}

type AnchorResp struct{}

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
	Volume   blobcache.Handle   `json:"volume"`
	TxParams blobcache.TxParams `json:"tx_params"`
}

type BeginTxResp struct {
	Handle blobcache.Handle `json:"handle"`
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
