// Package bchttp implements a Blobcache service over HTTP.
package bchttp

import "blobcache.io/blobcache/src/blobcache"

type CreateVolumeReq struct {
	Spec blobcache.VolumeSpec `json:"spec"`
}

type CreateVolumeResp struct {
	Handle blobcache.Handle `json:"handle"`
}

type AwaitReq struct {
	Conditions blobcache.Conditions `json:"conditions"`
}

type AwaitResp struct{}

type BeginTxReq struct {
	Volume blobcache.Handle `json:"volume"`
	Mutate bool             `json:"mutate"`
}

type BeginTxResp struct {
	Tx blobcache.Handle `json:"handle"`
}

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

type KeepAliveReq struct {
	Targets []blobcache.Handle `json:"target"`
}

type KeepAliveResp struct{}

type GetReq struct {
	CID  blobcache.CID  `json:"cid"`
	Salt *blobcache.CID `json:"salt,omitempty"`
}

type AnchorReq struct {
	Target blobcache.Handle `json:"target"`
}

type AnchorResp struct{}

type DropReq struct {
	Target blobcache.Handle `json:"target"`
}

type DropResp struct{}

type StartSyncReq struct {
	Src blobcache.Handle `json:"src"`
	Dst blobcache.Handle `json:"dst"`
}

type StartSyncResp struct{}

type CreateRuleReq struct {
	Spec blobcache.RuleSpec `json:"spec"`
}

type CreateRuleResp struct {
	Handle blobcache.Handle `json:"handle"`
}
