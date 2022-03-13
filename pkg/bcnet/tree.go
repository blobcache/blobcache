package bcnet

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/blobcache/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
)

type TreeService interface {
	Post(context.Context, PeerID, tries.Root, int64) (*TreeInfo, error)
	Drop(context.Context, PeerID, TreeID) (bool, error)
	List(context.Context, PeerID) ([]TreeInfo, error)
	GetNodeInfo(context.Context, PeerID) (*NodeInfo, error)
}

type TreeStatus string

const (
	StatusOK       = TreeStatus("OK")
	StatusDEGRADED = TreeStatus("DEGRADED")
	StatusINVALID  = TreeStatus("INVALID")
)

type TreeID int64

type TreeInfo struct {
	ID TreeID `json:"id"`

	Root     tries.Root `json:"root"`
	MaxCount int64      `json:"max_count"`

	Status TreeStatus `json:"status"`
	Reason string     `json:"message"`
}

type NodeInfo struct {
	TotalCount int64 `json:"quota_count"`
	FreeCount  int64 `json:"free_count"`
}

type TreeReq struct {
	Post        *TreePostReq `json:"post,omitempty"`
	Drop        *TreeID      `json:"delete,omitempty"`
	List        *struct{}    `json:"list,omitempty"`
	GetNodeInfo *struct{}    `json:"get_node_info,omitempty"`
}

type TreeResp struct {
	Error *WireError `json:"error,omitempty"`

	Post     *TreeInfo  `json:"post,omitempty"`
	Drop     *bool      `json:"drop,omitempty"`
	List     []TreeInfo `json:"list,omitempty"`
	NodeInfo *NodeInfo  `json:"node_info,omitempty"`
}

type TreePostReq struct {
	Root     tries.Root `json:"root"`
	MaxCount int64      `json:"max_nodes"`
}

type TreeServer struct {
	srv TreeService
}

func (s *TreeServer) HandleAsk(ctx context.Context, resp []byte, msg p2p.Message) int {
	var req TreeReq
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return -1
	}
	res, err := s.handleAsk(ctx, msg.Src.(PeerID), &req)
	if err != nil {
		return -1
	}
	data, err := json.Marshal(res)
	if err != nil {
		return -1
	}
	return copy(resp, data)
}

func (s *TreeServer) handleAsk(ctx context.Context, from PeerID, req *TreeReq) (*TreeResp, error) {
	var resp TreeResp
	switch {
	case req.Post != nil:
		res, err := s.srv.Post(ctx, from, req.Post.Root, req.Post.MaxCount)
		if err != nil {
			return nil, err
		}
		resp.Post = res
	case req.Drop != nil:
		res, err := s.srv.Drop(ctx, from, *req.Drop)
		if err != nil {
			return nil, err
		}
		resp.Drop = &res
	case req.List != nil:
		res, err := s.srv.List(ctx, from)
		if err != nil {
			return nil, err
		}
		resp.List = res
	case req.GetNodeInfo != nil:
		res, err := s.srv.GetNodeInfo(ctx, from)
		if err != nil {
			return nil, err
		}
		resp.NodeInfo = res
	}
	return &resp, nil
}

type TreeClient struct {
	swarm p2p.SecureAskSwarm
}

func (c *TreeClient) Post(ctx context.Context, dst PeerID, root tries.Root, maxCount int64) (*TreeInfo, error) {
	var resp TreeResp
	if err := c.askJSON(ctx, dst, &resp, TreeReq{
		Post: &TreePostReq{
			Root:     root,
			MaxCount: maxCount,
		},
	}); err != nil {
		return nil, err
	}
	if resp.Post == nil {
		return nil, errors.New("missing post response")
	}
	return resp.Post, nil
}

func (c *TreeClient) Drop(ctx context.Context, dst PeerID, root tries.Root, maxCount int64) (bool, error) {
	var resp TreeResp
	if err := c.askJSON(ctx, dst, &resp, TreeReq{
		Post: &TreePostReq{
			Root:     root,
			MaxCount: maxCount,
		},
	}); err != nil {
		return false, err
	}
	if resp.Drop == nil {
		return false, errors.New("missing drop in response")
	}
	return *resp.Drop, nil
}

func (c *TreeClient) List(ctx context.Context, dst PeerID) ([]TreeInfo, error) {
	var resp TreeResp
	if err := askJson(ctx, c.swarm, dst, &resp, TreeReq{
		List: &struct{}{},
	}); err != nil {
		return nil, err
	}
	return resp.List, nil
}

func (c *TreeClient) askJSON(ctx context.Context, dst PeerID, resp *TreeResp, req TreeReq) error {
	if err := askJson(ctx, c.swarm, dst, resp, req); err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	return nil
}
