package bcnet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

type BlobReq struct {
	Add    []cadata.ID  `json:"add,omitempty"`
	Delete []cadata.ID  `json:"delete,omitempty"`
	Exists []cadata.ID  `json:"exists,omitempty"`
	List   *ListBlobReq `json:"list,omitempty"`
}

type WireError struct {
	Code    codes.Code `json:"code"`
	Message string     `json:"msg"`
}

func (e WireError) Error() string {
	return fmt.Sprintf("%v: %s", e.Code, e.Message)
}

type BlobRes struct {
	Error *WireError `json:"error,omitempty"`

	Add    []bool       `json:"add,omitempty"`
	Delete []bool       `json:"delete,omitempty"`
	Exists []bool       `json:"exists,omitempty"`
	List   *ListBlobRes `json:"list,omitempty"`
}

type ListBlobReq struct {
	First cadata.ID `json:"first"`
	Limit int       `json:"limit"`
}

type ListBlobRes struct {
	End bool        `json:"end"`
	IDs []cadata.ID `json:"ids"`
}

type BlobMainServer struct {
	open       func(PeerID) cadata.Store
	pullClient *BlobPullClient
}

func (s *BlobMainServer) HandleAsk(ctx context.Context, resp []byte, msg p2p.Message) int {
	var req BlobReq
	var res BlobRes
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return -1
	}
	peer := msg.Src.(PeerID)
	switch {
	case req.Add != nil:
		addRes, err := s.handleAdd(ctx, peer, req.Add)
		if err != nil {
			res.Error = &WireError{
				Code:    codes.Unknown,
				Message: err.Error(),
			}
		} else {
			res.Add = addRes
		}
	case req.Delete != nil:
		delRes, err := s.handleDelete(ctx, peer, req.Delete)
		if err != nil {
			res.Error = &WireError{
				Code:    codes.Unknown,
				Message: err.Error(),
			}
		} else {
			res.Delete = delRes
		}
	case req.Exists != nil:
		existsRes, err := s.handleExists(ctx, peer, req.Exists)
		if err != nil {
			res.Error = &WireError{
				Code:    codes.Unknown,
				Message: err.Error(),
			}
		} else {
			res.Exists = existsRes
		}
	case req.List != nil:
		listRes, err := s.handleList(ctx, peer, req.List)
		if err != nil {
			res.Error = &WireError{
				Code:    codes.Unknown,
				Message: err.Error(),
			}
		} else {
			res.List = listRes
		}
	default:
		return -1
	}
	return copy(resp, marshal(res))
}

func (s *BlobMainServer) handleAdd(ctx context.Context, from PeerID, ids []cadata.ID) ([]bool, error) {
	store := s.open(from)
	affected := make([]bool, len(ids))
	eg, ctx := errgroup.WithContext(ctx)
	for i := range ids {
		i := i
		exists, err := cadata.Exists(ctx, store, ids[i])
		if err != nil {
			return nil, err
		}
		affected[i] = exists
		if exists {
			continue
		}
		eg.Go(func() error {
			buf := make([]byte, store.MaxSize())
			n, err := s.pullClient.Pull(ctx, from, ids[i], buf)
			if err != nil {
				return nil
			}
			if _, err := store.Post(ctx, buf[:n]); err != nil {
				return err
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return affected, nil
}

func (s *BlobMainServer) handleDelete(ctx context.Context, from PeerID, ids []cadata.ID) ([]bool, error) {
	store := s.open(from)
	affected := make([]bool, len(ids))
	for i := range ids {
		exists, err := cadata.Exists(ctx, store, ids[i])
		if err != nil {
			return nil, err
		}
		if exists {
			if err := store.Delete(ctx, ids[i]); err != nil {
				return nil, err
			}
		}
		affected[i] = exists
	}
	return affected, nil
}

func (s *BlobMainServer) handleExists(ctx context.Context, from PeerID, ids []cadata.ID) ([]bool, error) {
	store := s.open(from)
	exists := make([]bool, len(ids))
	for i := range ids {
		yes, err := cadata.Exists(ctx, store, ids[i])
		if err != nil {
			return nil, err
		}
		exists[i] = yes
	}
	return exists, nil
}

func (s *BlobMainServer) handleList(ctx context.Context, from PeerID, req *ListBlobReq) (*ListBlobRes, error) {
	store := s.open(from)
	ids := make([]cadata.ID, req.Limit)
	n, err := store.List(ctx, req.First, ids)
	end := false
	if errors.Is(err, cadata.ErrEndOfList) {
		end = true
	} else if err != nil {
		return nil, err
	}
	return &ListBlobRes{
		IDs: ids[:n],
		End: end,
	}, nil
}

type BlobMainClient struct {
	swarm p2p.SecureAskSwarm
}

func (c *BlobMainClient) Add(ctx context.Context, dst PeerID, ids []cadata.ID) ([]bool, error) {
	req := BlobReq{
		Add: ids,
	}
	var resp BlobRes
	if err := c.askJSON(ctx, dst, &resp, &req); err != nil {
		return nil, err
	}
	if resp.Add == nil {
		return nil, errors.New("no add in response")
	}
	return resp.Add, nil
}

func (c *BlobMainClient) Delete(ctx context.Context, dst PeerID, ids []cadata.ID) ([]bool, error) {
	req := BlobReq{
		Delete: ids,
	}
	var resp BlobRes
	if err := c.askJSON(ctx, dst, &resp, &req); err != nil {
		return nil, err
	}
	if resp.Delete == nil {
		return nil, errors.New("no delete in response")
	}
	return resp.Delete, nil
}

func (c *BlobMainClient) Exists(ctx context.Context, dst PeerID, ids []cadata.ID) ([]bool, error) {
	req := BlobReq{
		Exists: ids,
	}
	var resp BlobRes
	if err := c.askJSON(ctx, dst, &resp, &req); err != nil {
		return nil, err
	}
	if resp.Exists != nil {
		return nil, errors.New("no exists in response")
	}
	return resp.Exists, nil
}

func (c *BlobMainClient) List(ctx context.Context, dst PeerID, first cadata.ID, ids []cadata.ID) (int, error) {
	req := BlobReq{
		List: &ListBlobReq{
			First: first,
			Limit: len(ids),
		},
	}
	var resp BlobRes
	if err := c.askJSON(ctx, dst, &resp, &req); err != nil {
		return 0, err
	}
	if resp.List == nil {
		return 0, errors.New("no list in response")
	}
	res := resp.List
	var err error
	if res.End {
		err = cadata.ErrEndOfList
	}
	return copy(ids, res.IDs), err
}

func (c *BlobMainClient) askJSON(ctx context.Context, dst PeerID, resp *BlobRes, req *BlobReq) error {
	if err := askJson(ctx, c.swarm, dst, resp, req); err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	return nil
}

type blobSet struct {
	client *BlobMainClient
	peer   PeerID
}

func NewBlobSet(c *BlobMainClient, peer PeerID) cadata.Set {
	return blobSet{client: c}
}

func (s blobSet) Add(ctx context.Context, id cadata.ID) error {
	yes, err := s.client.Add(ctx, s.peer, []cadata.ID{id})
	if err != nil {
		return err
	}
	for i := range yes {
		if !yes[i] {
			return fmt.Errorf("error adding blob %v", id)
		}
	}
	return nil
}

func (s blobSet) Delete(ctx context.Context, id cadata.ID) error {
	_, err := s.client.Delete(ctx, s.peer, []cadata.ID{id})
	return err
}

func (s blobSet) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	exists, err := s.client.Exists(ctx, s.peer, []cadata.ID{id})
	if err != nil {
		return false, err
	}
	return exists[0], nil
}

func (s blobSet) List(ctx context.Context, first cadata.ID, ids []cadata.ID) (int, error) {
	return s.client.List(ctx, s.peer, first, ids)
}
