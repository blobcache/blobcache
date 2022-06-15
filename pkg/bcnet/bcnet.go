package bcnet

import (
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"lukechampine.com/blake3"
)

const MaxMessageSize = 1 << 22

const (
	channelBlobPull = "blobcache/blob-pull-v0"
	channelBlobMain = "blobcache/blob-v0"
	channelTrees    = "blobcache/trees-v0"
)

type Params struct {
	Swarm       p2p.SecureAskSwarm[PeerID]
	OpenStore   func(PeerID) cadata.Store
	TreeService TreeService
	Logger      *logrus.Logger
}

type Service struct {
	swarm p2p.SecureAskSwarm[PeerID]
	mux   p2pmux.SecureAskMux[PeerID, string]

	blobPullSwarm  p2p.SecureAskSwarm[PeerID]
	blobPullClient *BlobPullClient
	blobPullServer *BlobPullServer

	blobMainSwarm  p2p.SecureAskSwarm[PeerID]
	blobMainClient *BlobMainClient
	blobMainServer *BlobMainServer

	treeSwarm  p2p.SecureAskSwarm[PeerID]
	treeClient *TreeClient
	treeServer *TreeServer

	cf   context.CancelFunc
	done chan struct{}
}

func New(params Params) *Service {
	// blob pull
	mux := p2pmux.NewStringSecureAskMux(params.Swarm)
	ctx, cf := context.WithCancel(context.Background())
	s := &Service{
		swarm: params.Swarm,
		mux:   mux,
		cf:    cf,
		done:  make(chan struct{}),
	}
	s.blobPullSwarm = mux.Open(channelBlobPull)
	s.blobPullClient = &BlobPullClient{swarm: s.blobPullSwarm}
	s.blobPullServer = &BlobPullServer{open: params.OpenStore}

	s.blobMainSwarm = mux.Open(channelBlobMain)
	s.blobMainClient = &BlobMainClient{swarm: s.blobMainSwarm}
	s.blobMainServer = &BlobMainServer{open: params.OpenStore, pullClient: s.blobPullClient}

	s.treeSwarm = mux.Open(channelTrees)
	s.treeClient = &TreeClient{swarm: s.treeSwarm}
	s.treeServer = &TreeServer{srv: params.TreeService}

	go s.serve(ctx)
	return s
}

func (s Service) serve(ctx context.Context) {
	defer close(s.done)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return Serve(ctx, s.blobPullSwarm, s.blobPullServer)
	})
	eg.Go(func() error {
		return Serve(ctx, s.blobMainSwarm, s.blobMainServer)
	})
	eg.Go(func() error {
		return Serve(ctx, s.treeSwarm, s.treeServer)
	})
	eg.Wait()
}

func (s Service) Close() error {
	s.cf()
	<-s.done
	return nil
}

func (s Service) BlobPull() *BlobPullClient {
	return s.blobPullClient
}

func (s Service) BlobMain() *BlobMainClient {
	return s.blobMainClient
}

func (s Service) Trees() *TreeClient {
	return s.treeClient
}

type AskHandler interface {
	HandleAsk(ctx context.Context, res []byte, req p2p.Message[PeerID]) int
}

func Serve(ctx context.Context, asker p2p.Asker[PeerID], h AskHandler) error {
	for {
		if err := asker.ServeAsk(ctx, h.HandleAsk); err != nil {
			return err
		}
	}
}

func Hash(x []byte) cadata.ID {
	return blake3.Sum256(x)
}

type PeerID = inet256.ID

func askJson(ctx context.Context, s p2p.Asker[PeerID], dst PeerID, resp, req interface{}) error {
	reqData := marshal(req)
	respData := make([]byte, MaxMessageSize)
	n, err := s.Ask(ctx, respData, dst, p2p.IOVec{reqData})
	if err != nil {
		return err
	}
	return json.Unmarshal(respData[:n], resp)
}

func marshal(x interface{}) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}
