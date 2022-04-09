package blobcachecmd

import (
	"context"
	"net"

	"github.com/blobcache/blobcache/pkg/bcgrpc"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type DaemonParams struct {
	NodeParams blobcache.Params
	APIAddr    string
	Logger     *logrus.Logger
}

type Daemon struct {
	params DaemonParams

	node *blobcache.Node
	log  *logrus.Logger
}

func NewDaemon(params DaemonParams) *Daemon {
	node := blobcache.NewNode(params.NodeParams)
	return &Daemon{
		params: params,
		node:   node,
		log:    params.Logger,
	}
}

func (d *Daemon) Run(ctx context.Context) error {
	d.log.Infof("ROOT HANDLE: %s", d.node.Root().String())
	group := errgroup.Group{}
	group.Go(func() error {
		return d.runAPI(ctx)
	})
	return group.Wait()
}

func (d *Daemon) runAPI(ctx context.Context) error {
	l, err := net.Listen("tcp", d.params.APIAddr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	bcgrpc.RegisterBlobcacheServer(gs, bcgrpc.NewServer(d.node))
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		d.log.Infof("serving API on %v", l.Addr())
		return gs.Serve(l)
	})
	eg.Go(func() error {
		<-ctx.Done()
		gs.Stop()
		return nil
	})
	return eg.Wait()
}
