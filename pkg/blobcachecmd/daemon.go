package blobcachecmd

import (
	"context"
	"net/http"

	"github.com/blobcache/blobcache/pkg/bchttp"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type DaemonParams struct {
	NodeParams blobcache.Params
	APIAddr    string
	Logger     *logrus.Logger
}

type Daemon struct {
	params DaemonParams

	node      *blobcache.Node
	apiServer *bchttp.Server
}

func NewDaemon(params DaemonParams) *Daemon {
	node := blobcache.NewNode(params.NodeParams)
	return &Daemon{
		params:    params,
		node:      node,
		apiServer: bchttp.NewServer(node, params.Logger),
	}
}

func (d *Daemon) Run(ctx context.Context) error {
	group := errgroup.Group{}
	group.Go(func() error {
		return d.runAPI(ctx)
	})
	return group.Wait()
}

func (d *Daemon) runAPI(ctx context.Context) error {
	hs := &http.Server{
		Addr:    d.params.APIAddr,
		Handler: d.apiServer,
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return hs.ListenAndServe()
	})
	eg.Go(func() error {
		<-ctx.Done()
		return ctx.Err()
	})
	return eg.Wait()
}

func (d *Daemon) Close() error {
	return nil
}
