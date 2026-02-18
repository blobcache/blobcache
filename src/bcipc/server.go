package bcipc

import (
	"context"
	"net"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func Listen(p string) (*net.UnixListener, error) {
	laddr := net.UnixAddr{Name: p, Net: "unix"}
	return net.ListenUnix("unix", &laddr)
}

func ListenAndServe(ctx context.Context, p string, srv bcp.Handler) error {
	lis, err := Listen(p)
	if err != nil {
		return err
	}
	defer lis.Close()
	return Serve(ctx, lis, srv)
}

func Serve(ctx context.Context, lis *net.UnixListener, srv bcp.Handler) error {
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	eg, ctx := errgroup.WithContext(ctx)
	defer eg.Wait()

	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()

	for {
		uc, err := lis.AcceptUnix()
		if err != nil {
			if ctx.Err() == err {
				return nil
			}
			return err
		}
		eg.Go(func() error {
			defer uc.Close()
			if err := bcp.ServeStream(ctx, blobcache.Endpoint{}, uc, srv); err != nil {
				logctx.Error(ctx, "while serving:", zap.Error(err))
			}
			return nil
		})
	}
}

type Server = bcp.Server

func NewServer(svc blobcache.Service) *Server {
	return &bcp.Server{
		Access: func(ep blobcache.PeerID) blobcache.Service {
			return svc
		},
	}
}
