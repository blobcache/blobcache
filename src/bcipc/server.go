package bcipc

import (
	"context"
	"errors"
	"io"
	"net"

	"blobcache.io/blobcache/src/bcipc/internal/streammux"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func ListenAndServe(ctx context.Context, p string, srv bcp.Handler) error {
	laddr := net.UnixAddr{Name: p, Net: "unix"}
	lis, err := net.ListenUnix("unix", &laddr)
	if err != nil {
		return err
	}
	defer lis.Close()
	return Serve(ctx, lis, srv)
}

func Serve(ctx context.Context, lis *net.UnixListener, srv bcp.Handler) error {
	ctx, cf := context.WithCancel(ctx)
	eg, ctx := errgroup.WithContext(ctx)
	defer eg.Wait()
	defer cf()

	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()

	for {
		uc, err := lis.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		eg.Go(func() error {
			if err := serveMux(ctx, uc, srv); err != nil {
				logctx.Error(ctx, "while serving:", zap.Error(err))
			}
			return nil
		})
	}
}

func serveMux(ctx context.Context, conn io.ReadWriteCloser, srv bcp.Handler) error {
	mux := streammux.New(conn)
	defer mux.Close()
	for {
		req, rw, err := mux.Accept()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		var resp bcp.Message
		if srv.ServeBCP(ctx, blobcache.Endpoint{}, req, &resp) {
			if err := rw.Send(resp, true); err != nil {
				return err
			}
		}
	}
}
