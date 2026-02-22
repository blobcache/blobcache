package bcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
)

type Handler interface {
	ServeBCP(ctx context.Context, from blobcache.Endpoint, req Message, resp *Message) bool
}

// ServeStream serves BCP over a bidi-stream
func ServeStream(ctx context.Context, ep blobcache.Endpoint, conn io.ReadWriteCloser, srv Handler) error {
	var req, resp Message
	for {
		if _, err := req.ReadFrom(conn); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if srv.ServeBCP(ctx, ep, req, &resp) {
			if _, err := resp.WriteTo(conn); err != nil {
				return err
			}
		} else {
			return conn.Close()
		}
	}
}

func Serve(ctx context.Context, lis net.Listener, srv Handler) error {
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	wg := sync.WaitGroup{}
	for {
		conn, err := lis.Accept()
		if err != nil {
			cf()
			wg.Wait()
			return err
		}
		wg.Go(func() {
			defer conn.Close()
			defer cf()
			ServeStream(ctx, blobcache.Endpoint{}, conn, srv)
		})
		wg.Go(func() {
			<-ctx.Done()
			conn.Close()
		})
	}
}
