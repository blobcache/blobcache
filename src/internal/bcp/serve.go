package bcp

import (
	"context"
	"errors"
	"io"

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
