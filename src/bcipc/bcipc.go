// Package bcipc implements the Blobcache Protocol over UNIX sockets
package bcipc

import (
	"context"

	"blobcache.io/blobcache/src/bcipc/internal/streammux"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/pools"
)

const MaxMessageLen = 1<<24 + bcp.HeaderLen

var _ bcp.Asker = &clientTransport{}

type clientTransport struct {
	pool *pools.OpenClose[*streammux.Mux]
}

func (ct *clientTransport) Ask(ctx context.Context, remEp blobcache.Endpoint, req bcp.Message, resp *bcp.Message) error {
	mux, err := ct.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = ct.pool.Give(ctx, mux)
	}()
	stream, err := mux.Open(req)
	if err != nil {
		return err
	}
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	*resp = msg
	return nil
}
