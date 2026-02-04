// Package bcipc implements the Blobcache Protocol over UNIX sockets
package bcipc

import (
	"context"
	"net"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"blobcache.io/blobcache/src/internal/pools"
)

const MaxMessageLen = 1<<24 + bcp.HeaderLen

var _ bcp.Asker = &clientTransport{}

type clientTransport struct {
	pool   *pools.OpenClose[*net.UnixConn]
	bufLen int
}

func (ct *clientTransport) Ask(ctx context.Context, remEp blobcache.Endpoint, req bcp.Message, resp *bcp.Message) error {
	conn, err := ct.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = ct.pool.Give(ctx, conn)
	}()
	if _, err := req.WriteTo(conn); err != nil {
		return err
	}
	if _, err := resp.ReadFrom(conn); err != nil {
		return err
	}
	return nil
}
