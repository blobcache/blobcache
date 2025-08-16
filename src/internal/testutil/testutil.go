package testutil

import (
	"context"
	"net"
	"testing"

	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

func Context(t testing.TB) context.Context {
	ctx := context.Background()
	ctx, cf := context.WithCancel(ctx)
	t.Cleanup(cf)
	lg, _ := zap.NewDevelopment()
	ctx = logctx.NewContext(ctx, lg)
	return ctx
}

func Listen(t testing.TB) net.Listener {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		lis.Close()
	})
	return lis
}

func PacketConn(t testing.TB) net.PacketConn {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
	return conn
}
