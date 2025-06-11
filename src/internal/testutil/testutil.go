package testutil

import (
	"context"
	"net"
	"testing"
)

func Context(t testing.TB) context.Context {
	return context.TODO()
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
