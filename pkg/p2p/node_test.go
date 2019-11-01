package p2p

import (
	"context"
	"testing"
)

func TestNewNode(t *testing.T) {
	privKey := GeneratePrivateKey()
	n := New(privKey)
	ctx, cf := context.WithCancel(context.TODO())
	go func() {
		n.Run(ctx)
	}()
	cf()
}
