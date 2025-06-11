package bcnet

import (
	"context"
	"crypto/ed25519"
	"net"
)

type FQHandle struct {
}

type Node struct {
	pc net.PacketConn
}

func New(pc net.PacketConn, privateKey ed25519.PrivateKey) *Node {
	return &Node{
		pc: pc,
	}
}

func (n *Node) Run(ctx context.Context) error {
	return nil
}
