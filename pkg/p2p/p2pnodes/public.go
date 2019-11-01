package p2pnodes

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/p2p"
	"github.com/brendoncarroll/blobcache/pkg/p2p/transports/sshtransport"
)

var _ p2p.Node = &PublicNode{}

type PublicNode struct {
	p2p.Node

	sysSwarm p2p.Swarm
	appSwarm p2p.Swarm
}

func NewPublic(privateKey p2p.PrivateKey, bootstrapEdges []p2p.Edge) p2p.Node {
	n := p2p.New(privateKey)
	AddAllTransports(n)
	return &PublicNode{Node: n}
}

func (n *PublicNode) Run(ctx context.Context) error {
	return nil
}

func AddAllTransports(n p2p.Node) {
	ts := map[string]p2p.Transport{
		"ssh": sshtransport.New(),
	}
	for name, t := range ts {
		n.AddTransport(name, t)
	}
}
