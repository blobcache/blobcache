package p2pnodes

import "github.com/brendoncarroll/blobcache/pkg/p2p"

func NewPrivate(privateKey p2p.PrivateKey, edges []p2p.Edge) p2p.Node {
	n := p2p.New(privateKey)
	AddAllTransports(n)
	for _, e := range edges {
		n.AddEdge(e)
	}
	return n
}
