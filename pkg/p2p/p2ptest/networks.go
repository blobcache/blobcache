package p2ptest

import "github.com/brendoncarroll/blobcache/pkg/p2p"

type Network = []p2p.Node

const tname = "test"

func Connect(r *Realm, n1, n2 p2p.Node) {
	p1 := n1.ListEdges()[0].PeerID
	p2 := n1.ListEdges()[0].PeerID

	t1 := r.node2Transport[n1]
	t2 := r.node2Transport[n2]

	n1.AddEdge(p2p.Edge{
		Transport:  tname,
		PeerID:     p2,
		RemoteAddr: t2.Addr(),
	})
	n2.AddEdge(p2p.Edge{
		Transport:  tname,
		PeerID:     p1,
		RemoteAddr: t1.Addr(),
	})
}

func Chain(r *Realm, size int) Network {
	nodes := make([]p2p.Node, size)
	for i := 0; i < size; i++ {
		nodes[i] = makeNode(r)
	}

	for i := 0; i < size-1; i++ {
		Connect(r, nodes[i], nodes[i+1])
	}
	return nodes
}

func Ring(r *Realm, size int) Network {
	nodes := Chain(r, size)
	Connect(r, nodes[len(nodes)-1], nodes[0])
	return nodes
}

func Cluster(r *Realm, size int) Network {
	nodes := make([]p2p.Node, size)
	for i := 0; i < size; i++ {
		nodes[i] = makeNode(r)
	}

	for i := 0; i < size; i++ {
		for j := i; j < size; j++ {
			Connect(r, nodes[i], nodes[j])
		}
	}
	return nodes
}

func HubAndSpoke(r *Realm, size int) Network {
	nodes := make([]p2p.Node, size)
	for i := 0; i < size; i++ {
		nodes[i] = makeNode(r)
	}

	hub := nodes[0]
	for i := 1; i < size; i++ {
		Connect(r, hub, nodes[i])
	}
	return nodes
}

func makeNode(r *Realm) p2p.Node {
	n := p2p.New(p2p.GeneratePrivateKey())
	n.AddTransport(tname, r.New(n))
	return n
}
