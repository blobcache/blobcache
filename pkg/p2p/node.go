package p2p

import (
	"context"
)

type Node interface {
	ListEdges() []Edge
	AddEdge(Edge) int
	DeleteEdge(int)

	LocalID() PeerID

	AddTransport(name string, t Transport)

	Swarm() Swarm
	Run(ctx context.Context) error
}

type node struct {
	privateKey  PrivateKey
	localPeerID PeerID

	edges   []Edge
	edgesRc map[Edge]int
	peers   *peerStore

	transports map[string]Transport

	swarm *swarm
}

func New(privateKey PrivateKey) Node {
	pubKey := privateKey.Public()
	localPeerID := NewPeerID(pubKey)

	n := &node{
		localPeerID: localPeerID,
		privateKey:  privateKey,

		edges: []Edge{
			{PeerID: localPeerID},
		},
		edgesRc: make(map[Edge]int),

		transports: make(map[string]Transport),
		peers:      &peerStore{},
	}
	n.swarm = &swarm{n: n}

	return n
}

func (n *node) AddEdge(e Edge) int {
	i := len(n.edges)
	n.edges = append(n.edges, e)
	n.edgesRc[e]++
	return i
}

func (n *node) DeleteEdge(i int) {
	if i == 0 {
		panic("cannot delete loopback edge")
	}
	e := n.edges[i]
	n.edgesRc[e]--
	if n.edgesRc[e] == 0 {
		n.transports[e.Transport].DeleteEdge(i)
		delete(n.edgesRc, e)
	}
}

func (n *node) ListEdges() []Edge {
	return n.edges
}

func (n *node) AddTransport(name string, t Transport) {
	_, ok := n.transports[name]
	if ok {
		panic("transport already exists")
	}

	n.transports[name] = t
	t.Init(n.privateKey, n.peers)

	for i, e := range n.edges {
		if e.Transport == name {
			t.AddEdge(i, e)
		}
	}
}

func (n *node) LocalID() PeerID {
	return n.localPeerID
}

func (n *node) ask(ctx context.Context, msg Message) ([]byte, error) {
	panic("not implemented")
}

func (n *node) tell(ctx context.Context, msg Message) error {
	t, ok := n.transports[msg.Protocol]
	if !ok {
		panic("missing transport")
	}
	asyncT, ok := t.(AsyncTransport)
	if !ok {
		panic("transport does not support tell")
	}
	return asyncT.Tell(ctx, msg)
}

func (n *node) sign(data []byte) []byte {
	return nil
}

func (n *node) Swarm() Swarm {
	return n.swarm
}

func (n *node) findEdgeForPeer(id PeerID) uint {
	for i, e := range n.edges {
		if e.PeerID == id {
			return uint(i)
		}
	}
	return 0
}

func (n *node) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}
