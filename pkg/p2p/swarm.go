package p2p

import (
	"context"
)

type Message struct {
	DstID, SrcID PeerID
	Protocol     string
	Edge         int
	Payload      []byte
}

type TellHandler func(msg Message)
type AskHandler func(msg Message) []byte

type TellFunc func(ctx context.Context, msg Message) error
type AskFunc func(ctx context.Context, msg Message) ([]byte, error)

type Swarm interface {
	LocalID() PeerID
	Sign(data []byte) []byte

	// Peer
	Peer(PeerID) Peer
	Peers() []PeerID

	// Handlers
	OnAsk(AskHandler)
	OnTell(TellHandler)
}

type swarm struct {
	n      *node
	onAsk  AskHandler
	onTell TellHandler
}

func (s *swarm) LocalID() PeerID {
	return s.n.localPeerID
}

func (s *swarm) Sign(data []byte) []byte {
	return s.n.sign(data)
}

func (s *swarm) Peers() []PeerID {
	return s.n.peers.List()
}

func (s *swarm) Peer(id PeerID) Peer {
	return &peer{id: id, s: s, n: s.n}
}

func (s *swarm) OnAsk(fn AskHandler) {
	s.onAsk = fn
}

func (s *swarm) OnTell(fn TellHandler) {
	s.onTell = fn
}

type peer struct {
	id PeerID
	s  *swarm
	n  *node
}

func (p *peer) Ask(ctx context.Context, data []byte) ([]byte, error) {
	msg := Message{
		DstID:   p.id,
		Payload: data,
	}
	return p.n.ask(ctx, msg)
}

func (p *peer) Tell(ctx context.Context, data []byte) error {
	return nil
}

func (p *peer) ID() PeerID {
	return p.id
}

func (p *peer) PublicKey() PublicKey {
	return p.PublicKey()
}
