package swarms

import (
	"context"
	"strings"
	"sync"

	"github.com/brendoncarroll/blobcache/pkg/p2p"
)

type Muxer struct {
	mu       sync.RWMutex
	parent   p2p.Swarm
	children map[string]*MuxSwarm
}

func Mux(s p2p.Swarm) *Muxer {
	m := &Muxer{parent: s}
	s.OnAsk(m.handleAsk)
	s.OnTell(m.handleTell)
	return m
}

func (m *Muxer) Fork(name string) *MuxSwarm {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.children[name]; exists {
		return nil
	}
	swarm := &MuxSwarm{
		muxer: m,
	}
	m.children[name] = swarm
	return swarm
}

func (m *Muxer) getMuxSwarm(protocol string) (*MuxSwarm, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	parts := strings.SplitN(protocol, "/", 2)
	next := parts[0]
	leftover := ""
	if len(parts) > 1 {
		leftover = parts[1]
	}
	return m.children[next], leftover
}

func (m *Muxer) handleAsk(msg p2p.Message) []byte {
	s, protocol := m.getMuxSwarm(msg.Protocol)
	msg.Protocol = protocol
	return s.handleAsk(msg)
}

func (m *Muxer) handleTell(msg p2p.Message) {
	s, protocol := m.getMuxSwarm(msg.Protocol)
	msg.Protocol = protocol
	s.handleTell(msg)
}

type MuxSwarm struct {
	muxer      *Muxer
	handleAsk  p2p.AskHandler
	handleTell p2p.TellHandler
}

func (ms *MuxSwarm) LocalID() p2p.PeerID {
	return ms.muxer.parent.LocalID()
}

func (ms *MuxSwarm) Peers() []p2p.PeerID {
	return ms.muxer.parent.Peers()
}

func (ms *MuxSwarm) Peer(id p2p.PeerID) p2p.Peer {
	return &MuxPeer{
		parent: ms.muxer.parent.Peer(id),
	}
}

func (ms *MuxSwarm) OnAsk(fn p2p.AskHandler) {
	ms.handleAsk = fn
}

func (ms *MuxSwarm) OnTell(fn p2p.TellHandler) {
	ms.handleTell = fn
}

type MuxPeer struct {
	parent p2p.Peer
}

func (mp MuxPeer) ID() p2p.PeerID {
	return mp.parent.ID()
}

func (mp MuxPeer) PublicKey() p2p.PublicKey {
	return mp.parent.PublicKey()
}

func (mp MuxPeer) Ask(ctx context.Context, reqBytes []byte) ([]byte, error) {
	return nil, nil
}

func (mp MuxPeer) Tell(ctx context.Context, reqBytes []byte) error {
	return nil
}
