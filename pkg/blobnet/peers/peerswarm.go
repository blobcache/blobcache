package peers

import (
	"context"
	"errors"
	"io"

	"github.com/brendoncarroll/go-p2p"
	log "github.com/sirupsen/logrus"
)

var (
	ErrPeerUnreachable = errors.New("peer unreachable")
)

type PeerSwarm struct {
	s         p2p.SecureAskSwarm
	peerStore PeerStore
	localID   p2p.PeerID
}

func NewPeerSwarm(s p2p.SecureAskSwarm, peerStore PeerStore) *PeerSwarm {
	pubKey := s.PublicKey()
	return &PeerSwarm{
		s:         s,
		peerStore: peerStore,
		localID:   p2p.NewPeerID(pubKey),
	}
}

func (ps *PeerSwarm) AskPeer(ctx context.Context, dst p2p.PeerID, data []byte) ([]byte, error) {
	for _, addr := range ps.GetAddrs(dst) {
		res, err := ps.s.Ask(ctx, addr, data)
		if err != nil {
			log.Error(err)
			continue
		} else {
			return res, nil
		}
	}
	return nil, errors.New("peer unreachable")
}

func (ps *PeerSwarm) TellPeer(ctx context.Context, dst p2p.PeerID, data []byte) error {
	for _, addr := range ps.GetAddrs(dst) {
		err := ps.s.Tell(ctx, addr, data)
		if err != nil {
			log.Error(err)
			continue
		} else {
			return nil
		}
	}
	return errors.New("peer unreachable")
}

func (ps *PeerSwarm) OnAsk(fn p2p.AskHandler) {
	ps.s.OnAsk(func(ctx context.Context, m *p2p.Message, w io.Writer) {
		m.Src = p2p.NewPeerID(ps.s.LookupPublicKey(m.Src))
		m.Dst = ps.localID
		fn(ctx, m, w)
	})
}

func (ps *PeerSwarm) OnTell(fn p2p.TellHandler) {
	ps.s.OnTell(func(m *p2p.Message) {
		m.Src = p2p.NewPeerID(ps.s.LookupPublicKey(m.Src))
		m.Dst = ps.localID
		fn(m)
	})
}

func (ps *PeerSwarm) Close() error {
	return ps.s.Close()
}

func (ps *PeerSwarm) MTU(ctx context.Context, addr p2p.Addr) int {
	return ps.s.MTU(ctx, addr)
}

func (ps *PeerSwarm) GetAddrs(id p2p.PeerID) []p2p.Addr {
	return ps.peerStore.GetAddrs(id)
}

func (ps *PeerSwarm) ListPeers() []p2p.PeerID {
	return ps.peerStore.ListPeers()
}

func (ps *PeerSwarm) LocalAddrs() []p2p.Addr {
	return []p2p.Addr{&ps.localID}
}

func (ps *PeerSwarm) LocalID() p2p.PeerID {
	return ps.localID
}
