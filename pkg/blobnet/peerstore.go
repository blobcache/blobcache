package blobnet

import (
	"errors"

	"github.com/brendoncarroll/go-p2p"
)

type PeerStore interface {
	TrustFor(id p2p.PeerID) (int64, error)
	ListPeers() []p2p.PeerID
	GetAddrs(id p2p.PeerID) []p2p.Addr
}

type PeerSpec struct {
	ID    p2p.PeerID
	Trust int64
	Addrs []string
}

type PeerList struct {
	Peers []PeerSpec
	Swarm p2p.Swarm
}

func (pl PeerList) TrustFor(id p2p.PeerID) (int64, error) {
	i := pl.find(id)
	if i < 0 {
		return 0, errors.New("peer not found")
	}
	return pl.Peers[i].Trust, nil
}

func (pl PeerList) ListPeers() []p2p.PeerID {
	ids := make([]p2p.PeerID, len(pl.Peers))
	for i := range pl.Peers {
		ids[i] = pl.Peers[i].ID
	}
	return ids
}

func (pl PeerList) GetAddrs(id p2p.PeerID) []p2p.Addr {
	i := pl.find(id)
	if i < 0 {
		return nil
	}
	addrs := []p2p.Addr{}
	addrStrs := pl.Peers[i].Addrs
	for _, as := range addrStrs {
		addr, err := pl.Swarm.ParseAddr([]byte(as))
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}
	return addrs
}

func (pl PeerList) find(id p2p.PeerID) int {
	for i, pi := range pl.Peers {
		if pi.ID == id {
			return i
		}
	}
	return -1
}
