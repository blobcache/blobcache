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

type PeerList []PeerSpec

func (pl PeerList) TrustFor(id p2p.PeerID) (int64, error) {
	i := pl.find(id)
	if i < 0 {
		return 0, errors.New("peer not found")
	}
	return pl[i].Trust, nil
}

func (pl PeerList) ListPeers() []p2p.PeerID {
	ids := make([]p2p.PeerID, len(pl))
	for i := range pl {
		ids[i] = pl[i].ID
	}
	return ids
}

func (pl PeerList) find(id p2p.PeerID) int {
	for i, pi := range pl {
		if pi.ID == id {
			return i
		}
	}
	return -1
}
