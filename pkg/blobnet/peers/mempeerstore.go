package peers

import "github.com/brendoncarroll/go-p2p"

type MemPeerStore map[p2p.PeerID][]p2p.Addr

func (ps MemPeerStore) AddAddr(id p2p.PeerID, addr p2p.Addr) {
	addrs := ps[id]
	addrs = append(addrs, addr)
	ps[id] = addrs
}

func (ps MemPeerStore) GetAddrs(id p2p.PeerID) []p2p.Addr {
	return ps[id]
}

func (ps MemPeerStore) ListPeers() []p2p.PeerID {
	ids := []p2p.PeerID{}
	for id := range ps {
		ids = append(ids, id)
	}
	return ids
}

func (ps MemPeerStore) TrustFor(id p2p.PeerID) (int64, error) {
	return 0, nil
}
