package peerrouting

import (
	"sync"

	"github.com/brendoncarroll/go-p2p"
)

type LinkMap struct {
	mu   sync.RWMutex
	n    int
	atoi map[p2p.PeerID]int
	itoa map[int]p2p.PeerID
}

func NewLinkMap() *LinkMap {
	return &LinkMap{
		n:    0,
		atoi: make(map[p2p.PeerID]int),
		itoa: make(map[int]p2p.PeerID),
	}
}

func (lm *LinkMap) Int(id p2p.PeerID) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	i, ok := lm.atoi[id]
	if ok {
		return i
	}

	i = lm.n
	lm.n++
	lm.atoi[id] = i
	lm.itoa[i] = id
	return i
}

func (lm *LinkMap) Peer(i int) p2p.PeerID {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.itoa[i]
}
