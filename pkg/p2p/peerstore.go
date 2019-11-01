package p2p

import "sync"

type PublicKeyStore interface {
	Post(PublicKey) PeerID
	Lookup(PeerID) PublicKey
}

type peerStore struct {
	m sync.Map
}

func (s *peerStore) Post(pk PublicKey) PeerID {
	id := NewPeerID(pk)
	s.m.Store(id, pk)
	return id
}

func (s *peerStore) Lookup(id PeerID) PublicKey {
	pk, exists := s.m.Load(id)
	if !exists {
		return nil
	}
	return pk.(PublicKey)
}

func (s *peerStore) List() (ret []PeerID) {
	s.m.Range(func(key, value interface{}) bool {
		ret = append(ret, key.(PeerID))
		return true
	})
	return ret
}
