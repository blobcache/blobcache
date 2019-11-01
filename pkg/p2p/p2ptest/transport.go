package p2ptest

import (
	"context"
	"errors"
	"strconv"

	mrand "math/rand"

	"github.com/brendoncarroll/blobcache/pkg/p2p"
)

type Realm struct {
	node2Transport map[p2p.Node]*Transport
	transports     []*Transport
	dropRate       float64
}

func (r *Realm) SetDropRate(x float64) {
	r.dropRate = x
}

func (r *Realm) New(n p2p.Node) *Transport {
	if r.node2Transport == nil {
		r.node2Transport = map[p2p.Node]*Transport{}
	}
	t := &Transport{
		id:      len(r.transports),
		r:       r,
		edges:   make(map[uint]p2p.Edge),
		allowed: make(map[int]struct{}),
	}
	r.transports = append(r.transports, t)
	r.node2Transport[n] = t

	return t
}

type Transport struct {
	id int
	r  *Realm

	edges      map[uint]p2p.Edge
	allowed    map[int]struct{}
	localID    p2p.PeerID
	publicKeys p2p.PublicKeyStore

	onAsk  p2p.AskHandler
	onTell p2p.TellHandler
}

func (t *Transport) Init(pk p2p.PrivateKey, keys p2p.PublicKeyStore) {
	t.localID = p2p.NewPeerID(pk.Public())
	t.publicKeys = keys
}

func (t *Transport) AddEdge(i uint, e p2p.Edge) {
	t.edges[i] = e
	remoteAddr, _ := strconv.Atoi(e.RemoteAddr)
	t.allowed[remoteAddr] = struct{}{}
}

func (t *Transport) DeleteEdge(i uint) {
	e := t.edges[i]
	delete(t.edges, i)
	remoteAddr, _ := strconv.Atoi(e.RemoteAddr)
	delete(t.allowed, remoteAddr)
}

func (t *Transport) Tell(ctx context.Context, msg p2p.Message) error {
	e := t.edges[msg.Edge]
	i, _ := strconv.Atoi(e.RemoteAddr)
	if _, exists := t.allowed[i]; !exists {
		return errors.New("no connection to peer")
	}

	remote := t.r.transports[i]
	fn := remote.onTell
	if fn == nil {
		return nil
	}
	remote.publicKeys.Post(t.localID)
	fn(msg)
	return nil
}

func (t *Transport) Ask(ctx context.Context, msg p2p.Message) ([]byte, error) {
	e := t.edges[msg.Edge]
	i, _ := strconv.Atoi(e.RemoteAddr)
	if _, exists := t.allowed[i]; !exists {
		return nil, errors.New("no connection to peer")
	}

	// randomly drop the message
	if mrand.Float64() < t.r.dropRate {
		return nil, nil
	}

	remote := t.r.transports[i]
	fn := remote.onAsk
	if fn == nil {
		return nil, nil
	}
	remote.publicKeys.Post(t.localID)

	return fn(msg), nil
}

func (t *Transport) OnAsk(fn p2p.AskHandler) {
	t.onAsk = fn
}

func (t *Transport) OnTell(fn p2p.TellHandler) {
	t.onTell = fn
}

func (t *Transport) Addr() string {
	return strconv.Itoa(t.id)
}
