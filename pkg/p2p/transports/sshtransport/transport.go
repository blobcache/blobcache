package sshtransport

import (
	"context"
	"encoding/hex"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/brendoncarroll/blobcache/pkg/p2p"
)

var _ p2p.Transport = &Transport{}

type Transport struct {
	privateKey ssh.Signer
	publicKeys p2p.PublicKeyStore

	mu        sync.Mutex
	edges     map[uint]p2p.Edge
	listeners map[string]net.Listener

	sconns map[uint]ssh.Conn

	onAsk  p2p.SyncHandler
	onTell p2p.AsyncHandler
}

func New() *Transport {
	return &Transport{
		edges:     make(map[uint]p2p.Edge),
		listeners: make(map[string]net.Listener),
		sconns:    make(map[uint]ssh.Conn),
	}
}

func (t *Transport) Init(privKey p2p.PrivateKey, pubKeys p2p.PublicKeyStore) {
	sshPrivKey, err := ssh.NewSignerFromSigner(privKey)
	if err != nil {
		panic(err)
	}
	t.publicKeys = pubKeys
	t.privateKey = sshPrivKey
}

func (t *Transport) Shutdown() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sconn := range t.sconns {
		sconn.Close()
	}
	for _, l := range t.listeners {
		l.Close()
	}
}

func (t *Transport) AddEdge(i uint, e p2p.Edge) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.edges[i] = e

	if e.LocalAddr != "" {
		if _, exists := t.listeners[e.LocalAddr]; !exists {
			l, err := net.Listen("tcp", e.LocalAddr)
			if err != nil {
				log.Println(err)
			}
			t.listeners[e.LocalAddr] = l
			go t.listenerLoop(l)
		}
	}

	if e.RemoteAddr != "" {
		go t.dialerLoop(i)
	}
}

func (t *Transport) DeleteEdge(i uint) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e := t.edges[i]
	delete(t.edges, i)

	l, exists := t.listeners[e.LocalAddr]
	if !exists {
		return
	}
	if err := l.Close(); err != nil {
		log.Println(err)
	}
	delete(t.listeners, e.LocalAddr)
}

func (t *Transport) OnTell(fn p2p.AsyncHandler) {
	t.onTell = fn
}

func (t *Transport) Tell(ctx context.Context, msg p2p.Message) error {
	t.mu.Lock()
	sconn := t.sconns[msg.Edge]
	t.mu.Unlock()

	_, _, err := sconn.SendRequest(msg.Protocol, false, msg.Payload)
	return err
}

func (t *Transport) OnAsk(fn p2p.SyncHandler) {
	t.onAsk = fn
}

func (t *Transport) Ask(ctx context.Context, msg p2p.Message) ([]byte, error) {
	t.mu.Lock()
	sconn := t.sconns[msg.Edge]
	t.mu.Unlock()
	_, reply, err := sconn.SendRequest(msg.Protocol, true, msg.Payload)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (t *Transport) dialerLoop(i uint) {
	const period = 30 * time.Second

	for {
		t.mu.Lock()
		e, exists := t.edges[i]
		t.mu.Unlock()
		if !exists {
			return
		}

		c, err := net.Dial("tcp", e.RemoteAddr)
		if err != nil {
			log.Println(err)
			time.Sleep(period)
		}

		addr := e.RemoteAddr
		sconn, newChans, requests, err := ssh.NewClientConn(c, addr, t.getClientConfig(e.LocalAddr))
		if err != nil {
			log.Println(err)
			time.Sleep(period)
		}

		t.sconns[i] = sconn
		t.sconnLoop(e.PeerID, sconn, newChans, requests)
	}
}

func (t *Transport) listenerLoop(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println(err)
			break
		}

		go t.serverLoop(c)
	}
}

func (t *Transport) serverLoop(c net.Conn) {
	sconn, newChans, requests, err := ssh.NewServerConn(c, t.getServerConfig())
	if err != nil {
		log.Println(err)
		return
	}
	t.sconnLoop(sconn, newChans, requests)
}

func (t *Transport) sconnLoop(peerID p2p.PeerID, sconn ssh.Conn, newChans <-chan ssh.NewChannel, requests <-chan *ssh.Request) {
	t.mu.Lock()
	t.sconns[uint(i)] = sconn
	t.mu.Unlock()

	for {
		select {
		case req, stillOpen := <-requests:
			if !stillOpen {
				return
			}
			msg := p2p.Message{
				Protocol: req.Type,
				Payload:  req.Payload,
			}
			if req.WantReply {
				data, err := t.onAsk(msg)
				if err != nil {
					log.Println(err)
				}
				if err := req.Reply(err == nil, data); err != nil {
					log.Println(err)
				}
			} else {
				t.onTell(msg)
			}

		case ch, stillOpen := <-newChans:
			if !stillOpen {
				return
			}
			log.Println("rejecting channel request", ch)
			ch.Reject(ssh.UnknownChannelType, "")
		}
	}
}

func (t *Transport) getServerConfig() *ssh.ServerConfig {
	conf := &ssh.ServerConfig{
		PublicKeyCallback: func(cmeta ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			peerID := p2p.NewPeerID(pubKey)
			e := p2p.Edge{
				PeerID:     peerID,
				LocalAddr:  cmeta.LocalAddr().String(),
				RemoteAddr: cmeta.RemoteAddr().String(),
			}

			i, err := t.findEdge(e)
			if err != nil {
				return nil, err
			}
			return &perms, nil
		},
	}
	conf.Config.SetDefaults()
	conf.AddHostKey(t.privateKey)
	return conf
}

func (t *Transport) getClientConfig(localAddr string) *ssh.ClientConfig {
	conf := &ssh.ClientConfig{
		HostKeyCallback: func(hostname string, remote net.Addr, pubKey ssh.PublicKey) error {
			peerID := p2p.NewPeerID(pubKey)
			e := p2p.Edge{
				PeerID:     peerID,
				LocalAddr:  localAddr,
				RemoteAddr: remote.String(),
			}

			_, err := t.findEdge(e)
			if err != nil {
				return err
			}

			return nil
		},
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(t.privateKey),
		},
	}
	conf.SetDefaults()
	return conf
}

func (t *Transport) findEdge(a p2p.Edge) (uint, error) {
	for i, b := range t.edges {
		if b.Matches(a) {
			return i, nil
		}
	}
	return 0, errors.New("edge is not desired")
}

func peerID2String(x p2p.PeerID) string {
	return hex.EncodeToString(x[:])
}

func string2PeerID(x string) p2p.PeerID {
	data, err := hex.DecodeString(x)
	if err != nil {
		panic(err)
	}
	y := p2p.PeerID{}
	copy(y[:], data)
	return y
}
