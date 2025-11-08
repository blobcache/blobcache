// bcnet implements the Blobcache Protocol (BCP).
package bcnet

import (
	"context"
	goed25519 "crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/bcp"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/quic-go/quic-go"
	"go.brendoncarroll.net/exp/singleflight"
	"go.brendoncarroll.net/p2p/s/swarmutil"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Message = bcp.Message

var _ bcp.Asker = (*Node)(nil)

type Node struct {
	privateKey ed25519.PrivateKey
	pc         net.PacketConn

	tp    *quic.Transport
	mu    sync.RWMutex
	conns map[blobcache.Endpoint]quic.Connection

	dialSF   singleflight.Group[blobcache.Endpoint, quic.Connection]
	fromDial chan quic.Connection
}

func New(privateKey ed25519.PrivateKey, pc net.PacketConn) *Node {
	tp := &quic.Transport{
		Conn: pc,
	}
	return &Node{
		tp:         tp,
		pc:         pc,
		privateKey: privateKey,
		conns:      make(map[blobcache.Endpoint]quic.Connection),
		fromDial:   make(chan quic.Connection),
	}
}

func (n *Node) LocalID() blobcache.PeerID {
	return inet256.NewID(n.privateKey.Public().(ed25519.PublicKey))
}

func (n *Node) LocalAddr() netip.AddrPort {
	return n.pc.LocalAddr().(*net.UDPAddr).AddrPort()
}

func (n *Node) LocalEndpoint() blobcache.Endpoint {
	return blobcache.Endpoint{
		Peer:   n.LocalID(),
		IPPort: n.LocalAddr(),
	}
}

// Tell opens a uni-stream to the given peer and sends the given request.
func (n *Node) Tell(ctx context.Context, remote blobcache.Endpoint, req bcp.Message) error {
	conn, err := n.getConn(ctx, remote)
	if err != nil {
		return err
	}
	stream, err := conn.OpenUniStream()
	if err != nil {
		return err
	}
	defer stream.Close()
	if _, err := req.WriteTo(stream); err != nil {
		return err
	}
	return nil
}

// Ask opens a bidirectional stream to the given peer and sends the given request, then waits for a response.
func (n *Node) Ask(ctx context.Context, remote blobcache.Endpoint, req Message, resp *Message) error {
	conn, err := n.getConn(ctx, remote)
	if err != nil {
		return err
	}
	stream, err := conn.OpenStream()
	if err != nil {
		return err
	}
	defer stream.Close()
	if _, err := req.WriteTo(stream); err != nil {
		return err
	}
	if _, err := resp.ReadFrom(stream); err != nil {
		return err
	}
	return nil
}

// Serve blocks handling all incoming connections, until ctx is cancelled.
func (n *Node) Serve(ctx context.Context, srv Server) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// handle connections from listening
	lis, err := n.tp.Listen(n.makeListenTlsConfig(), n.makeQuicConfig())
	if err != nil {
		return err
	}
	defer lis.Close()

	// handle connections from dialing
	go func() {
		for {
			select {
			case <-ctx.Done():
				lis.Close()
				return
			case conn := <-n.fromDial:
				ep := blobcache.Endpoint{
					Peer:   n.LocalID(),
					IPPort: ipPortFromConn(conn),
				}
				n.maybeSpawnHandler(ctx, ep, conn, srv.serve)
			}
		}
	}()

	for {
		conn, err := lis.Accept(ctx)
		if err != nil {
			return err
		}
		peerID, err := peerIDFromTLSState(conn.ConnectionState().TLS)
		if err != nil {
			conn.CloseWithError(1, "invalid peer id")
			continue
		}
		ep := blobcache.Endpoint{
			Peer:   *peerID,
			IPPort: ipPortFromConn(conn),
		}
		n.maybeSpawnHandler(ctx, ep, conn, srv.serve)
	}
}

func (n *Node) maybeSpawnHandler(ctx context.Context, ep blobcache.Endpoint, conn quic.Connection, fn messageHandler) {
	if _, added := n.attemptAddConn(ep, conn); added {
		go func() {
			if err := n.handleConn(ctx, ep, conn, fn); err != nil {
				logctx.Warn(ctx, "error handling connection", zap.Error(err))
			}
		}()
	} else {
		conn.CloseWithError(1, "found existing connection")
	}
}

// attemptAddConn gets the lock and adds the connection to the map if it is not already present.
// If it is already present, it returns the existing connection and false.
func (n *Node) attemptAddConn(ep blobcache.Endpoint, x quic.Connection) (quic.Connection, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if prevConn, exists := n.conns[ep]; exists {
		return prevConn, false
	} else {
		n.conns[ep] = x
		return x, true
	}
}

func (n *Node) handleConn(ctx context.Context, remote blobcache.Endpoint, conn quic.Connection, fn messageHandler) error {
	defer conn.CloseWithError(0, "")
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		for {
			s, err := conn.AcceptStream(ctx)
			if err != nil {
				return err
			}
			go func() {
				if err := n.handleStream(ctx, remote, s, fn); err != nil {
					logctx.Warn(ctx, "error handling stream", zap.Error(err))
				}
			}()
		}
	})
	eg.Go(func() error {
		for {
			s, err := conn.AcceptUniStream(ctx)
			if err != nil {
				return err
			}
			go func() {
				if err := n.handleUniStream(ctx, remote, s, fn); err != nil {
					logctx.Warn(ctx, "error handling uni-stream", zap.Error(err))
				}
			}()
		}
	})
	return eg.Wait()
}

// getConn returns a connection to the specified peer
func (node *Node) getConn(ctx context.Context, ep blobcache.Endpoint) (quic.Connection, error) {
	node.mu.RLock()
	conn := node.conns[ep]
	node.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}
	conn, err, _ := node.dialSF.Do(ep, func() (quic.Connection, error) {
		// check if there is a conn again.
		node.mu.RLock()
		conn := node.conns[ep]
		node.mu.RUnlock()
		if conn != nil {
			return conn, nil
		}

		conn, err := node.dialConn(ctx, ep)
		if err != nil {
			return nil, err
		}
		if _, added := node.attemptAddConn(ep, conn); added {
			node.fromDial <- conn
		} else {
			conn.CloseWithError(1, "found existing connection")
			return conn, nil
		}
		return conn, nil
	})
	return conn, err
}

// dialConn dials a new quic connection and returns it.
// It does not modify peers or take any locks.
func (qt *Node) dialConn(ctx context.Context, ep blobcache.Endpoint) (quic.Connection, error) {
	conn, err := qt.tp.Dial(ctx, net.UDPAddrFromAddrPort(ep.IPPort), qt.makeDialTlsConfig(ep.Peer), qt.makeQuicConfig())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (qt *Node) handleStream(ctx context.Context, ep blobcache.Endpoint, s quic.Stream, fn messageHandler) error {
	var req Message
	if _, err := req.ReadFrom(s); err != nil {
		return err
	}
	var resp Message
	fn(ctx, ep, &req, &resp)
	if _, err := resp.WriteTo(s); err != nil {
		return err
	}
	return s.Close()
}

func (qt *Node) handleUniStream(ctx context.Context, ep blobcache.Endpoint, s quic.ReceiveStream, fn messageHandler) error {
	var req Message
	if _, err := req.ReadFrom(s); err != nil {
		return err
	}
	fn(ctx, ep, &req, nil)
	return nil
}

type messageHandler = func(ctx context.Context, ep blobcache.Endpoint, req *Message, resp *Message)

// makeDialTlsConfig is called to create a tls.Config for outbound connections
func (qt *Node) makeDialTlsConfig(desiredPeer blobcache.PeerID) *tls.Config {
	cfg := qt.makeTlsConfig()
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		peer, err := peerIDFromTLSState(cs)
		if err != nil {
			return err
		}
		if *peer != desiredPeer {
			return fmt.Errorf("wrong peer: connected=%v expecting=%v", *peer, desiredPeer)
		}
		return nil
	}
	return cfg
}

// makeListenTlsConfig is called by the server side to create new connections.
func (qt *Node) makeListenTlsConfig() *tls.Config {
	return qt.makeTlsConfig()
}

func (n *Node) makeTlsConfig() *tls.Config {
	privateKey := goed25519.PrivateKey(n.privateKey)
	cert := swarmutil.GenerateSelfSigned(privateKey)
	localID := n.LocalID()
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ClientAuth:         tls.RequireAnyClientCert,
		ServerName:         localID.String(),
		InsecureSkipVerify: true,
	}
}

func (qt *Node) makeQuicConfig() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams:    1 << 16,
		MaxIncomingUniStreams: 1 << 16,
	}
}

func peerIDFromTLSState(tlsState tls.ConnectionState) (*blobcache.PeerID, error) {
	var cert *x509.Certificate
	switch len(tlsState.PeerCertificates) {
	case 0:
		return nil, errors.New("peer provided no certificates")
	case 1:
		cert = tlsState.PeerCertificates[0]
	default:
		return nil, errors.New("peer provided too many certificates")
	}

	switch pubKey := cert.PublicKey.(type) {
	case goed25519.PublicKey:
		peerID := inet256.NewID(ed25519.PublicKey(pubKey))
		return &peerID, nil
	default:
		return nil, fmt.Errorf("unsupported key type: %T", cert.PublicKey)
	}
}

func ipPortFromConn(x quic.Connection) netip.AddrPort {
	udpAddr := x.RemoteAddr().(*net.UDPAddr)
	return udpAddr.AddrPort()
}
