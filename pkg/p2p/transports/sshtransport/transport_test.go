package sshtransport

import (
	"context"
	"testing"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/p2p"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"golang.org/x/crypto/ed25519"
	"golang.org/x/crypto/ssh"
)

func TestTransport(t *testing.T) {
	seed1 := make([]byte, 32)
	seed1[0] = 'a'
	seed2 := make([]byte, 32)
	seed2[0] = 'b'

	priv1, _ := ssh.NewSignerFromKey(ed25519.NewKeyFromSeed(seed1))
	priv2, _ := ssh.NewSignerFromKey(ed25519.NewKeyFromSeed(seed2))

	t1 := New()
	t1.Init(priv1)
	t2 := New()
	t2.Init(priv2)

	peer1 := p2p.NewPeerID(priv1.PublicKey())
	peer2 := p2p.NewPeerID(priv2.PublicKey())

	eT1T2 := p2p.Edge{
		PeerID:     peer2,
		LocalAddr:  "127.0.0.1:50001",
		RemoteAddr: "",
	}
	t1.AddEdge(1, eT1T2)

	eT2T1 := p2p.Edge{
		PeerID:     peer1,
		LocalAddr:  "",
		RemoteAddr: eT1T2.LocalAddr,
	}
	t2.AddEdge(1, eT2T1)

	time.Sleep(time.Second)
	gotMessage := false
	ctx := context.TODO()
	t1.OnTell(func(p2p.Message) {
		gotMessage = true
	})
	err := t2.Tell(ctx, p2p.Message{
		DstPeerID: peer1,
		Edge:      1,
		Payload:   []byte("hello world"),
	})
	time.Sleep(time.Second)
	require.Nil(t, err)

	assert.True(t, gotMessage)
}
