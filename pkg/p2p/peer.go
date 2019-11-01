package p2p

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"log"

	"golang.org/x/crypto/ed25519"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"golang.org/x/crypto/sha3"
)

type PeerID = [32]byte

var NullPeerID = blobs.NullID

func NewPeerID(pk PublicKey) PeerID {
	data, err := x509.MarshalPKIXPublicKey(pk)
	if err != nil {
		log.Printf("type=%T", pk)
		panic(err)
	}
	sha3.Sum256(data)
	return sha3.Sum256(data)
}

type PrivateKey = crypto.Signer
type PublicKey = crypto.PublicKey

func GeneratePrivateKey() PrivateKey {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return priv
}

type Peer interface {
	ID() PeerID
	Ask(context.Context, []byte) ([]byte, error)
	Tell(context.Context, []byte) error
	PublicKey() PublicKey
}
