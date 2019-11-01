package p2p

import (
	"context"
)

type TMessage struct {
	SrcPeer, DstPeer PeerID

	Edge     uint
	Protocol string
	Payload  []byte
}

type Transport interface {
	Init(privKey PrivateKey, pks PublicKeyStore)
	AddEdge(int, Edge)
	DeleteEdge(int)
}

type SyncHandler func(Message) ([]byte, error)

type SyncTransport interface {
	Transport
	OnAsk(mh SyncHandler)
	Ask(context.Context, Message) ([]byte, error)
}

type AsyncHandler func(Message)

type AsyncTransport interface {
	Transport
	OnTell(th AsyncHandler)
	Tell(ctx context.Context, m Message) error
}
