package p2pservices

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/p2p"
)

type HeartbeatService struct {
	swarm           p2p.Swarm
	lastHeard       sync.Map
	period, timeout time.Duration
}

func NewHeartbeat(swarm p2p.Swarm, period, timeout time.Duration) *HeartbeatService {
	if timeout <= period {
		panic("timeout is <= period")
	}
	return &HeartbeatService{
		swarm:   swarm,
		period:  period,
		timeout: timeout,
	}
}

func (s *HeartbeatService) Run(ctx context.Context) error {
	go s.serve(ctx)

	tick := time.NewTicker(s.period)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			for _, id := range s.swarm.Peers() {
				peer := s.swarm.Peer(id)
				if err := peer.Tell(ctx, []byte("hello")); err != nil {
					log.Println(err)
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *HeartbeatService) Ping(ctx context.Context, peerID p2p.PeerID) bool {
	peer := s.swarm.Peer(peerID)
	res, err := peer.Ask(ctx, []byte("ping"))
	if err != nil {
		return false
	}

	return bytes.Compare(res, []byte("pong")) == 0
}

func (s *HeartbeatService) IsAlive(peerID p2p.PeerID) bool {
	x, exists := s.lastHeard.Load(peerID)
	return exists && x.(time.Time).Add(s.timeout).After(time.Now())
}

func (s *HeartbeatService) serve(ctx context.Context) {
	s.swarm.OnAsk(func(msg p2p.Message) []byte {
		return []byte("pong")
	})
	s.swarm.OnTell(func(msg p2p.Message) {
		s.lastHeard.Store(msg.SrcID, time.Now())
	})
}
