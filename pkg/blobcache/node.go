package blobcache

import (
	"context"
	"crypto/x509"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/bridges/fsbridge"
	"github.com/brendoncarroll/blobcache/pkg/p2p"
	"github.com/dustin/go-humanize"

	bolt "go.etcd.io/bbolt"
)

type runner interface {
	Run(context.Context) error
}

type KV interface {
	Put(ctx context.Context, k, v []byte) error
	Get(ctx context.Context, k []byte) ([]byte, error)
}

type Node struct {
	p2pNode p2p.Node

	bridgeDB *bolt.DB

	localStore *LocalStore
	dataDB     *bolt.DB

	stack []blobs.Getter
}

func NewNode(config Config) (*Node, error) {
	// local store
	dataPath := filepath.Join(config.DataDir, "data.db")
	dataDB, err := bolt.Open(dataPath, 0666, nil)
	if err != nil {
		return nil, err
	}
	capacity, err := humanize.ParseBytes(config.Capacity)
	if err != nil {
		return nil, err
	}
	localStore := &LocalStore{
		db:       dataDB,
		capacity: capacity,
	}

	bridgePath := filepath.Join(config.DataDir, "bridges.db")
	bridgeDB, err := bolt.Open(bridgePath, 0666, nil)
	if err != nil {
		return nil, err
	}

	stack := []blobs.Getter{}
	for _, spec := range config.Stack {
		var x blobs.Getter
		switch {
		case spec.MemLRU != nil:
			x = NewMemLRU(spec.MemLRU.Count)
		case spec.MemARC != nil:
			x = NewMemARC(spec.MemARC.Count)
		case spec.FSBridge != nil:
			bucketName := "fs_bridge"
			kv, err := newBoltKV(bridgeDB, []byte(bucketName))
			if err != nil {
				return nil, err
			}
			x = fsbridge.New(*spec.FSBridge, kv)
		case spec.Local != nil:
		case spec.Network != nil:
		default:
			log.Println("WARN: bad store spec", spec)
		}
		stack = append(stack, x)
	}

	privKey, err := x509.ParsePKCS8PrivateKey(config.PrivateKey)
	if err != nil {
		return nil, err
	}

	privKey2, ok := privKey.(p2p.PrivateKey)
	if !ok {
		panic("bad private key")
	}
	n := &Node{
		p2pNode: p2p.New(privKey2),
		dataDB:  dataDB,

		bridgeDB:   bridgeDB,
		localStore: localStore,

		stack: stack,
	}

	return n, nil
}

func (n *Node) Run(ctx context.Context) error {
	// find things that need to be run
	services := []runner{
		n.p2pNode,
	}
	for _, s := range n.stack {
		if r, ok := s.(runner); ok {
			services = append(services, r)
		}
	}

	log.Println("running", len(services), "services")

	// run them
	wg := sync.WaitGroup{}
	wg.Add(len(services))
	for _, r := range services {
		r := r
		go func() {
			if err := r.Run(ctx); err != nil {
				log.Println(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

func (n *Node) Post(ctx context.Context, data []byte) (blobs.ID, error) {
	var id blobs.ID
	errs := []error{}
	for _, s := range n.stack {
		poster, ok := s.(blobs.Poster)
		if !ok {
			continue
		}
		id1, err := poster.Post(ctx, data)
		if err != nil {
			errs = append(errs, err)
		}
		id = id1
	}

	return id, fmt.Errorf("%v", errs)
}

func (n *Node) Pin(ctx context.Context, id blobs.ID, storeLocal bool) error {
	return nil
}

func (n *Node) Get(ctx context.Context, id blobs.ID) (blobs.Blob, error) {
	return getChain(ctx, n.stack, id)
}

func (n *Node) AddPeer(pspec PeerSpec) {
	n.p2pNode.AddEdge(pspec.Edge)
}

func (n *Node) MaxBlobSize() int {
	return 1 << 16
}

func getChain(ctx context.Context, stores []blobs.Getter, id blobs.ID) ([]byte, error) {
	var (
		n    int
		data []byte
	)
	for i, store := range stores {
		data, err := store.Get(ctx, id)
		if err != nil {
			log.Println(err)
			continue
		}
		if data == nil {
			continue
		}

		n = i
		break
	}

	if len(data) > 0 {
		for ; n > 0; n-- {
			if ws, ok := stores[n-1].(blobs.Poster); ok {
				if _, err := ws.Post(ctx, data); err != nil {
					log.Println(err)
				}
			}
		}
	}

	return data, nil
}
