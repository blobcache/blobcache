package blobcachecmd

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/blobcache/blobcache/pkg/stores"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/mbapp"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/inet256/inet256/client/go_client/inet256client"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"gopkg.in/yaml.v3"
)

const DefaultAPIAddr = "127.0.0.1:6025"

type Config struct {
	PrivateKey string           `yaml:"private_key,flow"`
	DataDir    string           `yaml:"data_dir"`
	APIAddr    string           `yaml:"api_addr"`
	Peers      []peers.PeerSpec `yaml:"peers"`
	Stores     []StoreSpec      `yaml:"stores"`
}

type LocalStoreSpec struct {
	Capacity string
}

type StoreSpec struct {
	Local *LocalStoreSpec `yaml:"local,omitempty"`
}

func (c *Config) Marshal() []byte {
	data, err := yaml.Marshal(c)
	if err != nil {
		panic(err)
	}
	return data
}

func (c *Config) Unmarshal(data []byte) error {
	return yaml.Unmarshal(data, c)
}

func DefaultConfig() *Config {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic(err)
	}
	pkData, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		panic(err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkData,
	})

	return &Config{
		PrivateKey: string(privPEM),
		DataDir:    ".",

		APIAddr: DefaultAPIAddr,
		Stores: []StoreSpec{
			{Local: &LocalStoreSpec{}},
		},
		Peers: nil,
	}
}

func buildParams(configPath string, c Config) (*blobcache.Params, error) {
	configDir := filepath.Dir(configPath)

	var dataDir string
	if strings.HasPrefix(c.DataDir, ".") {
		dataDir = filepath.Join(configDir, c.DataDir)
	}
	dbPath := filepath.Join(dataDir, "blobcache.db")
	db, err := bolt.Open(dbPath, 0o644, nil)
	if err != nil {
		return nil, err
	}

	// Private Key
	block, _ := pem.Decode([]byte(c.PrivateKey))
	if block.Type != "PRIVATE KEY" {
		return nil, errors.Errorf("wrong PEM type for private key %s", block.Type)
	}
	privKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	// Peers
	for i, peerSpec := range c.Peers {
		if peerSpec.ID.Equals(p2p.ZeroPeerID()) {
			return nil, errors.Errorf("peer # %d cannot have zero id", i)
		}
	}
	stores, err := makeStores(dataDir, c.Stores)
	if err != nil {
		return nil, err
	}
	return &blobcache.Params{
		PrivateKey: privKey.(p2p.PrivateKey),
		Stores:     stores,
		DB:         bcstate.NewBoltDB(db),
	}, nil
}

func makeStores(dataDir string, specs []StoreSpec) ([]blobcache.Store, error) {
	ret := make([]blobcache.Store, len(specs))
	for i, spec := range specs {
		var s blobcache.Store
		var err error
		switch {
		case spec.Local != nil:
			s, err = makeLocalStore(dataDir, i, *spec.Local)
			if err != nil {
				return nil, err
			}
		default:
			err = errors.Errorf("invalid store spec")
		}
		if err != nil {
			return nil, err
		}
		ret[i] = s
	}
	return ret, nil
}

func makeLocalStore(dataDir string, i int, spec LocalStoreSpec) (blobcache.Store, error) {
	p := makeStorePath(dataDir, i)
	fs := posixfs.NewDirFS(p)
	return stores.NewFSStore(fs), nil
}

func makeStorePath(dataDir string, i int) string {
	return path.Join(dataDir, "stores", fmt.Sprint(i))
}

func setupSwarm(privKey p2p.PrivateKey) (p2p.SecureAskSwarm, error) {
	client, err := inet256client.NewEnvClient()
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	node, err := client.CreateNode(ctx, privKey)
	if err != nil {
		return nil, err
	}
	sw := inet256client.NewSwarm(node, privKey.Public())
	return mbapp.New(sw, 1<<22), nil
}

type ConfigFile struct {
	p string
}

func NewConfigFile(p string) ConfigFile {
	return ConfigFile{p}
}

func (cf ConfigFile) Load() (Config, error) {
	data, err := ioutil.ReadFile(cf.p)
	if err != nil {
		return Config{}, err
	}
	config := Config{}
	if err := config.Unmarshal(data); err != nil {
		return Config{}, err
	}
	return config, nil
}
