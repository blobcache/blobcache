package blobcache

import (
	"crypto/x509"
	"io/ioutil"
	"path/filepath"

	"github.com/brendoncarroll/blobcache/pkg/bridges/fsbridge"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/aggswarm"
	"github.com/brendoncarroll/go-p2p/sshswarm"
	"github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
	"gopkg.in/yaml.v3"
)

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
	yaml.Unmarshal(data, &config)
	return config, nil
}

func (cf ConfigFile) Save(c Config) error {
	data, _ := yaml.Marshal(c)
	return ioutil.WriteFile(cf.p, data, 0644)
}

type Config struct {
	PrivateKey []byte     `yaml:"private_key,flow"`
	DataDir    string     `yaml:"data_dir"`
	Capacity   string     `yaml:"capacity"`
	Peers      []PeerSpec `yaml:"peers"`

	Stack []StoreSpec `yaml:"stack"`
}

func (c *Config) Params() (*Params, error) {
	// local store
	dataPath := filepath.Join(c.DataDir, "data.db")
	dataDB, err := bolt.Open(dataPath, 0666, nil)
	if err != nil {
		return nil, err
	}
	metadataPath := filepath.Join(c.DataDir, "metadata.db")
	metadataDB, err := bolt.Open(metadataPath, 0666, nil)
	if err != nil {
		return nil, err
	}
	capacity, err := humanize.ParseBytes(c.Capacity)
	if err != nil {
		return nil, err
	}

	privKey, err := x509.ParsePKCS8PrivateKey(c.PrivateKey)
	if err != nil {
		return nil, err
	}

	privKey2, ok := privKey.(p2p.PrivateKey)
	if !ok {
		panic("bad private key")
	}

	swarm, err := setupSwarm(privKey2)
	if err != nil {
		return nil, err
	}

	return &Params{
		Swarm: swarm,

		DataDB:     dataDB,
		MetadataDB: metadataDB,

		Capacity: capacity,
	}, nil
}

func setupSwarm(privKey p2p.PrivateKey) (p2p.Swarm, error) {
	sshs, err := sshswarm.New("[]:", privKey)
	if err != nil {
		return nil, err
	}

	transports := map[string]aggswarm.Transport{
		"ssh": sshs,
	}
	return aggswarm.New(privKey, transports), nil
}

type PeerSpec struct {
	Edge     aggswarm.Edge
	Trust    int64
	Nickname string
}

type StoreSpec struct {
	FSBridge *fsbridge.Spec `yaml:"fs_bridge,omitempty"`

	MemLRU *MemLRUSpec `yaml:"mem_lru,omitempty"`
	MemARC *MemARCSpec `yaml:"mem_arc,omitempty"`

	Local   *LocalStoreSpec `yaml:"local,omitempty"`
	Network *NetStoreSpec   `yaml:"network,omitempty"`
}

type LocalStoreSpec struct{}

type MemLRUSpec struct {
	Count int `yaml:"count"`
}

type MemARCSpec struct {
	Count int `yaml:"count"`
}

type NetStoreSpec struct{}
