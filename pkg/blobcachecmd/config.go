package blobcachecmd

import (
	"crypto/ed25519"
	"crypto/x509"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/brendoncarroll/blobcache/pkg/bckv"
	"github.com/brendoncarroll/blobcache/pkg/blobcache"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/peers"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	"github.com/brendoncarroll/go-p2p/s/multiswarm"
	"github.com/brendoncarroll/go-p2p/s/quicswarm"
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
	PrivateKey    []byte           `yaml:"private_key,flow"`
	DataDir       string           `yaml:"data_dir"`
	QUICAddr      string           `yaml:"quic_addr"`
	APIAddr       string           `yaml:"api_addr"`
	EphemeralCap  uint64           `yaml:"ephemeral_capacity"`
	PersistentCap uint64           `yaml:"persistent_capacity"`
	Peers         []peers.PeerSpec `yaml:"peers"`
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

	return &Config{
		PrivateKey: pkData,
		DataDir:    ".",

		QUICAddr: "0.0.0.0:",
		APIAddr:  "127.0.0.1:6025",

		EphemeralCap:  1000,
		PersistentCap: 1000,
		Peers:         nil,
	}
}

func buildParams(configPath string, c Config) (*blobcache.Params, error) {
	dataDir := c.DataDir
	if strings.HasPrefix(c.DataDir, "./") {
		dataDir = filepath.Join(configPath, c.DataDir)
	}
	persistentPath := filepath.Join(dataDir, "blobcache_persistent.db")
	persistentDB, err := bolt.Open(persistentPath, 0666, nil)
	if err != nil {
		return nil, err
	}
	ephemeralPath := filepath.Join(dataDir, "blobcache_ephemeral.db")
	ephemeralDB, err := bolt.Open(ephemeralPath, 0666, nil)
	if err != nil {
		return nil, err
	}
	metadataPath := filepath.Join(dataDir, "blobcache_metadata.db")
	metadataDB, err := bolt.Open(metadataPath, 0666, nil)
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

	swarm, err := setupSwarm(privKey2, c.QUICAddr)
	if err != nil {
		return nil, err
	}
	mux := simplemux.MultiplexSwarm(swarm)

	return &blobcache.Params{
		Mux:        mux,
		PeerStore:  &peers.PeerList{},
		MetadataDB: metadataDB,
		PrivateKey: privKey2,

		Ephemeral:  bckv.NewBoltKV(ephemeralDB, c.EphemeralCap),
		Persistent: bckv.NewBoltKV(persistentDB, c.PersistentCap),
	}, nil
}

func setupSwarm(privKey p2p.PrivateKey, quicAddr string) (p2p.Swarm, error) {
	quicSw, err := quicswarm.New(quicAddr, privKey)
	if err != nil {
		return nil, err
	}
	transports := map[string]p2p.SecureAskSwarm{
		"quic": quicSw,
	}
	return multiswarm.NewSecureAsk(transports), nil
}
