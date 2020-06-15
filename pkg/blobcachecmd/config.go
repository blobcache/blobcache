package blobcachecmd

import (
	"crypto/ed25519"
	"crypto/x509"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/simplemux"
	"github.com/brendoncarroll/go-p2p/s/multiswarm"
	"github.com/brendoncarroll/go-p2p/s/quicswarm"
	bolt "go.etcd.io/bbolt"
	"gopkg.in/yaml.v3"
)

const DefaultAPIAddr = "127.0.0.1:6025"

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
	PrivateKey   []byte `yaml:"private_key,flow"`
	PersistDir   string `yaml:"persist_dir"`
	EphemeralDir string `yaml:"ephemeral_dir"`

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
		PrivateKey:   pkData,
		PersistDir:   ".",
		EphemeralDir: ".",

		QUICAddr: "0.0.0.0:",
		APIAddr:  DefaultAPIAddr,

		EphemeralCap:  1000,
		PersistentCap: 1000,
		Peers:         nil,
	}
}

func buildParams(configPath string, c Config) (*blobcache.Params, error) {
	var persistDir, ephemeralDir string
	if strings.HasPrefix(c.PersistDir, ".") {
		persistDir = filepath.Join(configPath, c.PersistDir)
	}
	persistPath := filepath.Join(persistDir, "blobcache_persist.db")

	if strings.HasPrefix(c.EphemeralDir, ".") {
		ephemeralDir = filepath.Join(configPath, c.EphemeralDir)
	}
	ephemeralPath := filepath.Join(ephemeralDir, "blobcache_ephemeral.db")

	ephemeralDB, err := bolt.Open(ephemeralPath, 0666, nil)
	if err != nil {
		return nil, err
	}
	persistDB, err := bolt.Open(persistPath, 0666, nil)
	if err != nil {
		return nil, err
	}

	privKey, err := x509.ParsePKCS8PrivateKey(c.PrivateKey)
	if err != nil {
		return nil, err
	}

	swarm, err := setupSwarm(privKey.(p2p.PrivateKey), c.QUICAddr)
	if err != nil {
		return nil, err
	}
	mux := simplemux.MultiplexSwarm(swarm)

	return &blobcache.Params{
		Mux: mux,
		PeerStore: &peers.PeerList{
			Peers: c.Peers,
			Swarm: swarm,
		},
		PrivateKey: privKey.(p2p.PrivateKey),

		Ephemeral:  bcstate.NewBoltDB(ephemeralDB),
		Persistent: bcstate.NewBoltDB(persistDB),
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
