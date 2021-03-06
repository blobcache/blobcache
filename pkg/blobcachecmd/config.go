package blobcachecmd

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/s/peerswarm"
	"github.com/docker/go-units"
	"github.com/inet256/inet256/pkg/inet256p2p"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"gopkg.in/yaml.v3"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
)

const DefaultAPIAddr = "127.0.0.1:6025"

type Config struct {
	PrivateKey   string `yaml:"private_key,flow"`
	PersistDir   string `yaml:"persist_dir"`
	EphemeralDir string `yaml:"ephemeral_dir"`

	INet256API    string           `yaml:"inet256_api"`
	APIAddr       string           `yaml:"api_addr"`
	EphemeralCap  string           `yaml:"ephemeral_capacity"`
	PersistentCap string           `yaml:"persistent_capacity"`
	Peers         []peers.PeerSpec `yaml:"peers"`
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
		PrivateKey:   string(privPEM),
		PersistDir:   ".",
		EphemeralDir: ".",

		INet256API: "127.0.0.1:25600",
		APIAddr:    DefaultAPIAddr,

		EphemeralCap:  "10GB",
		PersistentCap: "1GB",
		Peers:         nil,
	}
}

func buildParams(configPath string, c Config) (*blobcache.Params, error) {
	configDir := filepath.Dir(configPath)
	// Capacities
	persistCap, err := units.FromHumanSize(c.PersistentCap)
	if err != nil {
		return nil, errors.Errorf("invalid peristent_capacity (%s)", c.PersistentCap)
	}
	ephemeralCap, err := units.FromHumanSize(c.EphemeralCap)
	if err != nil {
		return nil, errors.Errorf("invalid ephemeral_capacity (%s)", c.EphemeralCap)
	}

	var persistDir, ephemeralDir string
	if strings.HasPrefix(c.PersistDir, ".") {
		persistDir = filepath.Join(configDir, c.PersistDir)
	}
	persistPath := filepath.Join(persistDir, "blobcache_persist.db")

	if strings.HasPrefix(c.EphemeralDir, ".") {
		ephemeralDir = filepath.Join(configDir, c.EphemeralDir)
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

	return &blobcache.Params{
		PrivateKey: privKey.(p2p.PrivateKey),

		Ephemeral:  bcstate.NewBoltDB(ephemeralDB, uint64(ephemeralCap)),
		Persistent: bcstate.NewBoltDB(persistDB, uint64(persistCap)),
	}, nil
}

func setupSwarm(privKey p2p.PrivateKey, inet256APIAddr string) (peerswarm.AskSwarm, error) {
	sw, err := inet256p2p.NewSwarm(inet256APIAddr, privKey)
	if err != nil {
		return nil, err
	}
	return sw, nil
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
