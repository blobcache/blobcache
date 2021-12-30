package blobcachecmd

import (
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/blobcache/blobcache/pkg/bcdb"
	"github.com/blobcache/blobcache/pkg/blobcache"
	"github.com/blobcache/blobcache/pkg/stores"
)

const DefaultAPIAddr = "127.0.0.1:6025"

type Config struct {
	PrivateKeyPath string     `yaml:"private_key_path"`
	DataDir        string     `yaml:"data_dir"`
	APIAddr        string     `yaml:"api_addr"`
	Peers          []PeerSpec `yaml:"peers"`
}

type PeerSpec struct {
	ID         inet256.ID `yaml:"id"`
	QuotaCount uint64     `yaml:"quota_count"`
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
	return &Config{
		PrivateKeyPath: "./private_key.pem",
		DataDir:        ".",
		APIAddr:        DefaultAPIAddr,
		Peers:          nil,
	}
}

func buildParams(configPath string, c Config) (*blobcache.Params, error) {
	configDir := filepath.Dir(configPath)

	var dataDir string
	if strings.HasPrefix(c.DataDir, ".") {
		dataDir = filepath.Join(configDir, c.DataDir)
	}
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dataDir, "blobcache.db")
	db, err := bcdb.NewBadger(dbPath)
	if err != nil {
		return nil, err
	}
	storePath := filepath.Join(dataDir, "blobs")
	storeFS := posixfs.NewDirFS(storePath)
	store := stores.NewFSStore(storeFS)

	// Private Key
	data, err := ioutil.ReadFile(c.PrivateKeyPath)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block.Type != "PRIVATE KEY" {
		return nil, errors.Errorf("wrong PEM type for private key %s", block.Type)
	}
	privKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	// Peers
	for i, peerSpec := range c.Peers {
		if peerSpec.ID.IsZero() {
			return nil, errors.Errorf("peer # %d cannot have zero id", i)
		}
	}
	return &blobcache.Params{
		DB:         db,
		Store:      store,
		PrivateKey: privKey.(inet256.PrivateKey),
	}, nil
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
