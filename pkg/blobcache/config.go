package blobcache

import (
	"io/ioutil"

	"github.com/brendoncarroll/blobcache/pkg/bridges/fsbridge"
	"github.com/brendoncarroll/blobcache/pkg/p2p"
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
	Peers      []PeerSpec `yaml:"peers`

	Stack []StoreSpec `yaml:"stack"`
}

type PeerSpec struct {
	Edge     p2p.Edge
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
