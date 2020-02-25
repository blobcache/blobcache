module github.com/brendoncarroll/blobcache

go 1.13

require (
	github.com/brendoncarroll/go-p2p v0.0.0-20200213220407-ad3db897224e
	github.com/go-chi/chi v4.0.3+incompatible
	github.com/golang/protobuf v1.3.2
	github.com/google/martian v2.1.0+incompatible
	github.com/jonboulle/clockwork v0.1.1-0.20190114141812-62fb9bc030d1
	github.com/multiformats/go-multihash v0.0.13
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/zeebo/blake3 v0.0.1
	go.etcd.io/bbolt v1.3.3
	gopkg.in/yaml.v3 v3.0.0-20191026110619-0b21df46bc1d
	gotest.tools v2.2.0+incompatible
)

replace github.com/brendoncarroll/go-p2p => ../go-p2p
