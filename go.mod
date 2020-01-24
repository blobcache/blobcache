module github.com/brendoncarroll/blobcache

replace github.com/brendoncarroll/go-p2p => ../go-p2p

require (
	github.com/Yawning/chacha20 v0.0.0-20170904085104-e3b1f968fc63
	github.com/boltdb/bolt v1.3.1
	github.com/brendoncarroll/go-p2p v0.0.0
	github.com/brianolson/cbor_go v1.0.0
	github.com/dustin/go-humanize v1.0.0
	github.com/etcd-io/bbolt v1.3.3
	github.com/go-chi/chi v4.0.3+incompatible
	github.com/golang/protobuf v1.3.0
	github.com/hashicorp/golang-lru v0.5.3
	github.com/lestrrat-go/jwx v0.9.0
	github.com/lucas-clemente/quic-go v0.12.1
	github.com/multiformats/go-multihash v0.0.8
	github.com/pion/dtls v1.5.2
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.4.0
	go.etcd.io/bbolt v1.3.3
	golang.org/x/crypto v0.0.0-20191111213947-16651526fdb4
	gopkg.in/yaml.v2 v2.2.2
	gopkg.in/yaml.v3 v3.0.0-20191026110619-0b21df46bc1d
)

go 1.13
