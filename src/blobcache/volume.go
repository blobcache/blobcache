package blobcache

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/netip"
	"strings"

	"go.inet256.org/inet256/src/inet256"
)

// Endpoint is somewhere that a blobcache node can be found.
// The Zero endpoint means the node is not available on the network.
type Endpoint struct {
	Peer   PeerID         `json:"peer"`
	IPPort netip.AddrPort `json:"ip_port"`
}

func (e Endpoint) String() string {
	return fmt.Sprintf("%v@%v", e.Peer, e.IPPort)
}

func (e Endpoint) IsZero() bool {
	return e.Peer.IsZero() && e.IPPort == netip.AddrPort{}
}

func ParseEndpoint(s string) (Endpoint, error) {
	parts := strings.Split(s, "@")
	if len(parts) != 2 {
		return Endpoint{}, fmt.Errorf("invalid endpoint: %s", s)
	}
	peer, err := inet256.ParseAddrBase64([]byte(parts[0]))
	if err != nil {
		return Endpoint{}, err
	}
	ap, err := netip.ParseAddrPort(parts[1])
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{
		Peer:   peer,
		IPPort: ap,
	}, nil
}

// VolumeSpec is a specification for a volume.
type VolumeSpec = VolumeBackend[Handle]

// VolumeInfo is a volume info.
type VolumeInfo struct {
	// ID is always the local OID for the volume.
	ID OID `json:"id"`
	VolumeConfig
	Backend VolumeBackend[OID] `json:"backend"`
}

func (vi VolumeInfo) Marshal(out []byte) []byte {
	data, err := json.Marshal(vi)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (vi *VolumeInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, vi)
}

func (vi *VolumeInfo) GetRemoteFQOID() FQOID {
	if vi.Backend.Remote == nil {
		return FQOID{}
	}
	return FQOID{
		Peer: vi.Backend.Remote.Endpoint.Peer,
		OID:  vi.Backend.Remote.Volume,
	}
}

// VolumeBackend is a specification for a volume backend.
// If it is going into the API, the it will be a VolumeBackend[Handle].
// If it is coming out of the API, the it will be a VolumeBackend[OID].
type VolumeBackend[T handleOrOID] struct {
	Local     *VolumeBackend_Local     `json:"local,omitempty"`
	Remote    *VolumeBackend_Remote    `json:"remote,omitempty"`
	Git       *VolumeBackend_Git       `json:"git,omitempty"`
	Vault     *VolumeBackend_Vault[T]  `json:"vault,omitempty"`
	Consensus *VolumeBackend_Consensus `json:"consensus,omitempty"`
}

func (v *VolumeBackend[T]) Marshal(out []byte) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (v *VolumeBackend[T]) Unmarshal(data []byte) error {
	return json.Unmarshal(data, v)
}

// Deps returns the volumes which must exist before this volume can be created.
func (v *VolumeBackend[T]) Deps() iter.Seq[T] {
	switch {
	case v.Vault != nil:
		return unitIter[T](v.Vault.X)
	default:
		return emptyIter[T]()
	}
}

func (v VolumeBackend[T]) Params() VolumeConfig {
	switch {
	case v.Local != nil:
		return v.Local.VolumeConfig
	case v.Git != nil:
		return v.Git.VolumeConfig
	default:
		panic(v)
	}
}

func (v VolumeBackend[T]) String() string {
	sb := strings.Builder{}
	sb.WriteString("VolumeBackend{")
	if v.Local != nil {
		sb.WriteString("local")
	}
	if v.Remote != nil {
		sb.WriteString("remote:")
		sb.WriteString(v.Remote.Endpoint.String())
		sb.WriteString(" ")
		sb.WriteString(v.Remote.Volume.String())
	}
	if v.Git != nil {
		sb.WriteString("git")
	}
	if v.Vault != nil {
		sb.WriteString("vault:")
		sb.WriteString(fmt.Sprintf("%v", v.Vault.X))
	}
	sb.WriteString("}")
	return sb.String()
}

func (v *VolumeBackend[T]) Validate() (err error) {
	var count int
	if v.Local != nil {
		if err := v.Local.Validate(); err != nil {
			return err
		}
		count++
	}
	if v.Remote != nil {
		count++
	}
	if v.Git != nil {
		count++
	}
	if v.Vault != nil {
		if err := v.Vault.Validate(); err != nil {
			return err
		}
		count++
	}

	switch count {
	case 0:
		err = errors.Join(err, fmt.Errorf("no volume backend specified"))
	case 1:
	default:
		err = errors.Join(err, fmt.Errorf("only one volume backend can be specified"))
	}
	return err
}

// VolumeBackendToOID converts a VolumeBackend[Handle] to a VolumeBackend[OID].
// It is used to convert the volume backend to the OID format when it is returned from the API.
func VolumeBackendToOID(x VolumeBackend[Handle]) (ret VolumeBackend[OID]) {
	ret = VolumeBackend[OID]{
		Local:  x.Local,
		Remote: x.Remote,
		Git:    x.Git,
	}
	if x.Vault != nil {
		ret.Vault = &VolumeBackend_Vault[OID]{
			X:        x.Vault.X.OID,
			HashAlgo: x.Vault.HashAlgo,
			Secret:   x.Vault.Secret,
		}
	}
	return ret
}

// VolumeConfig contains parameters common to all Volumes.
// Not every volume backend allows them to be specified, but all Volumes have these Values set.
// e.g. the remote volume does not allow a max size to be specified, that's dictated by the remote node.
// However, the volume still has an effective max size, which is available if the volume has been mounted on the local Node.
type VolumeConfig struct {
	Schema   SchemaSpec `json:"schema"`
	HashAlgo HashAlgo   `json:"hash_algo"`
	MaxSize  int64      `json:"max_size"`
	Salted   bool       `json:"salted"`
}

func (v *VolumeConfig) Validate() error {
	if err := v.HashAlgo.Validate(); err != nil {
		return err
	}
	if v.MaxSize <= 0 {
		return fmt.Errorf("max size must be positive")
	}
	return nil
}

func DefaultVolumeParams() VolumeConfig {
	return VolumeConfig{
		Schema:   SchemaSpec{Name: Schema_NONE},
		HashAlgo: HashAlgo_BLAKE3_256,
		MaxSize:  1 << 22,
		Salted:   false,
	}
}

type VolumeBackend_Local struct {
	VolumeConfig
}

func (v *VolumeBackend_Local) Validate() error {
	if err := v.VolumeConfig.Validate(); err != nil {
		return err
	}
	return nil
}

type VolumeBackend_Remote struct {
	Endpoint Endpoint `json:"endpoint"`
	Volume   OID      `json:"volume"`
	HashAlgo HashAlgo `json:"hash_algo"`
}

type VolumeBackend_Git struct {
	URL string `json:"url"`

	VolumeConfig
}

type VolumeBackend_Vault[T handleOrOID] struct {
	X        T        `json:"x"`
	Secret   Secret   `json:"secret"`
	HashAlgo HashAlgo `json:"hash_algo"`
}

func (v *VolumeBackend_Vault[T]) Validate() error {
	return v.HashAlgo.Validate()
}

type Secret [32]byte

func (s Secret) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(s[:]))
}

func (s *Secret) UnmarshalJSON(data []byte) error {
	var hexString string
	if err := json.Unmarshal(data, &hexString); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(hexString)
	if err != nil {
		return err
	}
	copy(s[:], decoded)
	return nil
}

type VolumeBackend_Consensus struct {
	Schema   SchemaSpec `json:"schema"`
	HashAlgo HashAlgo   `json:"hash_algo"`
	MaxSize  int64      `json:"max_size"`
}

type handleOrOID interface {
	Handle | OID
}

// DefaultLocalSpec provides sensible defaults for a local volume.
func DefaultLocalSpec() VolumeSpec {
	return VolumeSpec{
		Local: &VolumeBackend_Local{
			VolumeConfig: DefaultVolumeParams(),
		},
	}
}

func emptyIter[T any]() iter.Seq[T] {
	return func(yield func(T) bool) {}
}

func unitIter[T any](x T) iter.Seq[T] {
	return func(yield func(T) bool) {
		yield(x)
	}
}

// DEK is a data encryption key.
type DEK [32]byte

func (d DEK) MarshalText() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(d[:]))
}

func (d *DEK) UnmarshalText(data []byte) error {
	var hexString string
	if err := json.Unmarshal(data, &hexString); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(hexString)
	if err != nil {
		return err
	}
	copy(d[:], decoded)
	return nil
}

func (d DEK) String() string {
	d2, err := d.MarshalText()
	if err != nil {
		panic(err)
	}
	return string(d2)
}
