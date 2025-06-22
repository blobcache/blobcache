package blobcache

import (
	"errors"
	"fmt"
	"net/netip"

	"go.inet256.org/inet256/pkg/inet256"
)

// PeerID uniquely identifies a peer by hash of the public key.
type PeerID = inet256.ID

// Endpoint is somewhere that a blobcache node can be found.
// The Zero endpoint means the node is not available on the network.
type Endpoint struct {
	Peer   PeerID         `json:"peer"`
	IPPort netip.AddrPort `json:"ip_port"`
}

func (e Endpoint) IsZero() bool {
	return e.Peer.IsZero() && e.IPPort == netip.AddrPort{}
}

// VolumeSpec is a specification for a volume.
type VolumeSpec struct {
	// HashAlgo is the hash algorithm to use for the volume.
	HashAlgo HashAlgo `json:"hash_algo"`
	// MaxSize is the maximum size of a blob
	MaxSize int64 `json:"max_size"`
	Salted  bool  `json:"salted"`
	// Backend is the implementation to use for the volume.
	Backend VolumeBackend[Handle] `json:"backend"`
}

func (v *VolumeSpec) Validate() (retErr error) {
	if err := v.Backend.Validate(); err != nil {
		retErr = errors.Join(retErr, err)
	}
	if err := v.HashAlgo.Validate(); err != nil {
		retErr = errors.Join(retErr, err)
	}
	if v.MaxSize <= 0 {
		retErr = errors.Join(retErr, fmt.Errorf("max size must be positive"))
	}
	return retErr
}

// VolumeInfo is a volume info.
type VolumeInfo struct {
	ID       OID                `json:"id"`
	HashAlgo HashAlgo           `json:"hash_algo"`
	MaxSize  int64              `json:"max_size"`
	Backend  VolumeBackend[OID] `json:"backend"`
}

// VolumeBackend is a specification for a volume backend.
// If it is going into the API, the it will be a VolumeBackend[Handle].
// If it is coming out of the API, the it will be a VolumeBackend[OID].
type VolumeBackend[T handleOrOID] struct {
	Local    *VolumeBackend_Local       `json:"local,omitempty"`
	Remote   *VolumeBackend_Remote      `json:"remote,omitempty"`
	Git      *VolumeBackend_Git         `json:"git,omitempty"`
	RootAEAD *VolumeBackend_RootAEAD[T] `json:"root_aead,omitempty"`
	Vault    *VolumeBackend_Vault[T]    `json:"vault,omitempty"`
}

func (v *VolumeBackend[T]) Validate() (err error) {
	var count int
	if v.Local != nil {
		count++
	}
	if v.Remote != nil {
		count++
	}
	if v.Git != nil {
		count++
	}
	if v.RootAEAD != nil {
		err = errors.Join(err, v.RootAEAD.Validate())
		count++
	}
	if v.Vault != nil {
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
			Inner:  x.Vault.Inner.OID,
			Secret: x.Vault.Secret,
		}
	}
	return ret
}

type VolumeBackend_Local struct{}

type VolumeBackend_Remote struct {
	Endpoint Endpoint `json:"endpoint"`
	Name     string   `json:"name"`
}

type VolumeBackend_Git struct {
	URL string `json:"url"`
}

// VolumeBackend_RootAEAD is a volume backend that uses a root AEAD to encrypt the volume's root.
// The volume's blobs are not encrypted.  The inner volume will have the same blobs as this volume,
// they will have different roots.
type VolumeBackend_RootAEAD[T handleOrOID] struct {
	Inner  T        `json:"inner"`
	Algo   AEADAlgo `json:"algo"`
	Secret [32]byte `json:"secret"`
}

func (v *VolumeBackend_RootAEAD[T]) Validate() error {
	if err := v.Algo.Validate(); err != nil {
		return err
	}
	return nil
}

type VolumeBackend_Vault[T handleOrOID] struct {
	Inner  T        `json:"inner"`
	Secret [32]byte `json:"secret"`
}

type handleOrOID interface {
	Handle | OID
}

// DefaultLocalSpec provides sensible defaults for a local volume.
func DefaultLocalSpec() VolumeSpec {
	return VolumeSpec{
		HashAlgo: HashAlgo_BLAKE3_256,
		MaxSize:  1 << 21,
		Backend: VolumeBackend[Handle]{
			Local: &VolumeBackend_Local{},
		},
	}
}

type AEADAlgo string

const (
	AEAD_CHACHA20POLY1305 AEADAlgo = "chacha20poly1305"
)

func (a AEADAlgo) Validate() error {
	switch a {
	case AEAD_CHACHA20POLY1305:
	default:
		return fmt.Errorf("unknown aead algo: %s", a)
	}
	return nil
}
