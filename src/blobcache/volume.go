package blobcache

import (
	"errors"
	"fmt"
	"net/netip"

	"go.inet256.org/inet256/pkg/inet256"
)

// PeerID uniquely identifies a peer by hash of the public key.
type PeerID inet256.ID

type Addr struct {
	Peer   PeerID         `json:"peer"`
	IPPort netip.AddrPort `json:"ip_port"`
}

// VolumeSpec is a specification for a volume.
type VolumeSpec struct {
	// HashAlgo is the hash algorithm to use for the volume.
	HashAlgo HashAlgo `json:"hash_algo"`
	// MaxSize is the maximum size of a blob
	MaxSize int64 `json:"max_size"`
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
	Local  *VolumeBackend_Local    `json:"local,omitempty"`
	Remote *VolumeBackend_Remote   `json:"remote,omitempty"`
	Git    *VolumeBackend_Git      `json:"git,omitempty"`
	Vault  *VolumeBackend_Vault[T] `json:"vault,omitempty"`
}

func (v *VolumeBackend[T]) Validate() (err error) {
	var count int
	if v.Local != nil {
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
	Addr Addr `json:"addr"`
	ID   OID  `json:"id"`
}

type VolumeBackend_Git struct {
	URL string `json:"url"`
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
