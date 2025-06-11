package blobcache

import (
	"errors"
	"fmt"
	"net/netip"

	"go.inet256.org/inet256/pkg/inet256"
)

type PeerID inet256.ID

type Addr struct {
	Peer     PeerID         `json:"peer"`
	AddrPort netip.AddrPort `json:"addrport"`
}

type VolumeSpec struct {
	HashAlgo HashAlgo      `json:"hash_algo"`
	Backend  VolumeBackend `json:"backend"`
}

type VolumeInfo struct {
	ID       OID           `json:"id"`
	HashAlgo HashAlgo      `json:"hash_algo"`
	Backend  VolumeBackend `json:"backend"`
}

type VolumeBackend struct {
	Local  *VolumeBackend_Local  `json:"local,omitempty"`
	Remote *VolumeBackend_Remote `json:"remote,omitempty"`
	Git    *VolumeBackend_Git    `json:"git,omitempty"`
}

func (v *VolumeBackend) Validate() (err error) {
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

type VolumeBackend_Local struct{}

type VolumeBackend_Remote struct {
	Addr Addr `json:"addr"`
	ID   OID  `json:"id"`
}

type VolumeBackend_Git struct {
	URL string `json:"url"`
}
