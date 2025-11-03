package blobcache

import (
	"fmt"
	"strings"
)

// FQOID is a fully qualified object identifier.
// It uniquely identifies an object anywhere on the Blobcache network.
// Volumes and transactions are both considered Objects.
type FQOID struct {
	Peer PeerID
	OID  OID
}

// URL is an Endpoint plus an OID.
type URL struct {
	Endpoint Endpoint
	OID      OID
}

func (u URL) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "bc://%v@%v/%v", u.Endpoint.Peer, u.Endpoint.IPPort, u.OID), nil
}

func (u *URL) UnmarshalText(x []byte) error {
	const prefix = "bc://"
	if !strings.HasPrefix(string(x), prefix) {
		return fmt.Errorf("invalid FQOID: %s", x)
	}
	x = x[len(prefix):]
	parts := strings.Split(string(x), "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid FQOID: %s", x)
	}
	ep, err := ParseEndpoint(parts[0])
	if err != nil {
		return err
	}
	oid, err := ParseOID(parts[1])
	if err != nil {
		return err
	}
	u.Endpoint = ep
	u.OID = oid
	return nil
}
