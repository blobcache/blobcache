package blobcache

import (
	"fmt"
	"strings"

	"go.inet256.org/inet256/src/inet256"
)

// FQOID is a fully qualified object identifier.
// It uniquely identifies an object anywhere on the Blobcache network.
type FQOID struct {
	Peer PeerID
	OID  OID
}

func (f FQOID) String() string {
	d, _ := f.MarshalText()
	return string(d)
}

func (f FQOID) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "bc://%v/%v", f.Peer, f.OID), nil
}

func (f *FQOID) UnmarshalText(x []byte) error {
	if !strings.HasPrefix(string(x), "bc://") {
		return fmt.Errorf("invalid FQOID: %s", x)
	}
	x = x[len("bc://"):]
	parts := strings.Split(string(x), "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid FQOID: %s", x)
	}
	peer, err := inet256.ParseAddrBase64([]byte(parts[0]))
	if err != nil {
		return err
	}
	oid, err := ParseOID(parts[1])
	if err != nil {
		return err
	}
	f.Peer = peer
	f.OID = oid
	return nil
}
