package blobcache

import (
	"fmt"
	"net/netip"
	"strings"

	"go.inet256.org/inet256/src/inet256"
)

// FQOID is a fully qualified object identifier.
// It uniquely identifies an object anywhere on the Blobcache network.
// Volumes and transactions are both considered Objects.
type FQOID struct {
	Peer PeerID
	OID  OID
}

// URL is the location of an Object in the Blobcache Network
type URL struct {
	Node   PeerID
	IPPort *netip.AddrPort
	Base   OID
	Path   []string
}

func ParseURL(x string) (*URL, error) {
	var u URL
	if err := u.UnmarshalText([]byte(x)); err != nil {
		return nil, err
	}
	return &u, nil
}

func (u URL) String() string {
	data, _ := u.MarshalText()
	return string(data)
}

func (u URL) MarshalText() ([]byte, error) {
	var out []byte
	out = fmt.Appendf(out, "bc://%v", u.Node)
	if u.IPPort != nil {
		out = fmt.Appendf(out, ":%v", u.IPPort)
	}
	out = fmt.Appendf(out, ":%v", u.Base)
	return out, nil
}

func (u *URL) UnmarshalText(xData []byte) error {
	x := string(xData)
	for _, prefix := range []string{"bc://", "bc::", "blobcache://"} {
		var ok bool
		if x, ok = strings.CutPrefix(x, prefix); ok {
			break
		}
	}

	parts := strings.Split(string(x), ":")
	switch len(parts) {
	case 2:
		peerID, err := inet256.ParseAddrBase64([]byte(parts[0]))
		if err != nil {
			return err
		}
		oid, err := ParseOID(parts[1])
		if err != nil {
			return err
		}
		u.Node = peerID
		u.IPPort = nil
		u.Base = oid
	case 3:
		peerID, err := inet256.ParseAddrBase64([]byte(parts[0]))
		if err != nil {
			return err
		}
		ap, err := netip.ParseAddrPort(parts[1])
		if err != nil {
			return err
		}
		oid, err := ParseOID(parts[3])
		if err != nil {
			return err
		}
		u.Node = peerID
		u.IPPort = &ap
		u.Base = oid
	default:
		return fmt.Errorf("invalid FQOID: %s", x)
	}

	return nil
}

func (u *URL) Endpoint() *Endpoint {
	if u.IPPort == nil {
		return nil
	}
	return &Endpoint{
		Peer:   u.Node,
		IPPort: *u.IPPort,
	}
}
