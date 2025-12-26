package blobcache

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/netip"
	"slices"
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
	// Node is the node that manages the object.
	Node PeerID
	// IPPort if non-nil is the IP address and UDP port where the Node
	// that manages the object is listening.
	IPPort *netip.AddrPort
	// Base is the OID that the caller has access to by fiat.
	// OpenFiat will be called on Base to get the first handle from the Node.
	Base OID
	// Path is the path of Volume links needed to reach the target object.
	// It can be empty if the object is directly accessible by fiat.
	Path OIDPath
	// Extra is the part of the URL that was not parsed.
	Extra string
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
	for _, oid := range u.Path {
		out = fmt.Appendf(out, ";%s", oid.String())
	}
	return out, nil
}

func (u *URL) UnmarshalText(xData []byte) error {
	var x string
	for _, prefix := range []string{"bc://", "bc::", "blobcache://"} {
		var ok bool
		if x, ok = strings.CutPrefix(string(xData), prefix); ok {
			break
		}
	}
	data := []byte(x)

	var ret URL
	// 1. Read PeerID
	if part, rest, err := readUntilDelim(data, ':'); err != nil {
		return err
	} else {
		peerID, err := inet256.ParseAddrBase64(part)
		if err != nil {
			return err
		}
		ret.Node = peerID
		data = rest
	}
	// 2. Read addr port, if we can find one.
	if addrPort, rest, err := readAddrPort(data); err == nil {
		// we found one, so advance data to rest
		ret.IPPort = addrPort
		data = rest
	}
	// 3. Read everything else as an OIDPath
	if oidp, rest, err := readOIDPath(data); err != nil {
		return err
	} else {
		if len(oidp) == 0 {
			return fmt.Errorf("must include at least 1 object ID")
		}
		ret.Base = oidp[0]
		if len(oidp) > 1 {
			ret.Path = oidp[1:]
		}
		ret.Extra = string(rest)
		data = rest
	}

	*u = ret
	return nil
}

// Endpoint returns the endpoint of the URL.
// If the URL does not contain an IP address and port, then nil is returned.
func (u *URL) Endpoint() *Endpoint {
	if u.IPPort == nil {
		return nil
	}
	return &Endpoint{
		Peer:   u.Node,
		IPPort: *u.IPPort,
	}
}

func (u URL) BaseFQOID() FQOID {
	return FQOID{
		Peer: u.Node,
		OID:  u.Base,
	}
}

func (u URL) TargetFQOID() FQOID {
	return FQOID{
		Peer: u.Node,
		OID:  u.Target(),
	}
}

// Target returns the target of the URL.
func (u URL) Target() OID {
	if len(u.Path) == 0 {
		return u.Base
	}
	return u.Path[len(u.Path)-1]
}

func readUntilDelim(x []byte, delim byte) ([]byte, []byte, error) {
	i := bytes.Index(x, []byte{delim})
	if i == -1 {
		return x, nil, nil
	}
	return x[:i], x[i+1:], nil
}

func readAddrPort(x []byte) (*netip.AddrPort, []byte, error) {
	var pos int
	for {
		i := slices.Index(x[pos+1:], ':')
		if i == -1 {
			return nil, x, fmt.Errorf("could not parse address port")
		} else {
			pos = i + pos + 1
		}
		ap, err := netip.ParseAddrPort(string(x[:pos]))
		if err == nil {
			return &ap, x[pos+1:], nil
		}
	}
}

// readOIDPath reads a semicolon separated list of OID strings.
// If a list element does not contain an OID, then the rest are returned.
func readOIDPath(x []byte) (ret OIDPath, rest []byte, err error) {
	parts := bytes.Split(x, []byte(";"))
	var success int
	for i := range parts {
		if len(parts[i]) != hex.EncodedLen(OIDSize) {
			break
		}
		oid, err := ParseOID(string(parts[i]))
		if err != nil {
			break
		}
		success = i
		ret = append(ret, oid)
	}
	return ret, bytes.Join(parts[success+1:], []byte(";")), nil
}
