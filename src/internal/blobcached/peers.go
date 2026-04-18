package blobcached

import (
	"context"
	"fmt"
	"iter"
	"net"
	"net/netip"
	"os"
	"strconv"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/groupfile"
)

const (
	peerLocPath    = "PEER_LOC"
	DefaultBCPPort = 6025
)

var _ bclocal.PeerLocator = &PeerLocator{}

type hostPort struct {
	// Host is either a domain name or an IP address as a string.
	Host string
	Port uint16
}

type PeerLocator struct {
	locs map[blobcache.NodeID][]hostPort
}

func (loc *PeerLocator) WhereIs(ctx context.Context, peer blobcache.NodeID) iter.Seq[netip.AddrPort] {
	return func(yield func(netip.AddrPort) bool) {
		for _, hostport := range loc.locs[peer] {
			var ap netip.AddrPort
			if err := func() error {
				ipAddr, err := net.ResolveIPAddr("ip", hostport.Host)
				if err != nil {
					return err
				}
				// TODO: avoid going to string and back
				ap = netip.AddrPortFrom(netip.MustParseAddr(ipAddr.String()), hostport.Port)
				return nil
			}(); err != nil {
				continue
			}
			if !yield(ap) {
				return
			}
		}
	}
}

// PeerEntry is an entry in the Peer locations file.
type PeerEntry = groupfile.Entry[blobcache.NodeID, hostPort]

func ParsePeerLocs(data []byte) ([]PeerEntry, error) {
	ents, err := groupfile.Parse(data, parsePeerID, parseHostPort)
	if err != nil {
		return nil, err
	}
	for _, ent := range ents {
		if mstmt := ent.MStmt; mstmt != nil {
			for _, m := range mstmt.Members {
				if m.GroupRef != nil {
					return nil, fmt.Errorf("%s cannot contain @group references", peerLocPath)
				}
			}
		}
	}
	return ents, nil
}

func parsePeerID(x []byte) (blobcache.NodeID, error) {
	var ret blobcache.NodeID
	err := ret.UnmarshalText(x)
	return ret, err
}

func parseHostPort(x []byte) (hostPort, error) {
	host, portStr, err := net.SplitHostPort(string(x))
	if err != nil {
		return hostPort{}, err
	}
	var port uint16 = DefaultBCPPort
	if portStr != "" {
		portInt, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return hostPort{}, err
		}
		port = uint16(portInt)
	}
	return hostPort{Host: host, Port: port}, nil
}

// LoadLocator creates a new locator using the PEER_LOC file at p
func LoadLocator(dir *os.Root, p string) (*PeerLocator, error) {
	data, err := dir.ReadFile(p)
	if err != nil {
		return nil, err
	}
	ents, err := ParsePeerLocs(data)
	if err != nil {
		return nil, err
	}
	locs := make(map[blobcache.NodeID][]hostPort, len(ents))
	for _, ent := range ents {
		if mstmt := ent.MStmt; mstmt != nil {
			for _, m := range mstmt.Members {
				if u := m.Unit; u != nil {
					locs[mstmt.Group] = append(locs[mstmt.Group], *u)
				}
			}
		}
	}
	return &PeerLocator{locs: locs}, nil
}
