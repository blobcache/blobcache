package blobcached

import (
	"fmt"
	"net/netip"
	"os"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/groupfile"
)

const peerLocPath = "PEER_LOC"

var _ bclocal.PeerLocator = &Locator{}

type Locator struct {
	locs map[blobcache.PeerID][]netip.AddrPort
}

func (loc *Locator) WhereIs(peer blobcache.PeerID) []netip.AddrPort {
	return loc.locs[peer]
}

func ParsePeerLocs(data []byte) ([]groupfile.Entry[blobcache.PeerID, netip.AddrPort], error) {
	ents, err := groupfile.Parse(data, parsePeerID, parseAddrPort)
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

func parsePeerID(x []byte) (blobcache.PeerID, error) {
	var ret blobcache.PeerID
	err := ret.UnmarshalText(x)
	return ret, err
}

func parseAddrPort(x []byte) (netip.AddrPort, error) {
	return netip.ParseAddrPort(string(x))
}

// LoadLocator creates a new locator using the PEER_LOC file at p
func LoadLocator(dir *os.Root, p string) (*Locator, error) {
	data, err := dir.ReadFile(p)
	if err != nil {
		return nil, err
	}
	ents, err := ParsePeerLocs(data)
	if err != nil {
		return nil, err
	}
	locs := make(map[blobcache.PeerID][]netip.AddrPort, len(ents))
	for _, ent := range ents {
		if mstmt := ent.MStmt; mstmt != nil {
			for _, m := range mstmt.Members {
				if u := m.Unit; u != nil {
					locs[mstmt.Group] = append(locs[mstmt.Group], *u)
				}
			}
		}
	}
	return &Locator{locs: locs}, nil
}
