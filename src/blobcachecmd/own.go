package blobcachecmd

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/blobcached"
	"go.brendoncarroll.net/star"
)

var ownCmd = star.Command{
	Metadata: star.Metadata{
		Short: "grant full access on the root volume to a peer",
	},
	Pos: []star.Positional{peerParam},
	Flags: map[string]star.Flag{
		"state": stateDirParam,
	},
	F: func(c star.Context) error {
		stateDir := stateDirParam.Load(c)
		peerID := peerParam.Load(c)

		d := blobcached.Daemon{StateDir: stateDir}

		// Ensure policy files exist with defaults
		if err := d.EnsurePolicyFiles(); err != nil {
			return err
		}

		// Add the peer to the "admin" identity group
		if err := d.AddPeerToAdmin(peerID); err != nil {
			return err
		}

		// Get this node's PeerID from the private key
		nodePeerID, err := d.GetPeerID()
		if err != nil {
			return err
		}

		rootOID := blobcache.OID{} // zero OID = root volume
		abbrevNodeID := hex.EncodeToString(nodePeerID[:4])

		// Detect local IP address
		localIP, err := detectLocalIP()
		if err != nil {
			return fmt.Errorf("detecting local IP: %w", err)
		}
		ipPort := netip.AddrPortFrom(localIP, 6025)

		spec := blobcache.VolumeSpec{
			Remote: &blobcache.VolumeBackend_Remote{
				Endpoint: blobcache.Endpoint{
					Peer:   nodePeerID,
					IPPort: ipPort,
				},
				Volume: rootOID,
			},
		}

		c.Printf("%s OWN\n\n", checkmark)
		c.Printf("Peer %s now has full access to this node.\n\n", peerID)
		c.Printf("Run these commands on your local Blobcache Node:\n\n")
		c.Printf("echo '%s' | blobcache ns create remote-%s-root\n", prettyJSON(spec), abbrevNodeID)
		return nil
	},
}

// detectLocalIP returns the first non-loopback IPv4 address found on the host.
func detectLocalIP() (netip.Addr, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return netip.Addr{}, err
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ip4 := ipNet.IP.To4(); ip4 != nil {
				addr, ok := netip.AddrFromSlice(ip4)
				if ok {
					return addr, nil
				}
			}
		}
	}
	return netip.Addr{}, fmt.Errorf("no non-loopback address found")
}

// prettyJSON marshals v to a compact JSON string.
// It panics if marshaling fails, so only call it on known types.
func prettyJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}
