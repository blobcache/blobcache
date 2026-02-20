package blobcachecmd

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

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
		if err := addPeerToAdmin(stateDir, peerID); err != nil {
			return err
		}

		// Get this node's PeerID from the private key
		nodePeerID, err := d.GetPeerID()
		if err != nil {
			return err
		}

		rootOID := blobcache.OID{} // zero OID = root volume
		abbrevNodeID := hex.EncodeToString(nodePeerID[:4])

		// Build a VolumeSpec JSON for a remote volume pointing to this node's root.
		// The ip_port must be filled in by the user.
		spec := map[string]any{
			"remote": map[string]any{
				"endpoint": map[string]any{
					"peer":    nodePeerID.String(),
					"ip_port": "<IP:PORT>",
				},
				"volume": rootOID.String(),
			},
		}
		var specBuf bytes.Buffer
		enc := json.NewEncoder(&specBuf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(spec); err != nil {
			return err
		}
		specJSON := bytes.TrimSpace(specBuf.Bytes())

		c.Printf("%s OWN\n\n", checkmark)
		c.Printf("Peer %s now has full access to this node.\n\n", peerID)
		c.Printf("Run these commands on your local Blobcache Node:\n\n")
		c.Printf("echo '%s' | blobcache ns create remote-%s-root\n\n", string(specJSON), abbrevNodeID)
		c.Printf("NOTE: Update the ip_port in the JSON above with this node's public address (e.g. \"1.2.3.4:5678\").\n")
		return nil
	},
}

func addPeerToAdmin(stateDir *os.Root, peerID blobcache.PeerID) error {
	// Append a line to the IDENTITIES file adding the peer to the "admin" group.
	f, err := stateDir.OpenFile(blobcached.IdentitiesFilename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("opening %s: %w", blobcached.IdentitiesFilename, err)
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "admin %s\n", peerID.String()); err != nil {
		return fmt.Errorf("writing to %s: %w", blobcached.IdentitiesFilename, err)
	}
	return nil
}
