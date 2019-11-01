package p2p

import (
	"encoding/hex"
	"fmt"
)

type Edge struct {
	PeerID     PeerID
	Transport  string
	LocalAddr  string
	RemoteAddr string
}

func (e Edge) IsSatisfiedBy(f Edge) bool {
	m := (e.PeerID == f.PeerID) &&
		(e.LocalAddr == "" || e.LocalAddr == f.LocalAddr) &&
		(e.RemoteAddr == "" || e.RemoteAddr == f.RemoteAddr)
	return m
}

func (e Edge) String() string {
	fp := hex.EncodeToString(e.PeerID[:8])
	return fmt.Sprintf("{%s @ %s: %s -> %s}", fp, e.Transport, e.LocalAddr, e.RemoteAddr)
}
