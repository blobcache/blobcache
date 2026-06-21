package bcns

import (
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"go.inet256.org/inet256/src/inet256"
)

const Sep = '/'

// FQP is a fully qualified path
// FQPs can be written as `{Node};{NSRoot}/{Path}`
// The zero NodeID is used to refer to the local node, and it can be ommitted.
type FQP struct {
	// Node is the node that the root namespace Volume is on.
	// If this is zero, then assume the local node.
	Node blobcache.NodeID
	// Root is the root namespace, it must be accessible by OpenFiat on Node.
	Root blobcache.OID
	// Path is a slash separated path.
	Path string
}

func (fqp FQP) String() string {
	sb := strings.Builder{}
	if !fqp.Node.IsZero() {
		sb.WriteString(fqp.Node.String())
		sb.WriteByte(';')
	}
	if fqp.Root != (blobcache.OID{}) {
		sb.WriteString(fqp.Root.String())
	}
	if fqp.Path != "" {
		sb.WriteByte(Sep)
		sb.WriteString(strings.Trim(fqp.Path, "/"))
	}
	return sb.String()
}

func ParseFQP(x string) (FQP, error) {
	if x == "" {
		// This is the root on the local node
		return FQP{}, nil
	}
	parts := strings.SplitN(x, ";", 2)

	var p string
	oidAndPath := parts[len(parts)-1]
	firstSep := strings.Index(oidAndPath, string(Sep))
	var oid blobcache.OID
	var err error
	if firstSep < 0 {
		oid, err = blobcache.ParseOID(oidAndPath)
		if err != nil {
			return FQP{}, err
		}
	} else {
		oid, err = blobcache.ParseOID(oidAndPath[:firstSep])
		if err != nil {
			return FQP{}, err
		}
		p = strings.Trim(oidAndPath[firstSep:], string(Sep))
	}

	var node blobcache.NodeID
	if len(parts) == 2 {
		node, err = inet256.ParseAddrBase64([]byte(parts[0]))
		if err != nil {
			return FQP{}, err
		}
	}
	return FQP{Node: node, Root: oid, Path: p}, nil
}
