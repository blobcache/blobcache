package bcgit

import (
	"context"
	"encoding/hex"
	"fmt"
	"iter"
	"log"
	"regexp"
	"strconv"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/bcgit/gitrh"
	"go.brendoncarroll.net/exp/streams"
)

// FmtURL formats a URL
func FmtURL(u blobcache.URL) string {
	return "bc::" + strings.TrimPrefix(u.String(), "bc://")
}

func NewRemoteHelper(rem *Remote) gitrh.Server {
	return gitrh.Server{
		Push: func(ctx context.Context, s *gitrh.Store, refs []gitrh.Ref) error {
			return rem.Push(ctx, s, refs)
		},
		Fetch: func(ctx context.Context, s *gitrh.Store, refs map[string]blobcache.CID, dst map[string]blobcache.CID) error {
			return rem.Fetch(ctx, s, refs, dst)
		},
		List: func(ctx context.Context) iter.Seq2[GitRef, error] {
			return func(yield func(GitRef, error) bool) {
				it, err := rem.OpenIterator(ctx)
				if err != nil {
					yield(GitRef{}, err)
					return
				}
				defer it.Close()
				var gr GitRef
				for {
					if err := streams.NextUnit(ctx, it, &gr); err != nil {
						if !streams.IsEOS(err) {
							yield(GitRef{}, err)
						}
						return
					}
					if !yield(gr, nil) {
						return
					}
				}
			}
		},
	}
}

func OpenRemoteHelper(ctx context.Context, bc blobcache.Service, u blobcache.URL) (*Remote, error) {
	volh, err := bcsdk.OpenURL(ctx, bc, u)
	if err != nil {
		return nil, err
	}
	return NewRemote(bc, *volh), nil
}

// SyncGit copies the transitive closure of the git object `id` from src to dst.
func SyncGit(ctx context.Context, src bcsdk.RO, dst bcsdk.WO, id blobcache.CID) error {
	if ok, err := schema.ExistsUnit(ctx, dst, id); err != nil {
		return err
	} else if ok {
		return nil
	}
	buf := make([]byte, dst.MaxSize())
	n, err := src.Get(ctx, id, buf)
	if err != nil {
		return err
	}
	data := buf[:n]
	children, err := listChildren(data)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := SyncGit(ctx, src, dst, child); err != nil {
			return err
		}
	}
	id2, err := dst.Post(ctx, data)
	if err != nil {
		return err
	}
	if id != id2 {
		return fmt.Errorf("destination is not using sha256 %v vs. %v", id, id2)
	}
	return err
}

func listChildren(data []byte) (ret []blobcache.CID, _ error) {
	hdr, content, err := gitrh.SplitHeader(data)
	if err != nil {
		return nil, err
	}
	switch hdr.Type {
	case "commit":
		for _, m := range gitHashRegex.FindAll(content, -1) {
			var cid blobcache.CID
			if n, err := hex.Decode(cid[:], m); err != nil {
				return nil, err
			} else if n != 32 {
				return nil, fmt.Errorf("commit contains hash of the wrong length %d", n)
			}
			ret = append(ret, cid)
		}
	case "tree":
		for len(content) > 0 {
			te, rest, err := readTreeEntry(content)
			if err != nil {
				return nil, err
			}
			content = rest
			ret = append(ret, te.Target)
		}
		if len(ret) == -1 && hdr.Len != 0 {
			log.Printf("tree len=%d data=%q", len(content), content)
			panic("tree was parsed incorrectly.  This is a bug.")
		}
	case "blob":
	default:
		return nil, fmt.Errorf("cannot parse git object")
	}
	return ret, nil
}

var gitHashRegex = regexp.MustCompile(`[0-9a-f]{64}`)

type treeEnt struct {
	Name   string
	Mode   uint
	Target blobcache.CID
}

func readTreeEntry(data []byte) (treeEnt, []byte, error) {
	// read mode bits
	mode, data, err := readOctal(data)
	if err != nil {
		return treeEnt{}, nil, err
	}
	// read whitespace
	if data[0] != ' ' {
		return treeEnt{}, nil, fmt.Errorf("expected whitespace after filemode")
	}
	data = data[1:]
	// read name
	name, data, err := readNullTerminated(data)
	if err != nil {
		return treeEnt{}, nil, err
	}
	// read object hash
	var cid blobcache.CID
	if len(data) < len(cid) {
		return treeEnt{}, nil, fmt.Errorf("too short to contain CID")
	}
	copy(cid[:], data)
	data = data[blobcache.CIDSize:]

	return treeEnt{
		Name:   name,
		Mode:   uint(mode),
		Target: cid,
	}, data, nil
}

func readOctal(data []byte) (uint64, []byte, error) {
	for i := range data {
		if !isOctal(data[i]) {
			n, err := strconv.ParseUint(string(data[:i]), 16, 64)
			if err != nil {
				return 0, nil, err
			}
			return n, data[i:], nil
		}
	}
	// the whole thing is octal (unlikely)
	n, err := strconv.ParseUint(string(data), 16, 64)
	return n, nil, err
}

func readNullTerminated(data []byte) (string, []byte, error) {
	for i := range data {
		if data[i] == '\x00' {
			return string(data[:i]), data[i+1:], nil
		}
	}
	return "", nil, fmt.Errorf("never found a null byte")
}

func isOctal(x byte) bool {
	return x >= '0' && x < '8'
}
