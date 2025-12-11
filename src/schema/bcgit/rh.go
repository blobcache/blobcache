package bcgit

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"iter"
	"regexp"
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
		Push: rem.Push,
		List: func(ctx context.Context) iter.Seq2[GitRef, error] {
			return func(yield func(GitRef, error) bool) {
				it, err := rem.OpenIterator(ctx)
				if err != nil {
					yield(GitRef{}, err)
					return
				}
				var gr GitRef
				for {
					if err := it.Next(ctx, &gr); err != nil {
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

// Sync copies the transitive closure of the git object `id` from src to dst.
func Sync(ctx context.Context, src bcsdk.RO, dst bcsdk.WO, id blobcache.CID) error {
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
		if err := Sync(ctx, src, dst, child); err != nil {
			return err
		}
	}
	id2, err := dst.Post(ctx, data)
	if id != id2 {
		return fmt.Errorf("destination is not using sha256")
	}
	return err
}

func listChildren(data []byte) (ret []blobcache.CID, _ error) {
	eot := bytes.Index(data, []byte{' '})
	if eot == -1 {
		return nil, fmt.Errorf("could not parse type")
	}
	ty := string(data[:eot])
	content := data[eot+1:]
	switch ty {
	case "commit":
		for _, m := range gitHashRegex.FindAll(content, -1) {
			var cid blobcache.CID
			if _, err := hex.Decode(cid[:], m); err != nil {
				return nil, err
			}
			ret = append(ret, cid)
		}
		return ret, nil
	case "tree":
		for _, m := range treeEntHashRegex.FindAll(content, -1) {
			var cid blobcache.CID
			copy(cid[:], m[len(m)-len(cid):])
			ret = append(ret, cid)
		}
		return ret, nil
	case "blob":
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot parse git object")
	}
}

var gitHashRegex = regexp.MustCompile(`[0-9a-f]{64}`)
var treeEntHashRegex = regexp.MustCompile(`\d+ [\w.]+\x00.{32}`)
