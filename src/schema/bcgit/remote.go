package bcgit

import (
	"context"
	"crypto/sha256"
	"iter"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/tries"
	"blobcache.io/blobcache/src/schema/bcgit/gitrh"
	"go.brendoncarroll.net/exp/streams"
)

func Hash(x []byte) blobcache.CID {
	return blobcache.HashAlgo_SHA2_256.HashFunc()(nil, x)
}

type GitRef = gitrh.Ref

// FmtURL formats a URL
func FmtURL(u blobcache.URL) string {
	return "bc::" + strings.TrimPrefix(u.String(), "bc://")
}

func NewRemoteHelper(bc blobcache.Service, u blobcache.URL) gitrh.Server {
	return gitrh.Server{
		// list
		// push
		// fetch
		Capabilities: []string{"list"},

		List: func() iter.Seq[GitRef] {
			return func(yield func(GitRef) bool) {
				yield(GitRef{Name: "refs/heads/999", Target: sha256.Sum256([]byte("asdfasdflkasjd;flkasjdf;"))})
				yield(GitRef{Name: "refs/heads/888"})
				yield(GitRef{Name: "refs/heads/777"})
				yield(GitRef{Name: "refs/heads/666"})
			}
		},
	}
}

// Remote is a Git Remote backed by a Blobcache volume
type Remote struct {
	svc  blobcache.Service
	volh blobcache.Handle

	tmach *tries.Machine
}

func NewRemote(svc blobcache.Service, volh blobcache.Handle) Remote {
	return Remote{
		svc:  svc,
		volh: volh,

		tmach: tries.NewMachine(nil, blobcache.HashAlgo_SHA2_256.HashFunc()),
	}
}

func (gr *Remote) Iterate(ctx context.Context) streams.Iterator[GitRef] {
	return streams.NewSlice([]GitRef{
		{Name: "refs/heads/asbasd"},
		{Name: "refs/heads/aasdfas"},
		{Name: "refs/heads/iuoi"},
	}, nil)
}

func (gr *Remote) Push(ctx context.Context, refs iter.Seq[GitRef]) error {
	return nil
}
