package blobcachecmd

import (
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcgit"
	"go.brendoncarroll.net/star"
)

var gitCmd = star.NewDir(star.Metadata{
	Short: "Work with git formatted volumes.  Unreleated to the git volume backend",
}, map[string]star.Command{
	"mkremote": gitMkRemote,
})

var gitMkRemote = star.Command{
	Metadata: star.Metadata{Short: "Create a new volume and format and format it as a git remote"},
	F: func(c star.Context) error {
		u := blobcache.URL{}
		c.Printf("%v\n", bcgit.FmtURL(u))
		return nil
	},
}
