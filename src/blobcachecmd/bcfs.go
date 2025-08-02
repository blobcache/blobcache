package blobcachecmd

import (
	"blobcache.io/blobcache/src/bcfs"
	"blobcache.io/blobcache/src/bcfs/scheme_glfs"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.brendoncarroll.net/star"
)

var fuseMountCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Mount a blobcache volume as a FUSE filesystem",
	},
	Flags: []star.AnyParam{stateDirParam},
	Pos: []star.AnyParam{
		volumeNameParam,
		mountpointParam,
	},
	F: func(c star.Context) error {
		svc, close, err := openLocal(c)
		if err != nil {
			return err
		}
		defer close()
		volName := volumeNameParam.Load(c)
		volh, err := svc.OpenAt(c.Context, blobcache.RootHandle(), volName)
		if err != nil {
			return err
		}
		db := dbutil.OpenMemory()
		fsx := bcfs.New(db, svc, *volh, scheme_glfs.NewScheme())
		fuseSrv, err := fs.Mount(mountpointParam.Load(c), fsx.FUSERoot(), &fs.Options{
			MountOptions: fuse.MountOptions{
				Debug: true,
			},
		})
		if err != nil {
			return err
		}
		fuseSrv.Serve()
		return nil
	},
}

var mountpointParam = star.Param[string]{
	Name:  "mp",
	Parse: star.ParseString,
}
