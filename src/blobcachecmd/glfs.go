package blobcachecmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"blobcache.io/glfs"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/star"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/glfsport"
	"blobcache.io/blobcache/src/schema"
	glfsschema "blobcache.io/blobcache/src/schema/glfs"
)

var glfsCmd = star.NewDir(star.Metadata{
	Short: "Git Like Filesystem",
}, map[string]star.Command{
	"init":   glfsInitCmd,
	"look":   glfsLookCmd,
	"import": glfsImportCmd,
	"read":   glfsReadCmd,
	"sync":   glfsSyncCmd,
})

var glfsInitCmd = star.Command{
	Flags: map[string]star.Flag{
		"nsrh": nsRootH,
		"nsr":  nsRoot,
	},
	Pos: []star.Positional{volNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, err := openService(c)
		if err != nil {
			return err
		}
		volh, err := openAt(c)
		if err != nil {
			return err
		}
		tx, err := bcsdk.BeginTx(c, svc, *volh, blobcache.TxParams{
			Modify: true,
		})
		if err != nil {
			return err
		}
		var root []byte
		if err := tx.Load(ctx, &root); err != nil {
			return err
		}
		if len(root) > 0 {
			return fmt.Errorf("there is already something in this volume")
		}
		ag := glfs.NewMachine()
		ref, err := ag.PostTreeSlice(ctx, tx, nil)
		if err != nil {
			return err
		}
		rootData, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		if err := tx.Save(ctx, rootData); err != nil {
			return err
		}
		return tx.Commit(ctx)
	},
}

var glfsLookCmd = star.Command{
	Flags: map[string]star.Flag{
		"nsrh": nsRootH,
		"nsr":  nsRoot,
	},
	Pos: []star.Positional{volNameParam, srcPathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, err := openService(c)
		if err != nil {
			return err
		}
		volh, err := openAt(c)
		if err != nil {
			return err
		}
		tx, err := bcsdk.BeginTx(c, svc, *volh, blobcache.TxParams{})
		if err != nil {
			return err
		}
		defer tx.Abort(ctx)
		var root []byte
		if err := tx.Load(ctx, &root); err != nil {
			return err
		}
		var ref glfs.Ref
		if err := json.Unmarshal(root, &ref); err != nil {
			return err
		}
		glfsAg := glfs.NewMachine()
		ref2, err := glfsAg.GetAtPath(ctx, tx, ref, srcPathParam.Load(c))
		if err != nil {
			return err
		}
		if ref2.Type == glfs.TypeTree {
			tr, err := glfsAg.NewTreeReader(tx, *ref2)
			if err != nil {
				return err
			}
			if err := streams.ForEach(ctx, tr, func(entry glfs.TreeEntry) error {
				c.Printf("%s %v %v\n", entry.Name, entry.FileMode, entry.Ref)
				return nil
			}); err != nil {
				return err
			}
		}
		c.Printf("%v\n", ref)
		return nil
	},
}

var glfsImportCmd = star.Command{
	Metadata: star.Metadata{
		Short: "import data from the local filesystem into a GLFS volume",
	},
	Flags: map[string]star.Flag{
		"nsrh": nsRootH,
		"nsr":  nsRoot,
	},
	Pos: []star.Positional{volNameParam, dstPathParam, srcPathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, err := openService(c)
		if err != nil {
			return err
		}
		volh, err := openAt(c)
		if err != nil {
			return err
		}
		return modifyGLFS(ctx, svc, *volh, func(ag *glfs.Machine, dst schema.WO, src schema.RO, root glfs.Ref) (*glfs.Ref, error) {
			imp := glfsport.Importer{
				Store: dst,
				Dir:   srcPathParam.Load(c),
			}
			ref, err := imp.Import(ctx, "")
			if err != nil {
				return nil, err
			}
			names := strings.Split(glfs.CleanPath(dstPathParam.Load(c)), "/")
			slices.Reverse(names)
			for _, name := range names {
				ref, err = glfs.PostTreeSlice(ctx, dst, []glfs.TreeEntry{{
					Name: name,
					Ref:  *ref,
				}})
				if err != nil {
					return nil, err
				}
			}
			return glfs.Merge(ctx, dst, src, root, *ref)
		})
	},
}

var glfsReadCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Read a file from a GLFS volume and write it to stdout",
	},
	Flags: map[string]star.Flag{

		"nsrh": nsRootH,
		"nsr":  nsRoot,
	},
	Pos: []star.Positional{volNameParam, srcPathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, err := openService(c)
		if err != nil {
			return err
		}
		volh, err := openAt(c)
		if err != nil {
			return err
		}
		return viewGLFS(ctx, svc, *volh, func(ag *glfs.Machine, src schema.RO, root glfs.Ref) error {
			ref, err := ag.GetAtPath(ctx, src, root, srcPathParam.Load(c))
			if err != nil {
				return err
			}
			if ref.Type != glfs.TypeBlob {
				return fmt.Errorf("path %s is not a blob", srcPathParam.Load(c))
			}
			br, err := ag.NewBlobReader(ctx, src, *ref)
			if err != nil {
				return err
			}
			_, err = io.Copy(c.StdOut, br)
			return err
		})
	},
}

var glfsSyncCmd = star.Command{
	Metadata: star.Metadata{
		Short: "sync efficiently sets the contents of the dst volume to the content of the src volume",
	},
	Flags: map[string]star.Flag{
		"nsrh": nsRootH,
		"nsr":  nsRoot,
	},
	Pos: []star.Positional{srcVolumeParam, dstVolumeParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, err := openService(c)
		if err != nil {
			return err
		}
		nsc, nsh, err := getNS(c)
		if err != nil {
			return err
		}

		srcVolh, err := nsc.OpenAt(c.Context, *nsh, srcVolumeParam.Load(c), blobcache.Action_ALL)
		if err != nil {
			return fmt.Errorf("failed to open source volume: %w", err)
		}
		dstVolh, err := nsc.OpenAt(c.Context, *nsh, dstVolumeParam.Load(c), blobcache.Action_ALL)
		if err != nil {
			return fmt.Errorf("failed to open destination volume: %w", err)
		}
		return glfsschema.SyncVolume(ctx, svc, *srcVolh, *dstVolh)
	},
}

var dstPathParam = star.Required[string]{
	ID:       "dst",
	ShortDoc: "the destination path",
	Parse:    star.ParseString,
}

var srcPathParam = star.Required[string]{
	ID:       "src",
	ShortDoc: "the source path",
	Parse:    star.ParseString,
}

var volumeNameParam = star.Required[string]{
	ID:       "volume",
	ShortDoc: "the name of a volume in a namespace",
	Parse:    star.ParseString,
}

var srcVolumeParam = star.Required[string]{
	ID:       "src",
	ShortDoc: "the source volume",
	Parse:    star.ParseString,
}

var dstVolumeParam = star.Required[string]{
	ID:       "dst",
	ShortDoc: "the destination volume",
	Parse:    star.ParseString,
}

func viewGLFS(ctx context.Context, s blobcache.Service, volh blobcache.Handle, fn func(ag *glfs.Machine, src schema.RO, root glfs.Ref) error) error {
	tx, err := bcsdk.BeginTx(ctx, s, volh, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	ag := glfs.NewMachine()
	var rootData []byte
	if err := tx.Load(ctx, &rootData); err != nil {
		return err
	}
	root, err := glfsschema.ParseRef(rootData)
	if err != nil {
		return err
	}
	return fn(ag, tx, *root)
}

func modifyGLFS(ctx context.Context, s blobcache.Service, volh blobcache.Handle, f func(ag *glfs.Machine, dst schema.WO, src schema.RO, root glfs.Ref) (*glfs.Ref, error)) error {
	tx, err := bcsdk.BeginTx(ctx, s, volh, blobcache.TxParams{
		Modify: true,
	})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	ag := glfs.NewMachine()
	// load and parse root
	var rootData []byte
	if err := tx.Load(ctx, &rootData); err != nil {
		return err
	}
	root, err := glfsschema.ParseRef(rootData)
	if err != nil {
		return err
	}
	// modify
	root2, err := f(ag, tx, tx, *root)
	if err != nil {
		return err
	}
	// TODO: delete old refs
	if err := tx.Save(ctx, glfsschema.MarshalRef(root2)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
