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
	"go.brendoncarroll.net/state/cadata"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/glfsport"
	"blobcache.io/blobcache/src/internal/simplens"
)

var glfsCmd = star.NewDir(star.Metadata{
	Short: "Git Like Filesystem",
}, map[star.Symbol]star.Command{
	"init":   glfsInitCmd,
	"look":   glfsLookCmd,
	"import": glfsImportCmd,
	"getf":   glfsGetFileCmd,
})

var glfsInitCmd = star.Command{
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{volumeNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, close, err := openLocal(c)
		if err != nil {
			return err
		}
		defer close()
		nsc := simplens.Client{Service: svc}
		volh, err := nsc.OpenAt(c.Context, blobcache.RootHandle(), volumeNameParam.Load(c), blobcache.Action_ALL)
		if err != nil {
			return err
		}
		tx, err := blobcache.BeginTx(c, svc, *volh, blobcache.TxParams{
			Mutate: true,
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
		return tx.Commit(ctx, rootData)
	},
}

var glfsLookCmd = star.Command{
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{volumeNameParam, srcPathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, close, err := openLocal(c)
		if err != nil {
			return err
		}
		defer close()
		nsc := simplens.Client{Service: svc}
		volh, err := nsc.OpenAt(c.Context, blobcache.RootHandle(), volumeNameParam.Load(c), blobcache.Action_ALL)
		if err != nil {
			return err
		}
		tx, err := blobcache.BeginTx(c, svc, *volh, blobcache.TxParams{})
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
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{volumeNameParam, dstPathParam, srcPathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, close, err := openLocal(c)
		if err != nil {
			return err
		}
		defer close()
		nsc := simplens.Client{Service: svc}
		volh, err := nsc.OpenAt(c.Context, blobcache.RootHandle(), volumeNameParam.Load(c), blobcache.Action_ALL)
		if err != nil {
			return err
		}
		return modifyGLFS(ctx, svc, *volh, func(ag *glfs.Machine, dst cadata.PostExister, src cadata.Getter, root glfs.Ref) (*glfs.Ref, error) {
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

var glfsGetFileCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Get a file from a GLFS volume and write it to stdout",
	},
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{volumeNameParam, srcPathParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, close, err := openLocal(c)
		if err != nil {
			return err
		}
		defer close()
		nsc := simplens.Client{Service: svc}
		volh, err := nsc.OpenAt(c, blobcache.RootHandle(), volumeNameParam.Load(c), blobcache.Action_ALL)
		if err != nil {
			return err
		}
		return viewGLFS(ctx, svc, *volh, func(ag *glfs.Machine, dst cadata.PostExister, src cadata.Getter, root glfs.Ref) error {
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

var dstPathParam = star.Param[string]{
	Name:    "dst",
	Default: star.Ptr(""),
	Parse:   star.ParseString,
}

var srcPathParam = star.Param[string]{
	Name:  "src",
	Parse: star.ParseString,
}

var volumeNameParam = star.Param[string]{
	Name:  "volume",
	Parse: star.ParseString,
}

func viewGLFS(ctx context.Context, s blobcache.Service, volh blobcache.Handle, fn func(ag *glfs.Machine, dst cadata.PostExister, src cadata.Getter, root glfs.Ref) error) error {
	tx, err := blobcache.BeginTx(ctx, s, volh, blobcache.TxParams{})
	if err != nil {
		return err
	}
	defer tx.Abort(ctx)
	ag := glfs.NewMachine()
	var rootData []byte
	if err := tx.Load(ctx, &rootData); err != nil {
		return err
	}
	var root glfs.Ref
	if err := json.Unmarshal(rootData, &root); err != nil {
		return err
	}
	return fn(ag, tx, tx, root)
}

func modifyGLFS(ctx context.Context, s blobcache.Service, volh blobcache.Handle, f func(ag *glfs.Machine, dst cadata.PostExister, src cadata.Getter, root glfs.Ref) (*glfs.Ref, error)) error {
	tx, err := blobcache.BeginTx(ctx, s, volh, blobcache.TxParams{
		Mutate: true,
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
	var root glfs.Ref
	if err := json.Unmarshal(rootData, &root); err != nil {
		return fmt.Errorf("failed to parse GLFS root: %w", err)
	}
	// modify
	root2, err := f(ag, tx, tx, root)
	if err != nil {
		return err
	}
	// TODO: delete old refs
	rootData, err = json.Marshal(root2)
	if err != nil {
		return err
	}
	return tx.Commit(ctx, rootData)
}
