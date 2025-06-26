package blobcachecmd

import (
	"encoding/json"
	"fmt"

	"blobcache.io/glfs"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/star"

	"blobcache.io/blobcache/src/blobcache"
)

var glfsCmd = star.NewDir(star.Metadata{
	Short: "Git Like Filesystem",
}, map[star.Symbol]star.Command{
	"init": glfsInitCmd,
	"look": glfsLookCmd,
})

var glfsInitCmd = star.Command{
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{volumeNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, err := openLocal(c)
		if err != nil {
			return err
		}
		volh, err := svc.OpenAt(c, blobcache.RootHandle(), volumeNameParam.Load(c))
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
		ag := glfs.NewAgent()
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
	Pos:   []star.AnyParam{volumeNameParam},
	F: func(c star.Context) error {
		ctx := c.Context
		svc, err := openLocal(c)
		if err != nil {
			return err
		}
		volh, err := svc.OpenAt(c, blobcache.RootHandle(), volumeNameParam.Load(c))
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
		var ref glfs.Ref
		if err := json.Unmarshal(root, &ref); err != nil {
			return err
		}
		glfsAg := glfs.NewAgent()
		if ref.Type == glfs.TypeTree {
			tr, err := glfsAg.NewTreeReader(tx, ref)
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

var volumeNameParam = star.Param[string]{
	Name:  "volume",
	Parse: star.ParseString,
}
