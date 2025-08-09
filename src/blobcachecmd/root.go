package blobcachecmd

import (
	"bufio"
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/simplens"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/star"
)

func Main() {
	star.Main(rootCmd)
}

func Root() star.Command {
	return rootCmd
}

var rootCmd = star.NewDir(
	star.Metadata{
		Short: "blobcache is content-addressable storage",
	}, map[star.Symbol]star.Command{
		"daemon":           daemonCmd,
		"daemon-ephemeral": daemonEphemeralCmd,
		"mkvol":            mkVolCmd,
		"ls":               lsCmd,
		"glfs":             glfsCmd,

		"addmem": addMemCmd,
		"rmmem":  rmMemCmd,
		"groups": groupsCmd,

		"grant":  grantCmd,
		"revoke": revokeCmd,

		"fuse-mount": fuseMountCmd,
	},
)

var mkVolCmd = star.Command{
	Metadata: star.Metadata{
		Short: "create a new volume",
	},
	Flags: []star.AnyParam{stateDirParam},
	Pos:   []star.AnyParam{nameParam},
	F: func(c star.Context) error {
		s, close, err := openLocal(c)
		if err != nil {
			return err
		}
		defer close()
		volh, err := s.CreateVolume(c, blobcache.DefaultLocalSpec())
		if err != nil {
			return err
		}
		nsc := simplens.Client{Service: s}
		if err := nsc.PutEntry(c, blobcache.RootHandle(), nameParam.Load(c), *volh); err != nil {
			return err
		}
		c.Printf("Volume successfully created.\n\n")
		c.Printf("HANDLE: %v\n", volh)
		return nil
	},
}

var lsCmd = star.Command{
	Metadata: star.Metadata{
		Short: "lists volumes",
	},
	Flags: []star.AnyParam{stateDirParam},
	F: func(c star.Context) error {
		s, close, err := openLocal(c)
		if err != nil {
			return err
		}
		defer close()
		nsc := simplens.Client{Service: s}
		names, err := nsc.ListNames(c, blobcache.RootHandle())
		if err != nil {
			return err
		}
		for _, name := range names {
			c.Printf("%v\n", name)
		}
		return nil
	},
}

var nameParam = star.Param[string]{
	Name:  "name",
	Parse: star.ParseString,
}

func openLocal(c star.Context) (*bclocal.Service, func(), error) {
	db, err := dbutil.OpenDB(filepath.Join(stateDirParam.Load(c), "blobcache.db"))
	if err != nil {
		return nil, nil, err
	}
	if err := bclocal.SetupDB(c, db); err != nil {
		return nil, nil, err
	}
	close := func() {
		db.Close()
	}
	return bclocal.New(bclocal.Env{DB: db}), close, nil
}

func RunTest(t testing.TB, env map[string]string, calledAs string, args []string, stdin *bufio.Reader, stdout *bufio.Writer, stderr *bufio.Writer) {
	if stdin == nil {
		stdin = bufio.NewReader(bytes.NewReader([]byte{}))
	}
	if stdout == nil {
		stdout = bufio.NewWriter(io.Discard)
	}
	if stderr == nil {
		stderr = bufio.NewWriter(io.Discard)
	}

	ctx := testutil.Context(t)
	err := star.Run(ctx, Root(), env, calledAs, args, stdin, stdout, stderr)
	require.NoError(t, err)
}
