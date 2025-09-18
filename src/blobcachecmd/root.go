package blobcachecmd

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema/basicns"
	"github.com/cockroachdb/pebble"
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

		// volume management
		"ls":           lsCmd,
		"mkvol.local":  mkVolLocalCmd,
		"mkvol.remote": mkVolRemoteCmd,
		"mkvol.vault":  mkVolVaultCmd,

		// group management
		"addmem": addMemCmd,
		"rmmem":  rmMemCmd,
		"groups": groupsCmd,

		// authorization management
		"grant":  grantCmd,
		"revoke": revokeCmd,

		// applications
		"glfs":       glfsCmd,
		"fuse-mount": fuseMountCmd,
	},
)

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
		nsc := basicns.Client{Service: s}
		names, err := nsc.ListNames(c, blobcache.Handle{})
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
	for _, d := range []string{"pebble", "blob"} {
		if err := os.MkdirAll(filepath.Join(stateDirParam.Load(c), d), 0755); err != nil {
			return nil, nil, err
		}
	}
	db, err := pebble.Open(filepath.Join(stateDirParam.Load(c), "pebble"), &pebble.Options{})
	if err != nil {
		return nil, nil, err
	}
	blobDir, err := os.OpenRoot(filepath.Join(stateDirParam.Load(c), "blob"))
	if err != nil {
		return nil, nil, err
	}
	close := func() {
		db.Close()
		blobDir.Close()
	}
	return bclocal.New(bclocal.Env{
		DB:      db,
		BlobDir: blobDir,

		Schemas: bclocal.DefaultSchemas(),
		Root:    bclocal.DefaultRoot(),
	}, bclocal.Config{}), close, nil
}

// openService opens a service
func openService(c star.Context) (blobcache.Service, error) {
	apiUrl := c.Env[bcclient.EnvBlobcacheAPI]
	return bcclient.NewClient(apiUrl), nil
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
	t.Log(calledAs, args)
	err := star.Run(ctx, Root(), env, calledAs, args, stdin, stdout, stderr)
	require.NoError(t, err)
}
