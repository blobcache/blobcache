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

		// basicns management
		"basicns": basicnsCmd,

		"mkvol.local":  mkVolLocalCmd,
		"mkvol.remote": mkVolRemoteCmd,
		"mkvol.vault":  mkVolVaultCmd,

		// applications
		"glfs":       glfsCmd,
		"fuse-mount": fuseMountCmd,
	},
)

func openLocal(c star.Context) (*bclocal.Service, error) {
	for _, d := range []string{"pebble", "blob"} {
		if err := os.MkdirAll(filepath.Join(stateDirParam.Load(c), d), 0755); err != nil {
			return nil, err
		}
	}
	svc, err := bclocal.New(bclocal.Env{
		Background: c.Context,
		StateDir:   stateDirParam.Load(c),

		Schemas: bclocal.DefaultSchemas(),
		Root:    bclocal.DefaultRoot(),
	}, bclocal.Config{})
	if err != nil {
		return nil, err
	}
	return svc, nil
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
