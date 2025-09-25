package blobcachecmd

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/internal/blobcached"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

func TestGLFS(t *testing.T) {
	stateDir := t.TempDir()
	runCmd(t, nil, []string{"--state", stateDir, "basicns", "createat", "vol1"})
	runCmd(t, nil, []string{"--state", stateDir, "basicns", "ls"})
	apiUrl := setupTestDaemon(t, stateDir)
	env := map[string]string{
		bcclient.EnvBlobcacheAPI: apiUrl,
	}
	runCmd(t, env, []string{"glfs", "init", "vol1"})
	runCmd(t, env, []string{"glfs", "look", "vol1", "/"})

	inputDir := t.TempDir()
	// write file1
	data1 := []byte("hello")
	require.NoError(t, os.WriteFile(filepath.Join(inputDir, "file1"), data1, 0644))
	// put file1
	runCmd(t, env, []string{"glfs", "import", "vol1", "/file1", filepath.Join(inputDir, "file1")})
	// get file1
	data2 := runCmdGetOut(t, env, []string{"glfs", "read", "vol1", "/file1"})
	// check file1
	require.Equal(t, data1, data2)
}

func setupTestDaemon(t testing.TB, stateDir string) (apiURL string) {
	ctx := testutil.Context(t)
	sockPath := filepath.Join(stateDir, "blobcache.sock")
	apiUrl := "unix://" + sockPath
	lis, err := net.ListenUnix("unix", &net.UnixAddr{
		Name: sockPath,
		Net:  "unix",
	})
	require.NoError(t, err)
	d := blobcached.Daemon{StateDir: stateDir}
	go func() {
		if err := d.Run(ctx, testutil.PacketConn(t), lis); err != nil {
			logctx.Error(ctx, "blobcached failed", zap.Error(err))
		}
	}()
	t.Cleanup(func() {
		require.NoError(t, lis.Close())
	})

	svc := bcclient.NewClient(apiUrl)
	t.Log("awaiting healthy", apiUrl)
	require.NoError(t, blobcached.AwaitHealthy(ctx, svc))
	return apiUrl
}

func runCmd(t testing.TB, env map[string]string, args []string) {
	RunTest(t, env, "blobcache", args, nil, nil, nil)
}

func runCmdGetOut(t testing.TB, env map[string]string, args []string) []byte {
	stdoutBuf := bytes.Buffer{}
	bufw := bufio.NewWriter(&stdoutBuf)
	RunTest(t, env, "blobcache", args, nil, bufw, nil)
	require.NoError(t, bufw.Flush())
	return stdoutBuf.Bytes()
}
