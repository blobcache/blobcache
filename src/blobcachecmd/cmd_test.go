package blobcachecmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/blobcached"
	"github.com/stretchr/testify/require"

	_ "blobcache.io/blobcache/src/schema/jsonns"
)

func TestGLFS(t *testing.T) {
	stateDir := t.TempDir()
	_, apiUrl := blobcached.BGTestDaemon(t)
	env := map[string]string{
		bcclient.EnvBlobcacheAPI: apiUrl,
	}

	specJSON, err := json.Marshal(blobcache.DefaultLocalSpec())
	require.NoError(t, err)
	runCmdWithStdin(t, env, []string{"--state", stateDir, "ns", "create", "vol1"}, specJSON)
	runCmd(t, env, []string{"--state", stateDir, "ns", "ls"})

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

func runCmd(t testing.TB, env map[string]string, args []string) {
	RunTest(t, env, "blobcache", args, nil, nil, nil)
}

func runCmdWithStdin(t testing.TB, env map[string]string, args []string, stdinData []byte) {
	stdin := bufio.NewReader(bytes.NewReader(stdinData))
	RunTest(t, env, "blobcache", args, stdin, nil, nil)
}

func runCmdGetOut(t testing.TB, env map[string]string, args []string) []byte {
	stdoutBuf := bytes.Buffer{}
	bufw := bufio.NewWriter(&stdoutBuf)
	RunTest(t, env, "blobcache", args, nil, bufw, nil)
	require.NoError(t, bufw.Flush())
	return stdoutBuf.Bytes()
}
