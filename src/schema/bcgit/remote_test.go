package bcgit

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

var enableGitOut = false

func TestRemoteHelper(t *testing.T) {
	wd := t.TempDir()
	testutil.BuildGoExec(t, filepath.Join(wd, "git-remote-bc"), "../../../cmd/git-remote-bc")

	gitInit(t, wd)
	u := blobcache.URL{}
	cmd(t, wd, "git", "remote", "add", "test1", FmtURL(u))
	t.Log(gitLsRemote(t, wd, "test1"))
}

func gitInit(t testing.TB, wd string) {
	cmd(t, wd, "git", "init", "--object-format=sha256")
}

func gitLsRemote(t testing.TB, wd string, remoteName string) []string {
	return strings.Split(string(cmd(t, wd, "git", "ls-remote", remoteName)), "\n")
}

func cmd(t testing.TB, wd string, name string, args ...string) []byte {
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	cmd := exec.Command(name, args...)
	cmd.Dir = wd
	cmd.Env = []string{
		"HOME=" + home,
		"PATH=/usr/bin:",
		// TODO: don't use the system blobcache
		"BLOBCACHE_API=" + os.Getenv("BLOBCACHE_API"),
	}
	var stdout bytes.Buffer
	if enableGitOut {
		cmd.Stdout = io.MultiWriter(&stdout, os.Stdout)
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = &stdout
	}

	require.NoError(t, cmd.Run())
	return stdout.Bytes()
}
