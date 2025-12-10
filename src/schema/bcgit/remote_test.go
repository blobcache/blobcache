package bcgit

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/blobcached"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

var enableGitOut = true

func TestLsRemote(t *testing.T) {
	ctx := testutil.Context(t)
	_, apiStr := blobcached.RunTestDaemon(t)
	bc := bchttp.NewClient(nil, apiStr)
	wd := t.TempDir()
	testutil.BuildGoExec(t, filepath.Join(wd, "git-remote-bc"), "../../../cmd/git-remote-bc")
	te := testEnv{
		Dir:    wd,
		APIStr: apiStr,
	}

	// prepare volume
	gitVol, err := bc.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	require.NoError(t, err)
	rem := NewRemote(bc, *gitVol)
	refs := []GitRef{
		{Name: "refs/heads/999", Target: sha256.Sum256([]byte("asdfasdflkasjd;flkasjdf;"))},
		{Name: "refs/heads/888"},
		{Name: "refs/heads/777"},
		{Name: "refs/heads/666"},
	}
	require.NoError(t, rem.putRefs(ctx, refs))
	// prepare url
	ep, err := bc.Endpoint(ctx)
	require.NoError(t, err)
	u := blobcache.URL{
		Node: ep.Peer,
		Base: gitVol.OID,
	}

	gitInit(t, te)
	cmd(t, te, "git", "remote", "add", "test1", FmtURL(u))
	refsActual := gitLsRemote(t, te, "test1")
	t.Log(refsActual)
	require.Equal(t, len(refs), len(refsActual))
}

type testEnv struct {
	Dir    string
	APIStr string
}

func gitInit(t testing.TB, te testEnv) {
	cmd(t, te, "git", "init", "--object-format=sha256")
}

func gitLsRemote(t testing.TB, te testEnv, remoteName string) []string {
	lines := strings.Split(string(cmd(t, te, "git", "ls-remote", remoteName)), "\n")
	lines = slices.DeleteFunc(lines, func(x string) bool { return x == "" })
	return lines
}

func cmd(t testing.TB, te testEnv, name string, args ...string) []byte {
	t.Helper()
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	cmd := exec.Command(name, args...)
	cmd.Dir = te.Dir
	cmd.Env = []string{
		"HOME=" + home,
		"PATH=/usr/bin:",
		// TODO: don't use the system blobcache
		"BLOBCACHE_API=" + te.APIStr,
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
