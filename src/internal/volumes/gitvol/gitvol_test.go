package gitvol

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cadata/storetest"
	"go.brendoncarroll.net/state/cells"
	"go.brendoncarroll.net/state/cells/celltest"
)

func TestVolumeInit(t *testing.T) {
	ctx := context.TODO()
	dir, repo := newTestRepo(t, true)
	vol := New(memory.NewStorage(), "file://"+dir, "master")
	require.NoError(t, vol.Reset(ctx))

	ref, err := repo.Reference(plumbing.ReferenceName("refs/heads/master"), true)
	require.NoError(t, err)
	t.Log(ref)
	commit, err := repo.CommitObject(ref.Hash())
	require.NoError(t, err)
	t.Log(commit)
	tree, err := commit.Tree()
	require.NoError(t, err)
	if len(tree.Entries) != 1 {
		t.Fatalf("expected 1 tree entry, got %d", len(tree.Entries))
	}
	if tree.Entries[0].Name != RootPath {
		t.Fatalf("expected tree entry name %q, got %q", RootPath, tree.Entries[0].Name)
	}
}

func TestVolumeLoad(t *testing.T) {
	ctx := context.TODO()
	dir, repo := newTestRepo(t, false)

	// Create a hello world file
	helloFile := filepath.Join(dir, RootPath)
	err := os.WriteFile(helloFile, []byte("Hello, World!\n"), 0644)
	require.NoError(t, err)

	w, err := repo.Worktree()
	require.NoError(t, err)
	_, err = w.Add(RootPath)
	require.NoError(t, err)
	_, err = w.Commit("Add hello world file", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
		},
	})
	require.NoError(t, err)

	vol := New(memory.NewStorage(), "file://"+dir, "master")

	buf, err := cells.Load(ctx, vol)
	require.NoError(t, err)
	t.Log(buf)
	require.Equal(t, "Hello, World!\n", string(buf))

	vol.CAS(ctx, &buf, nil, []byte("Hello, World!\n"))
}

func TestVolumeCellAPI(t *testing.T) {
	celltest.TestBytesCell(t, func(t testing.TB) cells.BytesCell {
		vol := NewTestVolume(t)
		return vol
	})
}

func TestVolumeStore(t *testing.T) {
	storetest.TestStore(t, func(t testing.TB) cadata.Store {
		if strings.HasSuffix(t.Name(), "MaxSize") {
			t.Skip("blobcache has different ErrTooLarge type")
		}
		vol := NewTestVolume(t)
		return vol
	})
}
