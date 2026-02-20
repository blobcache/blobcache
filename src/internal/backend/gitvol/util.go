package gitvol

import (
	"context"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/stretchr/testify/require"
)

func NewTestVolume(t testing.TB) *Volume {
	dir, _ := newTestRepo(t, true)
	vol := New(memory.NewStorage(), "file://"+dir, "master")
	require.NoError(t, vol.Reset(context.TODO()))
	return vol
}

func newTestRepo(t testing.TB, bare bool) (string, *git.Repository) {
	dir := t.TempDir()
	repo, err := git.PlainInit(dir, bare)
	require.NoError(t, err)
	return dir, repo
}
