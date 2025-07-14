package bcfs

import (
	"testing"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

// TestFS tests a filesystem implementation with the given scheme factory
func TestFS[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	t.Run("BasicOperations", func(t *testing.T) {
		testBasicOperations(t, newScheme)
	})

	t.Run("InodeMapping", func(t *testing.T) {
		testInodeMapping(t, newScheme)
	})

	t.Run("ExtentOperations", func(t *testing.T) {
		testExtentOperations(t, newScheme)
	})

	t.Run("FlushOperations", func(t *testing.T) {
		testFlushOperations(t, newScheme)
	})
}

// testBasicOperations tests basic filesystem operations
func testBasicOperations[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	fs := newTestFS(t, newScheme(t))

	// Test that we can create a filesystem
	require.NotNil(t, fs)

	// Test that we can get the FUSE root
	root := fs.FUSERoot()
	require.NotNil(t, root)
	require.Equal(t, int64(1), root.ino)
}

// testInodeMapping tests inode to ID mapping
func testInodeMapping[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	fs := newTestFS(t, newScheme(t))

	// We need a test ID - this will depend on the concrete type K
	// For now, we'll skip this test if we can't create a test ID
	var testID K
	// Try to create a meaningful test ID based on the type
	switch any(testID).(type) {
	case string:
		testID = any("test-file.txt").(K)
	default:
		t.Skip("Cannot create test ID for type", testID)
		return
	}

	// Test that we can get an inode for an ID
	ino := fs.GetOrCreateInode(testID)
	require.Greater(t, ino, int64(1)) // Should be > 1 (root)

	// Test that we get the same inode for the same ID
	ino2 := fs.GetOrCreateInode(testID)
	require.Equal(t, ino, ino2)

	// Test reverse lookup
	id, exists := fs.GetID(ino)
	require.True(t, exists)
	require.Equal(t, testID, id)

	// Test lookup of non-existent inode
	_, exists = fs.GetID(9999)
	require.False(t, exists)
}

// testExtentOperations tests extent buffering operations
func testExtentOperations[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fs := newTestFS(t, newScheme(t))

	// We need a test ID
	var fileID K
	switch any(fileID).(type) {
	case string:
		fileID = any("test-file.txt").(K)
	default:
		t.Skip("Cannot create test ID for type", fileID)
		return
	}

	// Test putting an extent
	data := []byte("hello world")
	err := fs.PutExtent(ctx, fileID, 0, data)
	require.NoError(t, err)

	// Test putting another extent
	data2 := []byte(" more data")
	err = fs.PutExtent(ctx, fileID, int64(len(data)), data2)
	require.NoError(t, err)
}

// testFlushOperations tests flushing extents to the volume
func testFlushOperations[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fs := newTestFS(t, newScheme(t))

	// We need a test ID
	var fileID K
	switch any(fileID).(type) {
	case string:
		fileID = any("test-file.txt").(K)
	default:
		t.Skip("Cannot create test ID for type", fileID)
		return
	}

	// Put some extent data
	data := []byte("hello world")
	err := fs.PutExtent(ctx, fileID, 0, data)
	require.NoError(t, err)

	// Test flushing extents
	err = fs.Flush(ctx)
	require.NoError(t, err)
}

// newTestFS creates a new filesystem for testing with the given scheme
func newTestFS[K comparable](t testing.TB, scheme Scheme[K]) *FS[K] {
	ctx := testutil.Context(t)
	db := dbutil.OpenMemory()
	require.NoError(t, SetupDB(ctx, db))
	svc := bclocal.NewTestService(t)
	volh, err := svc.CreateVolume(ctx, blobcache.DefaultLocalSpec())
	require.NoError(t, err)
	return New(db, svc, *volh, scheme)
}
