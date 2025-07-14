package bcfs

import (
	"context"
	"syscall"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// FUSERoot returns the root node for FUSE mounting
func (fsx *FS[K]) FUSERoot() *Node[K] {
	if fsx.rootNode == nil {
		fsx.rootNode = &Node[K]{
			fs:  fsx,
			ino: 1, // Root directory inode is always 1
		}
	}
	return fsx.rootNode
}

// Node represents a FUSE node with generic filesystem reference
type Node[K comparable] struct {
	fs.Inode
	fs  *FS[K]
	ino int64
}

var _ fs.NodeGetattrer = (*Node[string])(nil)
var _ fs.NodeLookuper = (*Node[string])(nil)
var _ fs.NodeReaddirer = (*Node[string])(nil)
var _ fs.NodeCreater = (*Node[string])(nil)
var _ fs.NodeUnlinker = (*Node[string])(nil)
var _ fs.NodeReader = (*Node[string])(nil)
var _ fs.NodeWriter = (*Node[string])(nil)

// Getattr returns file attributes
func (n *Node[K]) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// Get the scheme ID for this inode
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return syscall.ENOENT
	}

	// Set basic attributes
	out.Attr.Ino = uint64(n.ino)
	out.Attr.Mode = fuse.S_IFDIR | 0755 // Default to directory
	out.Attr.Nlink = 2

	// For root directory, we know it's a directory
	if n.ino == 1 {
		out.Attr.Mode = fuse.S_IFDIR | 0755
		return 0
	}

	// For other nodes, we could query the scheme for more info
	_ = id // Use the scheme ID for future lookups

	return 0
}

// Lookup finds a child node by name
func (n *Node[K]) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Get the scheme ID for this directory
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return nil, syscall.ENOENT
	}

	// For root directory, use zero value of K
	if n.ino == 1 {
		var zeroK K
		id = zeroK
	}

	// Begin read transaction
	txh, err := n.fs.svc.BeginTx(ctx, n.fs.vol, blobcache.TxParams{Mutate: false})
	if err != nil {
		return nil, syscall.EIO
	}
	defer func() { _ = n.fs.svc.Abort(ctx, *txh) }()

	// Create transaction wrapper
	tx, err := blobcache.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Mutate: false})
	if err != nil {
		return nil, syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Read directory entries from the scheme
	entries, err := n.fs.scheme.ReadDir(ctx, tx, n.fs.root, id)
	if err != nil {
		return nil, syscall.EIO
	}

	// Find the requested entry
	for _, entry := range entries {
		if entry.Name == name {
			// Get or create inode for this child
			childIno := n.fs.GetOrCreateInode(entry.Child)

			// Set attributes for the entry
			out.Attr.Ino = uint64(childIno)
			out.Attr.Mode = entry.Mode
			out.Attr.Nlink = 1

			// Create child node
			child := &Node[K]{
				fs:  n.fs,
				ino: childIno,
			}

			return n.NewInode(ctx, child, fs.StableAttr{
				Mode: entry.Mode,
				Ino:  uint64(childIno),
			}), 0
		}
	}

	return nil, syscall.ENOENT
}

// Readdir reads directory entries
func (n *Node[K]) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Get the scheme ID for this directory
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return nil, syscall.ENOENT
	}

	// For root directory, use zero value of K
	if n.ino == 1 {
		var zeroK K
		id = zeroK
	}

	// Begin read transaction
	tx, err := blobcache.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Mutate: false})
	if err != nil {
		return nil, syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Read directory entries from the scheme
	entries, err := n.fs.scheme.ReadDir(ctx, tx, n.fs.root, id)
	if err != nil {
		return nil, syscall.EIO
	}

	// Convert to FUSE directory entries
	var fuseEntries []fuse.DirEntry
	for _, entry := range entries {
		childIno := n.fs.GetOrCreateInode(entry.Child)
		fuseEntries = append(fuseEntries, fuse.DirEntry{
			Name: entry.Name,
			Ino:  uint64(childIno),
			Mode: entry.Mode,
		})
	}

	return fs.NewListDirStream(fuseEntries), 0
}

// Create creates a new file
func (n *Node[K]) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Get the scheme ID for this directory
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return nil, nil, 0, syscall.ENOENT
	}

	// For root directory, use zero value of K
	if n.ino == 1 {
		var zeroK K
		id = zeroK
	}

	// Begin write transaction
	tx, err := blobcache.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Mutate: true})
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Create the file in the scheme
	childID, newRoot, err := n.fs.scheme.CreateAt(ctx, tx, tx, n.fs.root, id, name, mode)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	// Commit the transaction
	if err := tx.Commit(ctx, newRoot); err != nil {
		return nil, nil, 0, syscall.EIO
	}

	// Update the root
	n.fs.root = newRoot

	// Create inode for the new file
	childIno := n.fs.GetOrCreateInode(childID)

	// Set attributes
	out.Attr.Ino = uint64(childIno)
	out.Attr.Mode = mode
	out.Attr.Nlink = 1

	// Create child node
	child := &Node[K]{
		fs:  n.fs,
		ino: childIno,
	}

	inode := n.NewInode(ctx, child, fs.StableAttr{
		Mode: mode,
		Ino:  uint64(childIno),
	})

	return inode, nil, 0, 0
}

// Unlink removes a file
func (n *Node[K]) Unlink(ctx context.Context, name string) syscall.Errno {
	// Get the scheme ID for this directory
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return syscall.ENOENT
	}

	// For root directory, use zero value of K
	if n.ino == 1 {
		var zeroK K
		id = zeroK
	}

	// Begin write transaction
	tx, err := blobcache.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Mutate: true})
	if err != nil {
		return syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Delete the file from the scheme
	newRoot, err := n.fs.scheme.DeleteAt(ctx, tx, tx, n.fs.root, id, name)
	if err != nil {
		return syscall.EIO
	}

	// Commit the transaction
	if err := tx.Commit(ctx, newRoot); err != nil {
		return syscall.EIO
	}

	// Update the root
	n.fs.root = newRoot

	return 0
}

// Read reads file data
func (n *Node[K]) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Get the scheme ID for this file
	id, exists := n.fs.GetID(n.ino)
	if !exists {
		return nil, syscall.ENOENT
	}

	// Begin read transaction
	tx, err := blobcache.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Mutate: false})
	if err != nil {
		return nil, syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Read from the scheme
	buf := make([]byte, len(dest))
	bytesRead, err := n.fs.scheme.ReadFile(ctx, tx, n.fs.root, id, buf)
	if err != nil {
		return nil, syscall.EIO
	}

	// Handle offset and length
	if off >= int64(bytesRead) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > int64(bytesRead) {
		end = int64(bytesRead)
	}

	return fuse.ReadResultData(buf[off:end]), 0
}

// Write writes file data
func (n *Node[K]) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	// Get the scheme ID for this file
	id, exists := n.fs.GetID(n.ino)
	if !exists {
		return 0, syscall.ENOENT
	}

	// Write to the extent buffer
	err := n.fs.PutExtent(ctx, id, off, data)
	if err != nil {
		return 0, syscall.EIO
	}

	return uint32(len(data)), 0
}
