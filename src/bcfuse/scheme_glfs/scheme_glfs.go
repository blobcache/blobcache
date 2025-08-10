package scheme_glfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/bcfuse"
	"blobcache.io/glfs"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/cadata"
)

// Verify that Scheme implements bcfs.Scheme[string]
var _ bcfuse.Scheme[string] = (*Scheme)(nil)

// Scheme implements the bcfs.Scheme interface using GLFS
type Scheme struct {
	Machine *glfs.Machine
}

// NewScheme creates a new GLFS scheme
func NewScheme() *Scheme {
	return &Scheme{Machine: glfs.NewMachine()}
}

// FlushExtents writes all the extents to the volume
func (s *Scheme) FlushExtents(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root []byte, extents []bcfuse.Extent[string]) ([]byte, error) {
	// Load the current filesystem state
	var fsRef glfs.Ref
	if len(root) > 0 {
		if err := json.Unmarshal(root, &fsRef); err != nil {
			return nil, fmt.Errorf("failed to unmarshal root: %w", err)
		}
	} else {
		// Create empty filesystem
		ref, err := glfs.PostTreeSlice(ctx, dst, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create empty tree: %w", err)
		}
		fsRef = *ref
	}

	// Apply all extents by writing files
	for _, extent := range extents {
		// Create a blob for the extent data
		fileRef, err := glfs.PostBlob(ctx, dst, bytes.NewReader(extent.Data))
		if err != nil {
			return nil, fmt.Errorf("failed to post blob for %s: %w", extent.ID, err)
		}

		// Create a tree entry for this file
		entry := glfs.TreeEntry{
			Name: extent.ID,
			Ref:  *fileRef,
		}

		// Create a new tree with this file
		treeRef, err := glfs.PostTreeSlice(ctx, dst, []glfs.TreeEntry{entry})
		if err != nil {
			return nil, fmt.Errorf("failed to create tree for %s: %w", extent.ID, err)
		}

		// Merge with the existing filesystem
		mergedRef, err := glfs.Merge(ctx, dst, src, fsRef, *treeRef)
		if err != nil {
			return nil, fmt.Errorf("failed to merge extent for %s: %w", extent.ID, err)
		}
		fsRef = *mergedRef
	}

	// Marshal the new root
	newRoot, err := json.Marshal(fsRef)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new root: %w", err)
	}

	return newRoot, nil
}

// ReadFile reads a file from the volume by identifier into the provided buffer
func (s *Scheme) ReadFile(ctx context.Context, src cadata.Getter, root []byte, id string, buf []byte) (int, error) {
	// Load the filesystem
	var fsRef glfs.Ref
	if err := json.Unmarshal(root, &fsRef); err != nil {
		return 0, fmt.Errorf("failed to unmarshal root: %w", err)
	}

	// Get the file reference
	fileRef, err := glfs.GetAtPath(ctx, src, fsRef, id)
	if err != nil {
		return 0, fmt.Errorf("failed to get file %s: %w", id, err)
	}

	// Read the file data
	data, err := glfs.GetBlobBytes(ctx, src, *fileRef, len(buf))
	if err != nil {
		return 0, fmt.Errorf("failed to read file data: %w", err)
	}

	// Copy to buffer
	n := copy(buf, data)
	return n, nil
}

// ReadDir reads directory entries for the given identifier
func (s *Scheme) ReadDir(ctx context.Context, src cadata.Getter, root []byte, id string) ([]bcfuse.DirEntry[string], error) {
	// Load the filesystem
	var fsRef glfs.Ref
	if err := json.Unmarshal(root, &fsRef); err != nil {
		return nil, fmt.Errorf("failed to unmarshal root: %w", err)
	}

	// Get the directory reference
	var dirRef *glfs.Ref
	var err error
	if id == "" || id == "/" {
		dirRef = &fsRef
	} else {
		dirRef, err = glfs.GetAtPath(ctx, src, fsRef, id)
		if err != nil {
			return nil, fmt.Errorf("failed to get directory %s: %w", id, err)
		}
	}

	if dirRef.Type != glfs.TypeTree {
		return nil, fmt.Errorf("path %s is not a directory", id)
	}

	// Read directory entries
	tr, err := s.Machine.NewTreeReader(src, *dirRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create tree reader: %w", err)
	}

	var result []bcfuse.DirEntry[string]
	err = streams.ForEach(ctx, tr, func(entry glfs.TreeEntry) error {
		// Convert file mode to POSIX mode
		mode := uint32(entry.FileMode)
		if entry.Ref.Type == glfs.TypeTree {
			mode = mode | 0040000 // S_IFDIR
		} else {
			mode = mode | 0100000 // S_IFREG
		}

		childPath := entry.Name
		if id != "" && id != "/" {
			childPath = id + "/" + entry.Name
		}

		result = append(result, bcfuse.DirEntry[string]{
			Name:  entry.Name,
			Child: childPath,
			Mode:  mode,
		})
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read directory entries: %w", err)
	}

	return result, nil
}

// CreateAt creates a new file in the directory
func (s *Scheme) CreateAt(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root []byte, parentID string, name string, mode uint32) (string, []byte, error) {
	// Load the filesystem
	var fsRef glfs.Ref
	if len(root) > 0 {
		if err := json.Unmarshal(root, &fsRef); err != nil {
			return "", nil, fmt.Errorf("failed to unmarshal root: %w", err)
		}
	} else {
		// Create empty filesystem
		ref, err := glfs.PostTreeSlice(ctx, dst, nil)
		if err != nil {
			return "", nil, fmt.Errorf("failed to create empty tree: %w", err)
		}
		fsRef = *ref
	}

	// Create empty file
	fileRef, err := glfs.PostBlob(ctx, dst, bytes.NewReader([]byte{}))
	if err != nil {
		return "", nil, fmt.Errorf("failed to create empty file: %w", err)
	}

	// Create the file path
	filePath := name
	if parentID != "" && parentID != "/" {
		filePath = parentID + "/" + name
	}

	// Create a tree entry for this file
	entry := glfs.TreeEntry{
		Name: name,
		Ref:  *fileRef,
	}

	// Create a new tree with this file
	treeRef, err := glfs.PostTreeSlice(ctx, dst, []glfs.TreeEntry{entry})
	if err != nil {
		return "", nil, fmt.Errorf("failed to create tree: %w", err)
	}

	// Merge with the existing filesystem
	newFsRef, err := glfs.Merge(ctx, dst, src, fsRef, *treeRef)
	if err != nil {
		return "", nil, fmt.Errorf("failed to merge new file: %w", err)
	}

	// Marshal the new root
	newRoot, err := json.Marshal(newFsRef)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal new root: %w", err)
	}

	return filePath, newRoot, nil
}

// DeleteAt removes a file from the directory
func (s *Scheme) DeleteAt(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root []byte, parentID string, name string) ([]byte, error) {
	// Load the filesystem
	var fsRef glfs.Ref
	if err := json.Unmarshal(root, &fsRef); err != nil {
		return nil, fmt.Errorf("failed to unmarshal root: %w", err)
	}

	// For now, return an error since GLFS doesn't have a simple delete operation
	// This would need to be implemented by reconstructing the tree without the target file
	return nil, fmt.Errorf("delete operation not yet implemented")
}
