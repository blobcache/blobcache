// Package bcfuse implements a filesystem interface for blobcache.
// bcfuse handles the frontend components of a filesystem, like FUSE and NFS mounting.
// The backend is provided by a blobcache volume.
// Translating the filesystem operations into blobcache operations is handled by a Scheme.
// bcfuse provides write buffering before the data is flushed to the volume.
package bcfuse

import (
	"context"
	"fmt"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/dbutil"
	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/state/cadata"
)

// Scheme handles filesystem operations on blobcache volumes.
// All reading methods follow the pattern: func(ctx context.Context, src cadata.Getter, root []byte, ...) (..., error)
// All writing methods follow the pattern: func(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root []byte, ...) (..., []byte, error)
type Scheme[K comparable] interface {
	// FlushExtents writes all the extents to the volume.
	FlushExtents(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root []byte, extents []Extent[K]) ([]byte, error)

	// ReadFile reads a file from the volume by identifier into the provided buffer
	ReadFile(ctx context.Context, src cadata.Getter, root []byte, id K, buf []byte) (int, error)

	// ReadDir reads directory entries for the given identifier
	ReadDir(ctx context.Context, src cadata.Getter, root []byte, id K) ([]DirEntry[K], error)

	// CreateAt creates a new file in the directory
	CreateAt(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root []byte, parentID K, name string, mode uint32) (K, []byte, error)

	// DeleteAt removes a file from the directory
	DeleteAt(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root []byte, parentID K, name string) ([]byte, error)
}

// Extent represents a data extent with generic ID
type Extent[K comparable] struct {
	ID    K
	Start int64
	Data  []byte
}

// DirEntry represents a directory entry with generic ID
type DirEntry[K comparable] struct {
	Name  string
	Child K
	Mode  uint32
}

// FS represents the filesystem with thread-safe operations
type FS[K comparable] struct {
	db     *sqlx.DB
	svc    blobcache.Service
	vol    blobcache.Handle
	scheme Scheme[K]

	// rootNode is set the first time FUSERoot is called
	rootNode *Node[K]

	mu sync.RWMutex
	// Root bytes held in memory (<1MB)
	root []byte
	// Inode management
	nextInode int64
	// Bidirectional mapping between POSIX inodes and scheme identifiers
	inodeToID map[int64]K
	idToInode map[K]int64
}

// New creates a new filesystem instance
func New[K comparable](db *sqlx.DB, svc blobcache.Service, vol blobcache.Handle, scheme Scheme[K]) *FS[K] {
	return &FS[K]{
		db:        db,
		svc:       svc,
		vol:       vol,
		scheme:    scheme,
		nextInode: 2, // Start at 2 since 1 is reserved for root
		inodeToID: make(map[int64]K),
		idToInode: make(map[K]int64),
	}
}

// Flush writes all pending extents to the volume
func (fs *FS[K]) Flush(ctx context.Context) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Get all extents from the database
	extents, err := fs.getAllExtents(ctx)
	if err != nil {
		return fmt.Errorf("failed to get extents: %w", err)
	}

	if len(extents) == 0 {
		return nil
	}

	// Begin a write transaction
	tx, err := blobcache.BeginTx(ctx, fs.svc, fs.vol, blobcache.TxParams{Mutate: true})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Load current root
	if err := tx.Load(ctx, &fs.root); err != nil {
		return fmt.Errorf("failed to load root: %w", err)
	}

	// Flush extents to the volume
	newRoot, err := fs.scheme.FlushExtents(ctx, tx, tx, fs.root, extents)
	if err != nil {
		return fmt.Errorf("failed to flush extents: %w", err)
	}

	// Commit the transaction with the new root
	if err := tx.Save(ctx, newRoot); err != nil {
		return fmt.Errorf("failed to save root: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fs.root = newRoot

	// Clear the extents table after successful flush
	return fs.clearExtents(ctx)
}

// PutExtent writes data to the extent table.
// Any overlapping extents are truncated so that they are non-overlapping.
func (fs *FS[K]) PutExtent(ctx context.Context, id K, startAt int64, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	endAt := startAt + int64(len(data))
	return dbutil.DoTx(ctx, fs.db, func(tx *sqlx.Tx) error {
		// Remove overlapping extents
		_, err := tx.Exec(`
			DELETE FROM extents 
			WHERE id = ? AND (
				(start < ? AND "end" > ?) OR
				(start >= ? AND start < ?)
			)
		`, id, endAt, startAt, startAt, endAt)
		if err != nil {
			return err
		}

		// Insert the new extent
		_, err = tx.Exec(`INSERT INTO extents (id, start, "end", data) VALUES (?, ?, ?, ?)`,
			id, startAt, endAt, data)
		return err
	})
}

// GetInode returns the POSIX inode for a scheme identifier
func (fs *FS[K]) GetInode(id K) int64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if ino, exists := fs.idToInode[id]; exists {
		return ino
	}

	return 0
}

// GetOrCreateInode returns the POSIX inode for a scheme identifier, creating one if needed
func (fs *FS[K]) GetOrCreateInode(id K) int64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if ino, exists := fs.idToInode[id]; exists {
		return ino
	}

	// Create new inode
	ino := fs.nextInode
	fs.nextInode++

	fs.idToInode[id] = ino
	fs.inodeToID[ino] = id

	return ino
}

// GetID returns the scheme identifier for a POSIX inode
func (fs *FS[K]) GetID(ino int64) (K, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	id, exists := fs.inodeToID[ino]
	return id, exists
}

// getAllExtents retrieves all extents from the database
func (fs *FS[K]) getAllExtents(ctx context.Context) ([]Extent[K], error) {
	rows, err := fs.db.QueryxContext(ctx, `SELECT id, start, "end", data FROM extents ORDER BY id, start`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var extents []Extent[K]
	for rows.Next() {
		var id K
		var start, end int64
		var data []byte

		err := rows.Scan(&id, &start, &end, &data)
		if err != nil {
			return nil, err
		}

		extents = append(extents, Extent[K]{
			ID:    id,
			Start: start,
			Data:  data,
		})
	}

	return extents, rows.Err()
}

// clearExtents removes all extents from the database
func (fs *FS[K]) clearExtents(ctx context.Context) error {
	return dbutil.DoTx(ctx, fs.db, func(tx *sqlx.Tx) error {
		_, err := tx.Exec("DELETE FROM extents")
		return err
	})
}
