// package dbtab contains constant IDs for each Table
package dbtab

import (
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
)

const (
	TID_UNKNOWN = pdb.TableID(iota)

	// tid_SYS_TXNS holds the transaction sequence number, and set of active transactions.
	// This is for database-wide transactions, rather than Volume transactions
	// although one is implemented using the other.
	TID_SYS_TXNS
	// TID_MISC is for one off key value pairs
	TID_MISC
)

const (
	// tid_BLOB_META holds blob metadata.
	TID_BLOB_META = pdb.TableID(1*256 + iota)
	// tid_BLOB_REF_COUNT holds the reference count for a blob.
	TID_BLOB_REF_COUNT
)

const (
	// tid_VOLUMES holds volume information.
	// This includes all volume information, not just local volumes.
	TID_VOLUMES = pdb.TableID(2*256 + iota)
	// tid_VOLUME_DEPS holds the dependencies for a volume.
	// These are not user-defined relationships
	// Volumes that wrap other volumes will reference those volumes as dependencies.
	TID_VOLUME_DEPS
	// tid_VOLUME_DEPS_INV holds the inverse of the VOLUME_DEPS table.
	TID_VOLUME_DEPS_INV
)

const (
	// tid_LOCAL_VOLUME_TXNS holds active transactions on local volumes.
	TID_LOCAL_VOLUME_TXNS = pdb.TableID(3*256 + iota)
	// tid_LOCAL_VOLUME_CELLS holds the root data for a local volume.
	// This table uses the MVCC keys
	TID_LOCAL_VOLUME_CELLS
	// tid_LOCAL_VOLUME_BLOBS holds the blobs for a local volume.
	// This table uses the MVCC keys.
	TID_LOCAL_VOLUME_BLOBS
	// tid_LOCAL_VOLUME_LINKS holds links from one volume to another.
	// These are user-defined relationships, provided through the AllowLink method on transactions.
	TID_LOCAL_VOLUME_LINKS
	// tid_VOLUME_LINKS_INV holds the inverse of the VOLUME_LINKS table.
	TID_LOCAL_VOLUME_LINKS_INV
)
