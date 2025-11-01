package blobcache

import (
	"errors"
	"fmt"

	"go.brendoncarroll.net/state/cadata"
)

// ErrTxDone is returned when a transaction is already done.
// This would be when calling Commit twice, or Abort after either a Commit or Abort.
// Calling Abort more than once is not an error, but it is not useful.
type ErrTxDone struct {
	ID OID
}

func (e ErrTxDone) Error() string {
	return fmt.Sprintf("transaction %v is already done", e.ID)
}

func IsErrTxDone(err error) bool {
	return errors.As(err, &ErrTxDone{})
}

// ErrNotFound is returned when a blob is not found.
type ErrNotFound = cadata.ErrNotFound

func IsErrNotFound(err error) bool {
	return cadata.IsNotFound(err)
}

// ErrTooLarge is returned when a blob is exceeds the maximum size of the Volume.
type ErrTooLarge struct {
	BlobSize int
	MaxSize  int
}

func (e ErrTooLarge) Error() string {
	return fmt.Sprintf("blob too large: %d > %d", e.BlobSize, e.MaxSize)
}

func IsErrTooLarge(err error) bool {
	return errors.As(err, &ErrTooLarge{})
}

// ErrInvalidHandle is returned when a handle is invalid.
type ErrInvalidHandle struct {
	Handle Handle
}

func (e ErrInvalidHandle) Error() string {
	return fmt.Sprintf("invalid handle: %v", e.Handle)
}

func IsErrInvalidHandle(err error) bool {
	return errors.As(err, &ErrInvalidHandle{})
}

// ErrCannotSalt is returned when a salt is provided to a volume that does not support salts.
type ErrCannotSalt struct{}

func (e ErrCannotSalt) Error() string {
	return "cannot salt"
}

// ErrNoLink is returned when a volume does not grant access to the requested target.
// It is never returned for broken handles, or missing blobs.
type ErrNoLink struct {
	Base   OID
	Target OID
}

func (e ErrNoLink) Error() string {
	return fmt.Sprintf("volume %v does not grant access to %v", e.Base, e.Target)
}

// ErrBadData is returned when the data does not match the expected CID.
type ErrBadData struct {
	Salt     *CID
	Expected CID
	Actual   CID
	Len      int
}

func (e ErrBadData) Error() string {
	return fmt.Sprintf("bad data: salt=%v, expected %s, actual %s, len=%d", e.Salt, e.Expected, e.Actual, e.Len)
}

// ErrTxReadOnly is returned when a transaction is read-only, and the caller calls a mutating method.
type ErrTxReadOnly struct {
	Tx OID
	Op string
}

func (e ErrTxReadOnly) Error() string {
	return fmt.Sprintf("transaction %v is read-only, cannot perform %v", e.Tx, e.Op)
}

type ErrTxNotGC struct {
	Op string
}

func (e ErrTxNotGC) Error() string {
	return fmt.Sprintf("transaction is not a GC transaction, cannot perform %v", e.Op)
}

type ErrPermission struct {
	Handle   Handle
	Rights   ActionSet
	Requires ActionSet
}

func (e ErrPermission) Error() string {
	return fmt.Sprintf("permission denied: handle=%v, rights=%v, requires=%v", e.Handle, e.Rights, e.Requires)
}
