package blobcache

import (
	"errors"
	"fmt"
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

// ErrNotFound is returned when a resource is not found.
type ErrNotFound struct {
	Type string
	ID   OID
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("%s %v not found", e.Type, e.ID)
}

func IsErrNotFound(err error) bool {
	return errors.As(err, &ErrNotFound{})
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

// ErrTxReadOnly is returned when a transaction is read-only, and the caller calls Commit.
type ErrTxReadOnly struct {
	Tx OID
}

func (e ErrTxReadOnly) Error() string {
	return fmt.Sprintf("transaction %v is read-only", e.Tx)
}
