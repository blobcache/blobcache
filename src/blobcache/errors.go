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
