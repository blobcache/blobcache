package bckv

import "errors"

var (
	ErrFull = errors.New("store is full")
)

type KV interface {
	Get(k []byte) ([]byte, error)

	Put(k, v []byte) error

	Delete(k []byte) error

	Bucket(p string) KV

	ForEach(first, last []byte, fn func(k, v []byte) error) error

	SizeTotal() uint64
	SizeUsed() uint64
}