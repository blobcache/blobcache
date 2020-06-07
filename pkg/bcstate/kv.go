package bcstate

import "errors"

var (
	ErrFull     = errors.New("store is full")
	ErrNotExist = errors.New("key does not exist")
)

type KV interface {
	GetF(k []byte, f func([]byte) error) error
	Put(k, v []byte) error
	Delete(k []byte) error

	ForEach(first, last []byte, fn func(k, v []byte) error) error

	SizeTotal() uint64
	SizeUsed() uint64
}

type DB interface {
	Bucket(string) KV
}

func Exists(kv KV, key []byte) (bool, error) {
	err := kv.GetF(key, func([]byte) error { return nil })
	if err == ErrNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
