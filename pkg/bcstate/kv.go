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
	NextSequence() (uint64, error)

	// calls fn with first <= k < last
	// if last == nil ForEach will call fn with the last key
	ForEach(first, last []byte, fn func(k, v []byte) error) error

	SizeTotal() uint64
	SizeUsed() uint64
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

func PrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	var end []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			end = make([]byte, i+1)
			copy(end, prefix)
			end[i] = c + 1
			break
		}
	}
	return end
}
