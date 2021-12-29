package bcdb

import (
	"encoding/binary"

	"github.com/brendoncarroll/go-state"
	"github.com/pkg/errors"
)

type DB interface {
	Update(func(Tx) error) error
	View(func(Tx) error) error
}

type Tx interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	ForEach(br state.ByteRange, fn func(key, value []byte) error) error
}

// Increment returns and increments a 64 bit integer stored at k
// In a transaction.
func Increment(tx Tx, k []byte) (uint64, error) {
	value, err := tx.Get(k)
	if err != nil {
		return 0, err
	}
	var x uint64
	if value == nil {
	} else if len(value) != 8 {
		return 0, errors.Errorf("value (%q) at key (%q) not a 64 bit integer", value, k)
	} else {
		x = binary.BigEndian.Uint64(value)
	}
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], x)
	if err := tx.Put(k, buf[:]); err != nil {
		return 0, err
	}
	return x, nil
}
