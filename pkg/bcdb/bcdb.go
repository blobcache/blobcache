package bcdb

import (
	"context"
	"encoding/binary"

	"github.com/brendoncarroll/go-state"
	"github.com/pkg/errors"
)

type DB interface {
	Update(context.Context, func(Tx) error) error
	View(context.Context, func(Tx) error) error
}

type Tx interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	ForEach(br state.ByteRange, fn func(key, value []byte) error) error
}

// Increment increments and returns a 64 bit integer stored at k.
// An empty value is interpretted as 0.
// Increment returns the value after the increment, so it is suitable for reference counts.
func Increment(tx Tx, k []byte) (uint64, error) {
	return increment(tx, k, 1)
}

func Decrement(tx Tx, k []byte) (uint64, error) {
	return increment(tx, k, -1)
}

func increment(tx Tx, k []byte, delta int64) (uint64, error) {
	value, err := tx.Get(k)
	if err != nil {
		return 0, err
	}
	var x uint64
	if value == nil {
		x = 0
	} else if len(value) != 8 {
		return 0, errors.Errorf("value (%q) at key (%q) not a 64 bit integer", value, k)
	} else {
		x = binary.BigEndian.Uint64(value)
	}
	var y uint64
	if delta < 0 {
		y = x - uint64(-delta)
	} else {
		y = x + uint64(delta)
	}
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], y)
	if err := tx.Put(k, buf[:]); err != nil {
		return 0, err
	}
	return y, nil
}

// PrefixEnd return the key > all the keys with prefix p, but < any other key
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
