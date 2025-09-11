// package sbe implements simple binary encoding formats for serializing and deserializing data.
package sbe

import (
	"encoding/binary"
	"fmt"
)

func AppendUVarint(out []byte, x uint64) []byte {
	return binary.AppendUvarint(out, x)
}

func ReadUVarint(x []byte) (uint64, []byte, error) {
	i, n := binary.Uvarint(x)
	if n <= 0 {
		return 0, nil, fmt.Errorf("too short to contain uvarint")
	}
	return i, x[n:], nil
}
