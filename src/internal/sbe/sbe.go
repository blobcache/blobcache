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

func AppendLP(out []byte, x []byte) []byte {
	out = AppendUVarint(out, uint64(len(x)))
	return append(out, x...)
}

func ReadLP(x []byte) ([]byte, []byte, error) {
	l, x, err := ReadUVarint(x)
	if err != nil {
		return nil, nil, err
	}
	if l > uint64(len(x)) {
		return nil, nil, fmt.Errorf("buffer too short to contain %d bytes. only %d", l, len(x))
	}
	return x[:int(l)], x[int(l):], nil
}

func AppendUint32(out []byte, x uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], x)
	return append(out, buf[:]...)
}

func ReadUint32(x []byte) (uint32, []byte, error) {
	if len(x) < 4 {
		return 0, nil, fmt.Errorf("too short to contain uint32")
	}
	return binary.LittleEndian.Uint32(x[:4]), x[4:], nil
}

// Uint64Bytes returns x as bytes in little endian order.
func Uint64Bytes(x uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], x)
	return buf[:]
}
