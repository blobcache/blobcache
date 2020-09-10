package bitstrings

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

type BitString struct {
	n int
	b string
}

func FromBytes(n int, buf []byte) BitString {
	end := bufLen(n)
	if end > len(buf) {
		panic("buffer not long enough to create bitstring")
	}
	buf = buf[:end]
	return BitString{n: n, b: string(buf)}
}

func (bs BitString) AppendBit(x bool) BitString {
	n := bs.n + 1
	end := bufLen(n)
	b := []byte(bs.b)
	if len(b) < end {
		b = append(b, 0)
	}
	b[end-1] &= 1 << uint(n%8)

	return BitString{
		n: bs.n + 1,
		b: string(b),
	}
}

func (bs BitString) At(i int) bool {
	x := bs.b[i/8] & (128 >> uint(i%8))
	return x > 0
}

func (bs BitString) Len() int {
	return bs.n
}

func (bs BitString) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%x", bs.full()))
	if bs.n%8 > 0 {
		sb.WriteString(".")
		for i := len(bs.full()) * 8; i < bs.n; i++ {
			if bs.At(i) {
				sb.WriteString("1")
			} else {
				sb.WriteString("0")
			}
		}
	}
	return sb.String()
}

func (x BitString) LongString() string {
	sb := strings.Builder{}
	for i := 0; i < x.n; i++ {
		if x.At(i) {
			sb.WriteString("1")
		} else {
			sb.WriteString("0")
		}
	}
	return sb.String()
}

func (x BitString) EnumBytePrefixes() [][]byte {
	if x.n%8 == 0 {
		return [][]byte{[]byte(x.b)}
	}

	prefixes := [][]byte{}
	m := x.mask()
	end := bufLen(x.n)
	for i := 0; i < 256; i++ {
		c := byte(i)
		if (^(x.b[end-1] ^ c))&m > 0 {
			prefix := append([]byte(x.b), c)
			prefixes = append(prefixes, prefix)
		}
	}
	return prefixes
}

func (a *BitString) full() string {
	return string(a.b[:a.n/8])
}

func (a *BitString) last() uint8 {
	if a.n == 0 {
		return 0
	}
	if a.n < 8 {
		return 0
	}
	return a.b[a.n/8-1]
}

func (a *BitString) mask() uint8 {
	return ^(0xff >> (a.n % 8))
}

func bufLen(n int) int {
	end := n / 8
	if n%8 > 0 {
		end++
	}
	return end
}

func (bs *BitString) UnmarshalBinary(data []byte) error {
	x, total := binary.Uvarint(data)
	bs.n = int(x)
	if total >= len(data) {
		return errors.New("could not unmarshal bitstring data too short")
	}
	bs.b = string(data[total:])
	return nil
}

func (bs BitString) MarshalBinary() ([]byte, error) {
	buf := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(buf, uint64(bs.n))
	buf = buf[:n]
	buf = append(buf, bs.b...)
	return buf, nil
}

func (bs BitString) Marshal() []byte {
	data, _ := bs.MarshalBinary()
	return data
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func HasPrefix(x, prefix BitString) bool {
	if x.n < prefix.n {
		return false
	}
	for i := 0; i < prefix.n; i++ {
		if x.At(i) != prefix.At(i) {
			return false
		}
	}
	return true
}
