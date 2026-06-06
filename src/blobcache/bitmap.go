package blobcache

import (
	"math/bits"

	"go.brendoncarroll.net/exp/sbe"
)

// BitMap is a vector of bits where each bit can be set to 0 or 1 independently.
type BitMap []uint

// IsSet returns true if bit i is true
func (bm BitMap) IsSet(i int) bool {
	if i < 0 {
		return false
	}

	word := i / bits.UintSize
	if word >= len(bm) {
		return false
	}

	bit := uint(i % bits.UintSize)
	return bm[word]&(uint(1)<<bit) != 0
}

// Set causes bit i to be true/1
func (bm *BitMap) Set(i int) {
	if i < 0 {
		return
	}
	word := i / bits.UintSize
	if word >= len(*bm) {
		grown := make(BitMap, word+1)
		copy(grown, *bm)
		*bm = grown
	}
	bit := uint(i % bits.UintSize)
	(*bm)[word] |= uint(1) << bit
}

// Unset causes bit i to be false/0
func (bm *BitMap) Unset(i int) {
	if i < 0 {
		return
	}
	word := i / bits.UintSize
	if word >= len(*bm) {
		return
	}
	bit := uint(i % bits.UintSize)
	(*bm)[word] &^= uint(1) << bit
}

// Reset sets all bits in the bitmap to 0
func (bm *BitMap) Reset() {
	clear(*bm)
}

// Len returns the number of bits currently represented in the bitmap
func (bm *BitMap) Len() int {
	return len(*bm) * bits.UintSize
}

// OR sets the bit map to be equal to itself bitwised OR'd with o
func (bm *BitMap) OR(o BitMap) {
	for i := 0; i < o.Len(); i++ {
		if o.IsSet(i) {
			bm.Set(i)
		}
	}
}

func (bm *BitMap) Marshal(out []byte) []byte {
	switch bits.UintSize {
	case 32:
		for _, x := range *bm {
			out = sbe.AppendUint32(out, uint32(x))
		}
	case 64:
		for _, x := range *bm {
			out = sbe.AppendUint64(out, uint64(x))
		}
	default:
		panic(bits.UintSize)
	}
	return out
}

func (bm *BitMap) Unmarshal(data []byte) error {
	wordSize := bits.UintSize / 8
	n := (len(data) + wordSize - 1) / wordSize
	if cap(*bm) < n {
		*bm = make(BitMap, n)
	} else {
		*bm = (*bm)[:n]
	}
	clear(*bm)

	switch bits.UintSize {
	case 32:
		for i := range *bm {
			if len(data) >= 4 {
				x, rest, err := sbe.ReadUint32(data)
				if err != nil {
					return err
				}
				(*bm)[i] = uint(x)
				data = rest
				continue
			}
			for j, b := range data {
				(*bm)[i] |= uint(b) << (8 * j)
			}
			data = nil
		}
	case 64:
		for i := range *bm {
			if len(data) >= 8 {
				x, rest, err := sbe.ReadUint64(data)
				if err != nil {
					return err
				}
				(*bm)[i] = uint(x)
				data = rest
				continue
			}
			for j, b := range data {
				(*bm)[i] |= uint(b) << (8 * j)
			}
			data = nil
		}
	default:
		panic(bits.UintSize)
	}
	return nil
}
