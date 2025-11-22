package shard

import (
	"sync/atomic"
	"unsafe"
)

// bloom2048 is a bloom filter with 2048 bits.
//
// The idea is have a bloom filter indexing every 4096 bytes in the table file.
// Each row in the table file is allocated 32 bytes.
// So that is 128 rows per bloom filter.
// The bloom filter is lock free, and safe for concurrent use.
//
// Parameter Choices: https://hur.st/bloomfilter/?n=128&p&m=2048&k=8
type bloom2048 struct {
	data [32]uint64
}

func (bf *bloom2048) set(update [256]byte) {
	update2 := (*[32]uint64)(unsafe.Pointer(&update))
	for i := range update2 {
		if update2[i] == 0 {
			continue
		}
		atomic.OrUint64(&bf.data[i], update2[i])
	}
}

func (bf *bloom2048) allExist(fp [256]byte) bool {
	fp2 := (*[32]uint64)(unsafe.Pointer(&fp))
	var exists uint64
	for i := range fp2 {
		if fp2[i] == 0 {
			continue
		}
		d := atomic.LoadUint64(&bf.data[i])
		exists |= d & fp2[i]
	}
	return exists != 0
}

func (bf *bloom2048) bitPattern(k Key) [256]byte {
	var pat [256]byte
	// need 8 hashes
	var hashes [8]uint64
	bloomHash(k, &hashes)
	for h := range hashes {
		bucket := hashes[h] % 2048
		pat[bucket/8] |= 1 << (bucket % 8)
	}
	return pat
}

func (bf *bloom2048) add(k Key) {
	bf.set(bf.bitPattern(k))
}

func (bf *bloom2048) contains(k Key) bool {
	return bf.allExist(bf.bitPattern(k))
}

func bloomHash(k Key, out *[8]uint64) {
	for i := range *out {
		k := k.RotateAway(i * 8)
		out[i] = k.Uint64(0) ^ k.Uint64(1)
	}
}
