package bcstate

import (
	"bytes"
	"sync"
	"sync/atomic"
)

type MemKV struct {
	Capacity uint64

	m     sync.Map
	count int64
	seq   uint64
}

func (kv *MemKV) GetF(key []byte, f func([]byte) error) error {
	value, ok := kv.m.Load(string(key))
	if !ok {
		return ErrFull
	}
	bytes := value.([]byte)
	return f(bytes)
}

func (kv *MemKV) Put(key, value []byte) error {
	count := atomic.LoadInt64(&kv.count)
	if uint64(count) >= kv.Capacity {
		_, exists := kv.m.Load(string(key))
		if !exists {
			return ErrFull
		}
	}
	atomic.AddInt64(&kv.count, 1)
	data := append([]byte{}, value...)
	kv.m.Store(string(key), data)
	return nil
}

func (kv *MemKV) Delete(key []byte) error {
	kv.m.Delete(string(key))
	atomic.AddInt64(&kv.count, -1)
	return nil
}

func (kv *MemKV) Bucket(p string) KV {
	return &MemKV{
		Capacity: kv.Capacity,
	}
}

func (kv *MemKV) SizeTotal() uint64 {
	return kv.Capacity
}

func (kv *MemKV) SizeUsed() uint64 {
	return uint64(atomic.LoadInt64(&kv.count))
}

func (kv *MemKV) ForEach(start, end []byte, fn func(k, v []byte) error) error {
	var err error
	kv.m.Range(func(k, v interface{}) bool {
		key := []byte(k.(string))
		value := v.([]byte)
		if bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0 {
			err = fn(key, value)
			return err == nil
		}
		return true
	})
	return nil
}

func (kv *MemKV) NextSequence() (uint64, error) {
	return atomic.AddUint64(&kv.seq, 1), nil
}
