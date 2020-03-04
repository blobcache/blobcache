package bckv

import (
	"sync"
	"sync/atomic"
)

type MemKV struct {
	Capacity uint64

	m     sync.Map
	count int64
}

func (kv *MemKV) Get(key []byte) ([]byte, error) {
	value, ok := kv.m.Load(string(key))
	if !ok {
		return nil, nil
	}
	bytes := value.([]byte)
	return append([]byte{}, bytes...), nil
}

func (kv *MemKV) Put(key, value []byte) error {
	count := atomic.LoadInt64(&kv.count)
	if uint64(count) >= kv.Capacity {
		if v, err := kv.Get(key); err != nil {
			return err
		} else if v == nil {
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
		err = fn([]byte(k.(string)), v.([]byte))
		return err == nil
	})
	return nil
}
