package bcstate

import "sync"

type Cell interface {
	LoadF(func([]byte) error) error
	Store([]byte) error
}

type KVCell struct {
	KV  KV
	Key string
}

func (c KVCell) LoadF(f func(data []byte) error) error {
	return c.KV.GetF([]byte(c.Key), f)
}

func (c KVCell) Store(data []byte) error {
	return c.KV.Put([]byte(c.Key), data)
}

type MemCell struct {
	mu   sync.Mutex
	data []byte
}

func (c *MemCell) LoadF(f func(data []byte) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return f(c.data)
}

func (c *MemCell) Store(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = append([]byte{}, data...)
	return nil
}
