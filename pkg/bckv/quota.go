package bckv

type FixedQuota struct {
	Store    KV
	Capacity uint64
}

func (q *FixedQuota) Get(key []byte) ([]byte, error) {
	return q.Store.Get(key)
}

func (q *FixedQuota) Put(key, value []byte) error {
	if q.SizeUsed() >= q.Capacity {
		if x, err := q.Get(key); err != nil {
			return err
		} else if x == nil {
			return ErrFull
		}
	}
	return q.Store.Put(key, value)
}

func (q *FixedQuota) Delete(key []byte) error {
	return q.Store.Delete(key)
}

func (q *FixedQuota) Bucket(p string) KV {
	return &FixedQuota{
		Store:    q.Store.Bucket(p),
		Capacity: q.Capacity,
	}
}

func (q *FixedQuota) SizeTotal() uint64 {
	return q.Capacity
}

func (q *FixedQuota) SizeUsed() uint64 {
	return q.Store.SizeUsed()
}

func (q *FixedQuota) ForEach(start, end []byte, fn func(k, v []byte) error) error {
	return q.Store.ForEach(start, end, fn)
}
