package bcstate

import "sync"

type QuotaDB struct {
	Capacity uint64
	DB
	mu      sync.RWMutex
	buckets map[string]KV
}

func (db *QuotaDB) Bucket(p string) KV {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.buckets == nil {
		db.buckets = map[string]KV{}
	}
	if _, exists := db.buckets[p]; !exists {
		db.buckets[p] = db.DB.Bucket(p)
	}
	return &quotaDBKV{KV: db.buckets[p], quotaDB: db}
}

type quotaDBKV struct {
	KV
	quotaDB *QuotaDB
}

func (q *quotaDBKV) SizeTotal() uint64 {
	q.quotaDB.mu.RLock()
	defer q.quotaDB.mu.RUnlock()
	othersUsed := uint64(0)
	for _, kv := range q.quotaDB.buckets {
		othersUsed += kv.Count()
	}

	return q.quotaDB.Capacity - othersUsed + q.Count()
}

func (q *quotaDBKV) Put(key, value []byte) error {
	if q.Count() >= q.MaxCount() {
		exists, err := Exists(q.KV, key)
		if err != nil {
			return err
		}
		if !exists {
			return ErrFull
		}
	}
	return q.KV.Put(key, value)
}

// type QuotaKV struct {
// 	Store KV
// 	Capacity uint64
// }

// func (q *QuotaKV) GetF(key []byte, f func([]byte) error) error {
// 	return q.Store.GetF(key, f)
// }

// func (q *QuotaKV) Put(key, value []byte) error {
// 	if q.SizeUsed() >= q.Capacity {
// 		exists, err := Exists(q.Store, key)
// 		if err != nil {
// 			return err
// 		}
// 		if !exists {
// 			return ErrFull
// 		}
// 	}
// 	return q.Store.Put(key, value)
// }

// func (q *FixedQuota) Delete(key []byte) error {
// 	return q.Store.Delete(key)
// }

// func (q *FixedQuota) SizeTotal() uint64 {
// 	return q.Capacity
// }

// func (q *FixedQuota) SizeUsed() uint64 {
// 	return q.Store.SizeUsed()
// }

// func (q *FixedQuota) ForEach(start, end []byte, fn func(k, v []byte) error) error {
// 	return q.Store.ForEach(start, end, fn)
// }
