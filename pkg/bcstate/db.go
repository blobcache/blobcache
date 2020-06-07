package bcstate

import "path"

type PrefixedDB struct {
	Prefix string
	DB
}

func (db PrefixedDB) Bucket(p string) KV {
	p2 := path.Join(db.Prefix, p)
	return db.DB.Bucket(p2)
}

type MemDB struct {
	buckets map[string]*MemKV
}

func (db *MemDB) Bucket(p string) KV {
	if db.buckets == nil {
		db.buckets = make(map[string]*MemKV)
	}
	if _, exists := db.buckets[p]; !exists {
		db.buckets[p] = &MemKV{}
	}
	return db.buckets[p]
}
