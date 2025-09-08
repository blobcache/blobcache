package pebbletest

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestPebble(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)

	b1 := db.NewIndexedBatch()
	b2 := db.NewIndexedBatch()
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		require.NoError(t, b1.Set(k, []byte(fmt.Sprintf("value-%d", i)), nil))
		if _, closer, err := b2.Get(k); err == nil {
			require.NoError(t, closer.Close())
		}
		require.NoError(t, b2.Set(k, []byte(fmt.Sprintf("othervalue-%d", i)), nil))
	}
	require.NoError(t, b1.Commit(pebble.Sync))
	require.NoError(t, b2.Commit(pebble.Sync))

	it, err := db.NewIter(nil)
	require.NoError(t, err)
	defer it.Close()

	// Print the first 10 keys from the iterator
	count := 0
	it.SeekGE([]byte("key-0"))
	for it.First(); it.Valid() && count < 10; it.Next() {
		fmt.Printf("%d) %s %s\n", count+1, string(it.Key()), string(it.Value()))
		count++
	}
	require.NoError(t, it.Close())

	require.NoError(t, db.Close())
}
