package shard

import (
	"fmt"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPack(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	maxSize := uint32(1 << 20)
	pf, err := CreatePackFile(root, 0, maxSize)
	require.NoError(t, err)
	defer pf.Close()

	pk, err := NewPack(pf, 0)
	require.NoError(t, err)
	var wg sync.WaitGroup
	const N = 100000
	offsets := make([]uint32, N)
	datas := make([][]byte, N)
	for i := range offsets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			datas[i] = fmt.Appendf(nil, "hello-%d", i)
			offsets[i] = pk.Append(datas[i])
		}()
	}
	wg.Wait()
	require.NoError(t, pk.Flush())

	for i := 0; i < N-1; i++ {
		if offsets[i] == math.MaxUint32 {
			break
		}
		pk.Get(offsets[i], uint32(len(datas[i])), func(data []byte) {
			require.Equal(t, datas[i], data)
		})
	}
}
