package bcns

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"
)

func TestParseObjectish(t *testing.T) {
	type testCase struct {
		I string
		O ObjectExpr
	}
	h1 := blobcache.Handle{}
	h1.Secret[0] = 'a'
	tcs := []testCase{
		{I: ""},
		{I: blobcache.OID{}.String()},
		{I: h1.String(), O: ObjectExpr{Base: h1}},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d\n", i), func(t *testing.T) {
			actual, err := ParseObjectish(tc.I)
			require.NoError(t, err)
			if len(actual.Path) == 0 {
				actual.Path = nil
			}
			require.Equal(t, actual, tc.O)
		})
	}
}
