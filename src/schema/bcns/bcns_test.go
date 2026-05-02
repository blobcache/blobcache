package bcns

import (
	"fmt"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"
)

func TestValidName(t *testing.T) {
	type testCase struct {
		Name  string
		Valid bool
	}
		tcs := []testCase{
			// valid
			{Name: "a", Valid: true},
			{Name: "ab", Valid: true},
			{Name: "1abc", Valid: true},
			{Name: "hello", Valid: true},
			{Name: "a-b", Valid: true},
			{Name: "a_b", Valid: true},
			{Name: "a.b", Valid: true},
			{Name: "a/b", Valid: true},
			{Name: "a/b/c", Valid: true},
			{Name: "a/b.c-d_e/2", Valid: true},
			{Name: "abc/def/ghi", Valid: true},
			{Name: "a1b", Valid: true},
			{Name: "a1-2b", Valid: true},
			{Name: "a/b-c/d", Valid: true},

			// invalid: empty
			{Name: "", Valid: false},
			// invalid: starts with symbol
			{Name: "-abc", Valid: false},
			{Name: "/abc", Valid: false},
			{Name: "_abc", Valid: false},
			{Name: ".abc", Valid: false},
			// invalid: ends with non-alphanumeric
			{Name: "abc1", Valid: true},
			{Name: "abc-", Valid: false},
			{Name: "abc/", Valid: false},
			{Name: "abc_", Valid: false},
			{Name: "abc.", Valid: false},
			// invalid: disallowed characters
			{Name: "a b", Valid: false},
			{Name: "a@b", Valid: false},
		}
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			require.Equal(t, tc.Valid, IsValidName(tc.Name))
		})
	}
}

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
