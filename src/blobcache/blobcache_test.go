package blobcache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	t.Run("OID", func(t *testing.T) {
		tcs := []OID{
			{},
		}
		for i, tc := range tcs {
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				x := tc
				data := x.Marshal(nil)
				var y OID
				require.NoError(t, y.Unmarshal(data))
			})
		}
	})
	t.Run("HandleInfo", func(t *testing.T) {
		tcs := []HandleInfo{
			{},
		}
		for i, tc := range tcs {
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				x := tc
				data := x.Marshal(nil)
				var y HandleInfo
				require.NoError(t, y.Unmarshal(data))
			})
		}
	})
	t.Run("Handle", func(t *testing.T) {
		tcs := []Handle{
			{},
		}
		for i, tc := range tcs {
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				x := tc
				data := x.Marshal(nil)
				var y Handle
				require.NoError(t, y.Unmarshal(data))
			})
		}
	})
}

func TestParseURL(t *testing.T) {
	type testCase struct {
		I   string
		O   *URL
		Err error
	}
	u1 := URL{}
	tcs := []testCase{
		{
			I: u1.String(),
			O: &u1,
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d-%s", i, tc.I), func(t *testing.T) {
			u, err := ParseURL(tc.I)
			if tc.Err != nil {
				require.Equal(t, tc.Err, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.O, u)
		})
	}
}
