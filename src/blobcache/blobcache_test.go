package blobcache

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"testing"

	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/src/inet256"
	"lukechampine.com/blake3"
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
	zeroCID := CID{}
	peer1 := inet256.NewID(ed25519.PublicKey(zeroCID[:]))
	ap1 := netip.MustParseAddrPort("127.0.0.1:1234")
	ap2 := netip.MustParseAddrPort("[::]:1234")
	u2 := URL{Node: peer1, IPPort: &ap1}
	u3 := URL{Node: peer1, IPPort: &ap2}
	u4 := URL{Node: peer1, IPPort: &ap2, Path: OIDPath{mkOID(t, 1), mkOID(t, 2)}}

	tcs := []testCase{
		{I: u1.String(), O: &u1},
		{I: u2.String(), O: &u2},
		{I: u3.String(), O: &u3},
		{I: u4.String(), O: &u4},
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

func mkOID(t testing.TB, i int) OID {
	var ret OID
	h := blake3.Sum256(binary.LittleEndian.AppendUint64(nil, uint64(i)))
	copy(ret[:], h[:])
	return ret
}
