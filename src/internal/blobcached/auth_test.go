package blobcached

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

func TestParseIdentitiesFile(t *testing.T) {
	peer1 := mkPeerID(1)
	tcs := []struct {
		I string
		O []Membership[Identity]
	}{
		{
			I: "alice " + peer1.String() + "\n\n",
			O: []Membership[Identity]{{Group: "alice", Member: Member[Identity]{Unit: &peer1}}},
		},
		{
			I: "everyone " + Everyone.String() + "\n\n",
			O: []Membership[Identity]{
				{Group: "everyone", Member: Member[Identity]{Unit: &Everyone}},
			},
		},
		{
			I: "\n\n\n",
			O: nil,
		},
		{
			I: "alice " + peer1.String() +
				"\ngroup-a @alice\n\n",
			O: []Membership[Identity]{
				{Group: "alice", Member: Member[Identity]{Unit: &peer1}},
				{Group: "group-a", Member: Member[Identity]{GroupRef: ptr[GroupName]("alice")}},
			},
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			memberships, err := ParseIdentitiesFile(strings.NewReader(tc.I))
			require.NoError(t, err)
			require.Equal(t, tc.O, memberships)

			// also check that we can write it back and get the same result.
			buf := bytes.NewBuffer(nil)
			require.NoError(t, WriteIdentitiesFile(buf, memberships))
			memberships2, err := ParseIdentitiesFile(buf)
			require.NoError(t, err)
			require.Equal(t, memberships, memberships2)
		})
	}
}

func TestDefaultActionsFile(t *testing.T) {
	actions, err := ParseActionsFile(strings.NewReader(DefaultActionsFile()))
	require.NoError(t, err)
	require.NotEmpty(t, actions)
}

func TestParseActionsFile(t *testing.T) {
	tcs := []struct {
		I string
		O []Membership[Action]
	}{}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			actions, err := ParseActionsFile(strings.NewReader(tc.I))
			require.NoError(t, err)
			require.Equal(t, tc.O, actions)
		})
	}
}

func TestParseObjectsFile(t *testing.T) {
	tcs := []struct {
		I string
		O []Membership[ObjectSet]
	}{}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			objects, err := ParseObjectsFile(strings.NewReader(tc.I))
			require.NoError(t, err)
			require.Equal(t, tc.O, objects)
		})
	}
}

func TestParseGrantsFile(t *testing.T) {
	tcs := []struct {
		I string
		O []Grant
	}{
		{
			I: "@alice LOAD 00000000000000000000000000000000\n",
			O: []Grant{
				{
					Subject: GroupRef[Identity]("alice"),
					Verb:    Unit(Action_LOAD),
					Object:  Unit(ObjectSet{ByOID: ptr(blobcache.OID{})}),
				},
			},
		},
		{
			I: "@alice LOAD 00000000000000000000000000000000\n@bob SAVE 00000000000000000000000000000000\n",
			O: []Grant{
				{
					Subject: GroupRef[Identity]("alice"),
					Verb:    Unit(Action_LOAD),
					Object:  Unit(ObjectSet{ByOID: ptr(blobcache.OID{})}),
				},
				{
					Subject: GroupRef[Identity]("bob"),
					Verb:    Unit(Action_SAVE),
					Object:  Unit(ObjectSet{ByOID: ptr(blobcache.OID{})}),
				},
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			authz, err := ParseGrantsFile(strings.NewReader(tc.I))
			require.NoError(t, err)
			require.Equal(t, tc.O, authz)

			buf := bytes.NewBuffer(nil)
			require.NoError(t, WriteGrantsFile(buf, authz))
			authz2, err := ParseGrantsFile(buf)
			require.NoError(t, err)
			require.Equal(t, authz, authz2)
		})
	}
}

func mkPeerID(i int) blobcache.PeerID {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return blobcache.PeerID(blake3.Sum256(buf))
}
