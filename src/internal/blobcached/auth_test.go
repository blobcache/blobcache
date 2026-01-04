package blobcached

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/groupfile"
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
			ents, err := ParseIdentitiesFile([]byte(tc.I))
			require.NoError(t, err)
			require.Equal(t, tc.O, mFromE(ents))

			// also check that we can write it back and get the same result.
			buf := bytes.NewBuffer(nil)
			require.NoError(t, WriteIdentitiesFile(buf, ents))
			ents2, err := ParseIdentitiesFile(buf.Bytes())
			require.NoError(t, err)
			require.Equal(t, ents, ents2)
		})
	}
}

func TestDefaultActionsFile(t *testing.T) {
	actions, err := ParseActionsFile([]byte(DefaultActionsFile()))
	require.NoError(t, err)
	require.NotEmpty(t, actions)
}

func Unit[T any](x T) Member[T] {
	return groupfile.Unit[GroupName, T](x)
}

func GroupRef[T any](x GroupName) Member[T] {
	return groupfile.GroupRef[GroupName, T](x)
}

func TestParseActionsFile(t *testing.T) {
	tcs := []struct {
		I string
		O []Membership[Action]
	}{
		{
			I: "all LOAD SAVE\n\n",
			O: []Membership[Action]{
				{Group: "all", Member: Unit(Action_LOAD)},
				{Group: "all", Member: Unit(Action_SAVE)},
			},
		},
		{
			I: "look LOAD GET\n\n" +
				"touch @look SAVE\n\n",
			O: []Membership[Action]{
				{Group: "look", Member: Unit(Action_LOAD)},
				{Group: "look", Member: Unit(Action_GET)},
				{Group: "touch", Member: GroupRef[Action]("look")},
				{Group: "touch", Member: Unit(Action_SAVE)},
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ents, err := ParseActionsFile([]byte(tc.I))
			require.NoError(t, err)
			memberships2 := mFromE(ents)
			require.Equal(t, tc.O, memberships2)
		})
	}
}

func TestParseObjectsFile(t *testing.T) {
	tcs := []struct {
		I string
		O []Membership[ObjectSet]
	}{
		{
			I: "zero 00000000000000000000000000000000\n\n",
			O: []Membership[ObjectSet]{
				{Group: "zero", Member: Unit(ObjectSet{ByOID: ptr(blobcache.OID{})})},
			},
		},
		{
			I: "zero 00000000000000000000000000000000\n" +
				"vols @zero\n\n",
			O: []Membership[ObjectSet]{
				{Group: "zero", Member: Unit(ObjectSet{ByOID: ptr(blobcache.OID{})})},
				{Group: "vols", Member: GroupRef[ObjectSet]("zero")},
			},
		},
		{
			I: "all ALL\n\n",
			O: []Membership[ObjectSet]{
				{Group: "all", Member: Unit(ObjectSet{All: &struct{}{}})},
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ents, err := ParseObjectsFile([]byte(tc.I))
			require.NoError(t, err)
			objects := mFromE(ents)
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
					Action:  Unit(Action_LOAD),
					Object:  Unit(ObjectSet{ByOID: ptr(blobcache.OID{})}),
				},
			},
		},
		{
			I: "@alice LOAD 00000000000000000000000000000000\n@bob SAVE 00000000000000000000000000000000\n",
			O: []Grant{
				{
					Subject: GroupRef[Identity]("alice"),
					Action:  Unit(Action_LOAD),
					Object:  Unit(ObjectSet{ByOID: ptr(blobcache.OID{})}),
				},
				{
					Subject: GroupRef[Identity]("bob"),
					Action:  Unit(Action_SAVE),
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

func TestPolicy(t *testing.T) {
	peer1 := mkPeerID(1)
	vol1 := mkVolOID(1)

	type Check struct {
		Peer       blobcache.PeerID
		Target     blobcache.OID
		CanConnect bool
		Open       blobcache.ActionSet
		CanCreate  bool
	}

	tcs := []struct {
		Name    string
		Idens   []Membership[Identity]
		Actions []Membership[Action]
		Objects []Membership[ObjectSet]
		Grants  []Grant

		Checks []Check
	}{
		{
			Name: "no-grants",
			Idens: []Membership[Identity]{
				{Group: "alice", Member: Unit(peer1)},
			},
			Actions: nil,
			Objects: nil,
			Grants:  nil,
			Checks: []Check{
				{Peer: peer1, Target: vol1, CanConnect: false, Open: 0, CanCreate: false},
			},
		},
		{
			Name:    "anyone-get-on-vol1",
			Idens:   nil,
			Actions: nil,
			Objects: nil,
			Grants: []Grant{
				{Subject: Unit(Everyone), Action: Unit(Action_GET), Object: Unit(ObjectSet{ByOID: &vol1})},
			},
			Checks: []Check{
				{Peer: peer1, Target: vol1, CanConnect: true, Open: blobcache.Action_TX_GET, CanCreate: false},
			},
		},
		{
			Name: "grouped-actions-closure-and-create",
			Idens: []Membership[Identity]{
				{Group: "alice", Member: Unit(peer1)},
			},
			Actions: []Membership[Action]{
				{Group: "look", Member: Unit(Action_GET)},
				{Group: "touch", Member: GroupRef[Action]("look")},
				{Group: "touch", Member: Unit(Action_SAVE)},
				{Group: "touch", Member: Unit(Action_CREATE)},
			},
			Objects: []Membership[ObjectSet]{
				{Group: "vols", Member: Unit(ObjectSet{ByOID: &vol1})},
			},
			Grants: []Grant{
				{Subject: GroupRef[Identity]("alice"), Action: GroupRef[Action]("touch"), Object: GroupRef[ObjectSet]("vols")},
			},
			Checks: []Check{
				{
					Peer:       peer1,
					Target:     vol1,
					CanConnect: true,
					Open:       blobcache.Action_TX_GET | blobcache.Action_TX_SAVE,
					CanCreate:  true,
				},
			},
		},
		{
			Name: "admin-all-all",
			Idens: []Membership[Identity]{
				{Group: "admin", Member: Unit(peer1)},
			},
			Actions: []Membership[Action]{
				{Group: "all", Member: Unit(Action_LOAD)},
				{Group: "all", Member: Unit(Action_SAVE)},
				{Group: "all", Member: Unit(Action_POST)},
				{Group: "all", Member: Unit(Action_GET)},
				{Group: "all", Member: Unit(Action_EXISTS)},
				{Group: "all", Member: Unit(Action_DELETE)},
				{Group: "all", Member: Unit(Action_COPY_FROM)},
				{Group: "all", Member: Unit(Action_COPY_TO)},
				{Group: "all", Member: Unit(Action_LINK_FROM)},
				{Group: "all", Member: Unit(Action_LINK_TO)},
				{Group: "all", Member: Unit(Action_UNLINK_FROM)},
				{Group: "all", Member: Unit(Action_CLONE)},
				{Group: "all", Member: Unit(Action_CREATE)},
			},
			Objects: []Membership[ObjectSet]{
				{Group: "all", Member: Unit(ObjectSet{ByOID: &vol1})},
			},
			Grants: []Grant{
				{Subject: GroupRef[Identity]("admin"), Action: GroupRef[Action]("all"), Object: GroupRef[ObjectSet]("all")},
			},
			Checks: []Check{
				{
					Peer:       peer1,
					Target:     vol1,
					CanConnect: true,
					Open: blobcache.Action_TX_LOAD | blobcache.Action_TX_SAVE | blobcache.Action_TX_POST |
						blobcache.Action_TX_GET | blobcache.Action_TX_EXISTS | blobcache.Action_TX_DELETE |
						blobcache.Action_TX_COPY_FROM | blobcache.Action_TX_COPY_TO |
						blobcache.Action_TX_LINK_FROM | blobcache.Action_TX_UNLINK_FROM | blobcache.Action_VOLUME_CLONE,
					CanCreate: true,
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			p, err := NewPolicy(tc.Idens, tc.Actions, tc.Objects, tc.Grants)
			require.NoError(t, err)
			for _, check := range tc.Checks {
				require.Equal(t, check.CanConnect, p.CanConnect(check.Peer))
				rights := p.OpenFiat(check.Peer, check.Target)
				require.Equal(t, check.Open, rights)
				require.Equal(t, check.CanCreate, p.CanCreate(check.Peer))
			}
		})
	}
}

func mkVolOID(i int) blobcache.OID {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	h := blake3.Sum256(buf)
	return blobcache.OID(h[:blobcache.OIDSize])
}
