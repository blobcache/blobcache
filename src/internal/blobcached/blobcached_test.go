package blobcached

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/bchttp"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

func TestRun(t *testing.T) {
	ctx := testutil.Context(t)
	ctx, cf := context.WithCancel(ctx)
	dir := t.TempDir()

	done := make(chan struct{})
	lis := testutil.Listen(t)
	go func() {
		err := Run(ctx, dir, nil, lis)
		require.NoError(t, err)
		close(done)
	}()
	hc := bchttp.NewClient(nil, "http://"+lis.Addr().String())

	t.Log("awaiting healthy")
	require.NoError(t, AwaitHealthy(ctx, hc))
	t.Log("shutting down")

	cf()
	<-done
	t.Log("done")
}

func TestParseAuthnFile(t *testing.T) {
	peer1 := mkPeerID(1)
	tcs := []struct {
		I string
		O []Membership
	}{
		{
			I: "alice " + peer1.String() + "\n\n",
			O: []Membership{{Group: "alice", Member: Identity{Peer: &peer1}}},
		},
		{
			I: "\n\n\n",
			O: nil,
		},
		{
			I: "alice " + peer1.String() +
				"\ngroup-a @alice\n\n",
			O: []Membership{
				{Group: "alice", Member: Identity{Peer: &peer1}},
				{Group: "group-a", Member: Identity{Name: ptr("alice")}},
			},
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			memberships, err := ParseAuthnFile(strings.NewReader(tc.I))
			require.NoError(t, err)
			require.Equal(t, tc.O, memberships)

			// also check that we can write it back and get the same result.
			buf := bytes.NewBuffer(nil)
			require.NoError(t, WriteAuthnFile(buf, memberships))
			memberships2, err := ParseAuthnFile(buf)
			require.NoError(t, err)
			require.Equal(t, memberships, memberships2)
		})
	}
}

func TestParseAuthzFile(t *testing.T) {
	tcs := []struct {
		I string
		O []Grant
	}{
		{
			I: "@alice LOOK .+\n",
			O: []Grant{
				{
					Subject: Identity{Name: ptr("alice")},
					Verb:    Action_LOOK,
					Object:  Object{NameSet: regexp.MustCompile(".+")},
				},
			},
		},
		{
			I: "@alice LOOK .+\n@bob TOUCH .+\n",
			O: []Grant{
				{
					Subject: Identity{Name: ptr("alice")},
					Verb:    Action_LOOK,
					Object:  Object{NameSet: regexp.MustCompile(".+")},
				},
				{
					Subject: Identity{Name: ptr("bob")},
					Verb:    Action_TOUCH,
					Object:  Object{NameSet: regexp.MustCompile(".+")},
				},
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			authz, err := ParseAuthzFile(strings.NewReader(tc.I))
			require.NoError(t, err)
			require.Equal(t, tc.O, authz)

			buf := bytes.NewBuffer(nil)
			require.NoError(t, WriteAuthzFile(buf, authz))
			authz2, err := ParseAuthzFile(buf)
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
