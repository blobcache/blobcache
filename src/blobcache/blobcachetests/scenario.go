package blobcachetests

import (
	"context"
	"testing"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/exp/slices2"
)

// Volume describes how a Volume should be setup in a Scene
type Volume interface {
	isVolume()
}

// LocalVolume describes a Volume in a Scenario
type LocalVolume struct {
	Schema   blobcache.SchemaSpec
	Contents Contents
}

func (v *LocalVolume) isVolume() {}

func (v *LocalVolume) VolumeSpec() blobcache.VolumeSpec {
	spec := blobcache.DefaultLocalSpec()
	spec.Local.Schema = v.Schema
	return spec
}

type PeerVolume struct {
	Node   int
	Volume int
}

func (v *PeerVolume) isVolume() {}

// Contents are Volume contents
type Contents interface {
	// Fill fills a Volume.
	// tx is a transaction on the Volume being filled.
	// vols are handles to all of the volumes on the same Node as the Volume being filled.
	Fill(ctx context.Context, tx *bcsdk.Tx, vols []blobcache.Handle) error
}

var _ Contents = &Data{}

type Data struct {
	Cell []byte
}

func (d *Data) Fill(ctx context.Context, tx *bcsdk.Tx, vols []blobcache.Handle) error {
	if d == nil {
		return nil
	}
	return tx.Save(ctx, d.Cell)
}

type Node struct {
	Root    LocalVolume
	Volumes []Volume
}

// Scene describes an initial configuration of Nodes and Volumes
type Scene struct {
	Nodes []Node
}

func SetupScene(t testing.TB, scen Scene, nodes []blobcache.Service) {
	t.Helper()
	if len(nodes) != len(scen.Nodes) {
		t.Fatalf("cannot setup scenario, wrong number of nodes: got=%d want=%d", len(nodes), len(scen.Nodes))
	}

	nodeIDs := slices2.Map(nodes, func(x blobcache.Service) blobcache.NodeID {
		return Endpoint(t, x).Node
	})
	handles := make([][]blobcache.Handle, len(scen.Nodes))

	// Create local volumes first so peer volumes always have concrete targets.
	for nodeIdx, nodeDef := range scen.Nodes {
		node := nodes[nodeIdx]
		handles[nodeIdx] = make([]blobcache.Handle, len(nodeDef.Volumes))
		for volIdx, vol := range nodeDef.Volumes {
			local, ok := vol.(*LocalVolume)
			if !ok {
				continue
			}
			spec := blobcache.DefaultLocalSpec()
			spec.Local.Schema = local.Schema
			handles[nodeIdx][volIdx] = CreateVolume(t, node, nil, spec)
		}
	}
	// Create peer volumes.
	for nodeIdx, nspec := range scen.Nodes {
		svc := nodes[nodeIdx]
		for volIdx, vol := range nspec.Volumes {
			pv, ok := vol.(*PeerVolume)
			if !ok {
				continue
			}
			spec := blobcache.VolumeSpec{Peer: &blobcache.VolumeBackend_Peer{
				Peer:   nodeIDs[pv.Node],
				Volume: handles[pv.Node][pv.Volume].OID,
			}}
			handles[nodeIdx][volIdx] = CreateVolume(t, svc, nil, spec)
		}
	}

	// Set root contents
	ctx := testutil.Context(t)
	for nodeIdx, nspec := range scen.Nodes {
		node := nodes[nodeIdx]
		volh, err := node.OpenFiat(ctx, blobcache.OID{}, blobcache.Action_ALL)
		require.NoError(t, err)
		setContents(t, node, *volh, nspec.Root.Contents, handles[nodeIdx])
	}

	// Add contents to Volumes
	for nodeIdx, nspec := range scen.Nodes {
		node := nodes[nodeIdx]
		for volIdx, vol := range nspec.Volumes {
			local, ok := vol.(*LocalVolume)
			if !ok {
				continue
			}
			setContents(t, node, handles[nodeIdx][volIdx], local.Contents, handles[nodeIdx])
		}
	}
}

// setContents opens
func setContents(t testing.TB, svc blobcache.Service, volh blobcache.Handle, contents Contents, hs []blobcache.Handle) {
	if contents == nil {
		return
	}
	ctx := testutil.Context(t)
	tx, err := bcsdk.BeginTx(ctx, svc, volh, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	defer tx.Abort(ctx)
	require.NoError(t, contents.Fill(ctx, tx, hs))
	require.NoError(t, tx.Commit(ctx))
}
