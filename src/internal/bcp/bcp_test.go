package bcp

import (
	"bytes"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"
)

func TestHeader(t *testing.T) {
	h := MessageHeader{}
	h.SetCode(MT_OPEN_FROM)
	h.SetBodyLen(10)

	require.Equal(t, h.Code(), MT_OPEN_FROM)
	require.Equal(t, h.BodyLen(), 10)
}

func TestMessageReadWrite(t *testing.T) {
	m := Message{}
	m.SetCode(MT_OPEN_FROM)
	m.SetBody([]byte("hello"))

	buf := &bytes.Buffer{}
	_, err := m.WriteTo(buf)
	require.NoError(t, err)

	m2 := Message{}
	_, err = m2.ReadFrom(buf)
	require.NoError(t, err)

	require.Equal(t, m2.Header().Code(), MT_OPEN_FROM)
	require.Equal(t, m2.Header().BodyLen(), len("hello"))
	require.Equal(t, string(m2.Body()), "hello")
}

func TestPermissionWireError(t *testing.T) {
	m := Message{}
	err0 := blobcache.ErrPermission{
		Handle: blobcache.Handle{OID: blobcache.OID{1}},
		Rights: 0x10,
		Requires: 0x20,
	}
	m.SetError(err0)
	require.Equal(t, MT_ERROR_NO_PERMISSION, m.Header().Code())

	err := parseWireError(m.Header().Code(), m.Body())
	var ep *blobcache.ErrPermission
	require.ErrorAs(t, err, &ep)
	require.Equal(t, err0.Rights, ep.Rights)
	require.Equal(t, err0.Requires, ep.Requires)
	require.Equal(t, err0.Handle, ep.Handle)
}
