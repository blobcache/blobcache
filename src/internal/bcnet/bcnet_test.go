package bcnet

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeader(t *testing.T) {
	h := MessageHeader{}
	h.SetCode(MT_NAMESPACE_OPEN)
	h.SetBodyLen(10)

	require.Equal(t, h.Code(), MT_NAMESPACE_OPEN)
	require.Equal(t, h.BodyLen(), 10)
}

func TestMessageReadWrite(t *testing.T) {
	m := Message{}
	m.SetCode(MT_NAMESPACE_OPEN)
	m.SetBody([]byte("hello"))

	buf := &bytes.Buffer{}
	_, err := m.WriteTo(buf)
	require.NoError(t, err)

	m2 := Message{}
	_, err = m2.ReadFrom(buf)
	require.NoError(t, err)

	require.Equal(t, m2.Header().Code(), MT_NAMESPACE_OPEN)
	require.Equal(t, m2.Header().BodyLen(), len("hello"))
	require.Equal(t, string(m2.Body()), "hello")
}
