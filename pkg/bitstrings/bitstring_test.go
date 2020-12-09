package bitstrings

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAt(t *testing.T) {
	x := FromBytes(12, []byte("\x00\x01"))
	assert.Equal(t, x.At(0), false)
	assert.Equal(t, x.At(7), false)
	assert.Equal(t, x.At(15), true)
}

func TestString(t *testing.T) {
	assert.Equal(t, FromBytes(8, []byte("\xab")).String(), "ab")
	assert.Equal(t, FromBytes(12, []byte("\xab\x10")).String(), "ab.0001")
	assert.Equal(t, FromBytes(14, []byte("\xab\x0f")).String(), "ab.000011")
	assert.Equal(t, FromBytes(16, []byte("\xab\x0f")).String(), "ab0f")
}
