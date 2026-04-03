package blobstream

import (
	"bytes"
	"encoding/binary"
	"io"
	"slices"
	"testing"
)

func TestReadWriteStream(t *testing.T) {
	blobs := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("this is a test"),
		[]byte(""),
		[]byte("end"),
	}

	var buf bytes.Buffer
	seq := slices.Values(blobs)
	if err := WriteStream(&buf, seq); err != nil {
		t.Fatalf("WriteStream failed: %v", err)
	}

	readSeq := ReadStream(&buf)
	var readBlobs [][]byte
	for b, err := range readSeq {
		if err != nil {
			t.Fatalf("ReadStream failed: %v", err)
		}
		readBlobs = append(readBlobs, b)
	}

	if len(readBlobs) != len(blobs) {
		t.Fatalf("expected %d blobs, got %d", len(blobs), len(readBlobs))
	}
	for i := range blobs {
		if !bytes.Equal(blobs[i], readBlobs[i]) {
			t.Errorf("expected %q, got %q", blobs[i], readBlobs[i])
		}
	}
}

func TestReadStreamUnexpectedEOF(t *testing.T) {
	var buf bytes.Buffer
	var lenBuf [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(lenBuf[:], 10) // length 10
	buf.Write(lenBuf[:n])
	buf.Write([]byte("short")) // length 5

	seq := ReadStream(&buf)
	var errOut error
	for _, err := range seq {
		if err != nil {
			errOut = err
		}
	}
	if errOut != io.ErrUnexpectedEOF {
		t.Errorf("expected ErrUnexpectedEOF, got %v", errOut)
	}
}

func TestReadStreamUnexpectedEOF2(t *testing.T) {
	var buf bytes.Buffer
	var lenBuf [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(lenBuf[:], 10) // length 10
	buf.Write(lenBuf[:n])
	// no blob data at all

	seq := ReadStream(&buf)
	var errOut error
	for _, err := range seq {
		if err != nil {
			errOut = err
		}
	}
	if errOut != io.ErrUnexpectedEOF {
		t.Errorf("expected ErrUnexpectedEOF, got %v", errOut)
	}
}
