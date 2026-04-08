// Package blobstream provides readers and writers for the blobstream format.
package blobstream

import (
	"bufio"
	"encoding/binary"
	"io"
	"iter"
)

// WriteStream writes a sequence of blobs to Writer in the blobstream format.
func WriteStream(w io.Writer, blobs iter.Seq[[]byte]) error {
	bw := bufio.NewWriter(w)
	for blob := range blobs {
		var lenBuf [binary.MaxVarintLen32]byte
		n := binary.PutUvarint(lenBuf[:], uint64(len(blob)))
		if _, err := bw.Write(lenBuf[:n]); err != nil {
			return err
		}
		if _, err := bw.Write(blob); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ReadStream iterates over a blobstream
func ReadStream(r io.Reader) iter.Seq2[[]byte, error] {
	bufR := bufio.NewReader(r)
	return func(yield func([]byte, error) bool) {
		for {
			length, err := binary.ReadUvarint(bufR)
			if err != nil {
				if err == io.EOF {
					return
				}
				yield(nil, err)
				return
			}
			blob := make([]byte, length)
			if _, err := io.ReadFull(bufR, blob); err != nil {
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				yield(nil, err)
				return
			}
			if !yield(blob, nil) {
				return
			}
		}
	}
}
