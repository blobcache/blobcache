// Package blobstream provides readers and writers for the blobstream format.
package blobstream

import (
	"bufio"
	"encoding/binary"
	"io"
	"iter"

	"go.brendoncarroll.net/exp/sbe"
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

// AppendBytes appends the blobstream format to a slice of bytes.
func AppendBytes(out []byte, blobs iter.Seq[[]byte]) []byte {
	for blob := range blobs {
		out = binary.AppendUvarint(out, uint64(len(blob)))
		out = append(out, blob...)
	}
	return out
}

// ReadBytes interprets data as the blobstream format and returns a slice.
func ReadBytes(data []byte) ([][]byte, error) {
	var ret [][]byte
	for len(data) > 0 {
		blob, rest, err := sbe.ReadLP(data)
		if err != nil {
			return nil, err
		}
		ret = append(ret, blob)
		data = rest
	}
	return ret, nil
}
