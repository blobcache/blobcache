package fsbridge

import (
	"io"
)

type Chunker interface {
	Next() (Chunk, error)
}

type Chunk struct {
	Offset int64
	Data   []byte
}

type FixedSizeChunker struct {
	r    io.Reader
	size int

	offset int64
}

func (c *FixedSizeChunker) Next() (chunk Chunk, err error) {
	if c.size == 0 {
		panic("cannot make 0 sized chunks")
	}

	buf := make([]byte, c.size)
	total := 0
	offset := c.offset

	for total < len(buf) {
		n, err := c.r.Read(buf)
		total += n
		c.offset += int64(n)

		if err == io.EOF {
			return Chunk{
				Offset: offset,
				Data:   buf[:total],
			}, io.EOF
		}
		if err != nil {
			return chunk, err
		}
	}

	return Chunk{Offset: offset, Data: buf}, err
}
