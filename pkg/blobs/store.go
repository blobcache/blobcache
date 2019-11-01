package blobs

import (
	"context"
	"errors"
)

type Store interface {
	Getter
	Poster
	Deleter
}

type Getter interface {
	Get(context.Context, ID) (Blob, error)
	Exists(context.Context, ID) (bool, error)
}

type Poster interface {
	Post(context.Context, Blob) (ID, error)
}

type Deleter interface {
	Delete(context.Context, ID) error
}

var ErrTooMany = errors.New("prefix would take up more space than buffer")

type WithPrefix interface {
	// List fills ids with all the IDs under that prefix
	List(prefix []byte, ids []ID) error
}
