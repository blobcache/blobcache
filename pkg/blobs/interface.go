package blobs

import (
	"context"
	"errors"
)

type Store interface {
	GetPostDelete
	Lister
}

type GetPostDelete interface {
	Getter
	Poster
	Deleter
}

type Getter interface {
	GetF(context.Context, ID, func(Blob) error) error
	Exists(context.Context, ID) (bool, error)
}

type Poster interface {
	Post(context.Context, Blob) (ID, error)
}

type Deleter interface {
	Delete(context.Context, ID) error
}

type Lister interface {
	// List fills ids with all the IDs under that prefix
	List(ctx context.Context, prefix []byte, ids []ID) (n int, err error)
}

var (
	ErrTooMany  = errors.New("prefix would take up more space than buffer")
	ErrNotFound = errors.New("blob no found")
)
