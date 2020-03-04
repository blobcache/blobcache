package tries

import (
	"errors"
	"fmt"
)

var (
	ErrPrefixNotParent  = errors.New("prefix is not parent in trie")
	ErrCannotDeleteRoot = errors.New("cannot delete trie root")
	ErrBranchEmpty      = errors.New("branch is empty")
)

type PointerError struct {
	Name string
	Ptr  int

	Buf []byte
}

func (e *PointerError) Error() string {
	return fmt.Sprintf("pointer %s to %d in buffer of length %d", e.Name, e.Ptr, len(e.Buf))
}
