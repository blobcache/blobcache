package tries

import "fmt"

type PointerError struct {
	Name   string
	Ptr    int
	BufLen int
}

func (e *PointerError) Error() string {
	return fmt.Sprintf("pointer %s to %d in buffer of length %d", e.Name, e.Ptr, e.BufLen)
}
