package groupfile

import "fmt"

type ErrParsing struct {
	FileData []byte
	Beg, End uint32
	Err      error
}

func (e ErrParsing) Error() string {
	input := e.FileData[e.Beg:e.End]
	return fmt.Sprintf("could not parse groups file:\n%q\n%s", input, e.Err.Error())
}
