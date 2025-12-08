// package gitrh provides a Server implementation of the Git remote helper IPC protocol
package gitrh

import (
	"bufio"
	"fmt"
	"io"
	"iter"
	"log"
)

type Ref struct {
	Name   string
	Target [32]byte
}

type Server struct {
	// list
	// push
	// fetch
	Capabilities []string

	List func() iter.Seq[Ref]
}

func (srv *Server) Serve(r io.Reader, w io.Writer) error {
	log.Println("running protocol...")
	scn := bufio.NewScanner(r)
	bufw := bufio.NewWriter(w)
LOOP:
	for scn.Scan() {
		line := scn.Text()
		log.Println("git->:", line)
		switch line {
		case `capabilities`:
			for _, c := range srv.Capabilities {
				bufw.WriteString(c)
				bufw.WriteString("\n")
			}
		case `list`:
			for gr := range srv.List() {
				fmt.Fprintf(bufw, "%x %s\n", gr.Target, gr.Name)
			}
		case ``:
			break LOOP
		}
		bufw.WriteString("\n")
		bufw.Flush()
		// log.Println("<-done")
	}
	return bufw.Flush()
}
