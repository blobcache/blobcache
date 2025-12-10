// package gitrh provides a Server implementation of the Git remote helper IPC protocol
package gitrh

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"iter"
	"log"

	"blobcache.io/blobcache/src/bcsdk"
)

type Ref struct {
	Name   string
	Target [32]byte
}

func (gr Ref) String() string {
	return fmt.Sprintf("%s -> %x", gr.Name, gr.Target)
}

type Server struct {
	List  func(context.Context) iter.Seq2[Ref, error]
	Fetch func(ctx context.Context)
	Push  func(ctx context.Context, s bcsdk.RO, refs []Ref) error
}

func (srv *Server) Serve(ctx context.Context, r io.Reader, w io.Writer) error {
	log.Println("running protocol...")
	scn := bufio.NewScanner(r)
	bufw := bufio.NewWriter(w)

	for scn.Scan() {
		line := scn.Text()
		log.Println("git->:", line)
		switch line {
		case `capabilities`:
			for c, fn := range map[string]any{
				"list":  srv.List,
				"fetch": srv.Fetch,
				"push":  srv.Push,
			} {
				if fn == nil {
					continue
				}
				bufw.WriteString(c)
				bufw.WriteString("\n")
			}
		case `list`:
			for gr, err := range srv.List(ctx) {
				if err != nil {
					return err
				}
				fmt.Fprintf(bufw, "%x %s\n", gr.Target, gr.Name)
			}
		case ``:
			continue
		}
		bufw.WriteString("\n")
		bufw.Flush()
		// log.Println("<-done")
	}
	return bufw.Flush()
}
