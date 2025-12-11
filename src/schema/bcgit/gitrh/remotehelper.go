// package gitrh provides a Server implementation of the Git remote helper IPC protocol
package gitrh

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"iter"
	"log"
	"os/exec"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
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
	Push  func(ctx context.Context, s *Store, refs []Ref) error
	//Import func(ctx context.Context) error
	//Export func(ctx context.Context) error
}

func (srv *Server) Serve(ctx context.Context, r io.Reader, w io.Writer) error {
	log.Println("running protocol...")
	bufr := bufio.NewReader(r)
	bufw := bufio.NewWriter(w)

	toPush := []Ref{}
LOOP:
	for {
		cmd, args, err := readCmd(bufr)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch cmd {
		case `capabilities`:
			for c, fn := range map[string]bool{
				"list":  srv.List != nil,
				"fetch": srv.Fetch != nil,
				"push":  srv.Push != nil,
				//"import": srv.Import != nil,
				//"export": srv.Export != nil,
			} {
				if !fn {
					continue
				}
				log.Println("advertising", c)
				bufw.WriteString(c)
				bufw.WriteString("\n")
			}
			bufw.WriteString("\n")
		case `list`:
			for gr, err := range srv.List(ctx) {
				if err != nil {
					return err
				}
				fmt.Fprintf(bufw, "%x %s\n", gr.Target, gr.Name)
			}
			bufw.WriteString("\n")
		case `push`:
			refSpec := args[0]
			refParts := strings.Split(refSpec, ":")
			src, dst := refParts[0], refParts[1]
			desired, err := getRefTarget(src)
			if err != nil {
				return err
			}
			ref := Ref{Name: dst, Target: desired}
			toPush = append(toPush, ref)
		case ``:
			break LOOP
		default:
			return fmt.Errorf("unknown command: %q", cmd)
		}
		if err := bufw.Flush(); err != nil {
			return err
		}
	}
	if len(toPush) > 0 {
		if err := srv.Push(ctx, &Store{}, toPush); err != nil {
			return err
		}
		for _, ref := range toPush {
			printOk(bufw, ref.Name)
		}
		bufw.WriteString("\n")
	}
	return bufw.Flush()
}

func readCmd(bufr *bufio.Reader) (string, []string, error) {
	lineData, err := bufr.ReadBytes('\n')
	if err != nil {
		return "", nil, err
	}
	lineData = bytes.TrimSpace(lineData)
	line := string(lineData)
	log.Println("git->:", line)
	parts := strings.Split(line, " ")
	cmd := parts[0]
	var args []string
	if len(parts) > 1 {
		args = parts[1:]
	}
	return cmd, args, nil
}

func printOk(w io.Writer, refName string) error {
	_, err := fmt.Fprintf(w, "ok %s\n", refName)
	return err
}

func printError(w io.Writer, refName, msg string) error {
	_, err := fmt.Fprintf(w, "error %s %q\n", refName, msg)
	return err
}

func getRefTarget(name string) (blobcache.CID, error) {
	c := exec.Command("git", "rev-parse", name)
	stdout, err := c.Output()
	if err != nil {
		return blobcache.CID{}, err
	}
	hexData := bytes.Trim(stdout, "\n")
	var ret blobcache.CID
	if _, err := hex.Decode(ret[:], hexData); err != nil {
		return blobcache.CID{}, err
	}
	return ret, nil
}

// getObject returns the raw Git object bytes for the given hash.
func getObject(id blobcache.CID) ([]byte, error) {
	hexID := hex.EncodeToString(id[:])
	// First, determine the object type (commit, tree, blob, tag)
	typeCmd := exec.Command("git", "cat-file", "-t", hexID)
	typeOutput, err := typeCmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("git cat-file -t %s: %w (stderr: %s)",
				hexID, err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("git cat-file -t failed: %w", err)
	}

	objType := string(bytes.TrimSpace(typeOutput))

	// Now get the raw object contents using the correct type
	cmd := exec.Command("git", "cat-file", objType, hexID)
	rawData, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("git cat-file %s %s: %w (stderr: %s)",
				objType, hexID, err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("git cat-file %s %s failed: %w", objType, hexID, err)
	}
	var header []byte
	header = fmt.Appendf(header, "%s %d", objType, len(rawData))
	return slices.Concat(header, []byte("\x00"), rawData), nil
}

type Store struct {
	SkipVerify bool
}

func (s Store) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	data, err := getObject(cid)
	if err != nil {
		return 0, err
	}
	actualCID := blobcache.CID(sha256.Sum256(data))
	if actualCID != cid {
		return 0, fmt.Errorf("sha256 does not match")
	}
	return copy(buf, data), nil
}
