// package gitrh provides a Server implementation of the Git remote helper IPC protocol
package gitrh

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
)

type Ref struct {
	Name   string
	Target [32]byte
	SHA1   [20]byte
}

func (gr Ref) String() string {
	return fmt.Sprintf("%s -> %x", gr.Name, gr.Target)
}

type Ctx struct {
	Remote string
	URL    string
}

func Main(fn func(Ctx) (*Server, error)) {
	remoteName, url := os.Args[1], os.Args[2]
	srv, err := fn(Ctx{Remote: remoteName, URL: url})
	if err != nil {
		log.Fatal(err)
		return
	}
	ctx := context.Background()
	if err := srv.Serve(ctx, os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

type Server struct {
	Remote string
	List   func(context.Context) iter.Seq2[Ref, error]
	Fetch  func(ctx context.Context, s *Store, toFetch map[string]blobcache.CID, updated map[string]blobcache.CID) error
	Push   func(ctx context.Context, s *Store, refs []Ref) error
}

func (srv *Server) Serve(ctx context.Context, r io.Reader, w io.Writer) error {
	log.Println("running protocol...")
	bufr := bufio.NewReader(r)
	bufw := bufio.NewWriter(w)

	toPush := []Ref{}
	toFetch := map[string]blobcache.CID{}
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
				"list":          srv.List != nil,
				"fetch":         srv.Fetch != nil,
				"push":          srv.Push != nil,
				"object-format": true,
				"option":        true,
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
			bufw.WriteString(":object-format sha256\n")
			for gr, err := range srv.List(ctx) {
				if err != nil {
					return err
				}
				bufw.WriteString(hex.EncodeToString(gr.Target[:]))
				bufw.WriteString(" ")
				bufw.WriteString(gr.Name)
				bufw.WriteString("\n")
			}
			bufw.WriteString("\n")
		case `push`:
			refSpec := args[0]
			refParts := strings.Split(refSpec, ":")
			src, dst := strings.TrimLeft(refParts[0], "+"), refParts[1]
			desired, err := getRefTarget(src)
			if err != nil {
				return err
			}
			ref := Ref{Name: dst, Target: desired}
			toPush = append(toPush, ref)
		case `fetch`:
			var h blobcache.CID
			if _, err := hex.Decode(h[:], []byte(args[0])); err != nil {
				return err
			}
			name := args[1]
			toFetch[name] = h
		case `option`:
			log.Println(args)
			bufw.WriteString("ok\n")
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
	} else if len(toFetch) > 0 {
		s := &Store{}
		updated := make(map[string]blobcache.CID)
		if err := srv.Fetch(ctx, s, toFetch, updated); err != nil {
			return err
		}
		// for name, target := range updated {
		// 	if err := setRefTarget(srv.Remote, name, target); err != nil {
		// 		return err
		// 	}
		// }
		log.Println("updated refs", len(updated))
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

// MaxSize is the maximum allowed size of an object.
const MaxSize = 1 << 23

type Store struct {
	SkipVerify bool
}

func (s *Store) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	cid := blobcache.CID(sha256.Sum256(data))
	if err := postObject(cid, data); err != nil {
		return blobcache.CID{}, err
	}
	return cid, nil
}

func (s *Store) Get(ctx context.Context, cid blobcache.CID, buf []byte) (int, error) {
	data, err := getObject(cid)
	if err != nil {
		return 0, err
	}
	if !s.SkipVerify {
		actualCID := blobcache.CID(sha256.Sum256(data))
		if actualCID != cid {
			return 0, fmt.Errorf("sha256 does not match")
		}
	}
	return copy(buf, data), nil
}

func (s *Store) MaxSize() int {
	return MaxSize
}

func (s *Store) Hash(x []byte) blobcache.CID {
	return sha256.Sum256(x)
}

func (s *Store) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	for i, cid := range cids {
		var err error
		if dst[i], err = objectExists(cid); err != nil {
			return err
		}
	}
	return nil
}

type Header struct {
	Type string
	Len  int
}

func SplitHeader(data []byte) (Header, []byte, error) {
	i := bytes.Index(data, []byte("\x00"))
	if i == -1 {
		return Header{}, nil, fmt.Errorf("no git header found")
	}
	hdrData := data[:i]
	rest := data[i+1:]
	parts := strings.Split(string(hdrData), " ")
	if len(parts) != 2 {
		return Header{}, nil, fmt.Errorf("could not parse type and length")
	}
	l, err := strconv.Atoi(parts[1])
	if err != nil {
		return Header{}, nil, err
	}
	return Header{
		Type: parts[0],
		Len:  l,
	}, rest, nil
}

// TypeOf returns the type of the GitObject in data.
func TypeOf(data []byte) (string, error) {
	eot := bytes.Index(data, []byte{' '})
	if eot == -1 {
		return "", fmt.Errorf("could not parse type")
	}
	return string(data[:eot]), nil
}

// func setRefTarget(remote, ref string, target blobcache.CID) error {
// 	c := exec.Command("git", "update-ref", ref, hex.EncodeToString(target[:]))
// 	c.Stderr = os.Stderr
// 	out, err := c.Output()
// 	if err != nil {
// 		return err
// 	}
// 	log.Println("set", ref, hex.EncodeToString(target[:]), string(out))
// 	return nil
// }

func postObject(cid blobcache.CID, data []byte) error {
	hexcid := hex.EncodeToString(cid[:])
	p := filepath.Join(".git/objects", hexcid[:2], hexcid[2:])
	if err := os.Mkdir(filepath.Dir(p), 0o755); err != nil && !os.IsExist(err) {
		return err
	}
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	log.Println("w", p)
	return os.WriteFile(p, buf.Bytes(), 0o444)
}

// func postObject(cid blobcache.CID, data []byte) error {
// 	hdr, rest, err := SplitHeader(data)
// 	if err != nil {
// 		return err
// 	}
// 	c := exec.Command("git", "hash-object", "-t", hdr.Type, "-w", "--stdin")
// 	c.Stdin = bytes.NewReader(rest)
// 	c.Stderr = os.Stderr
// 	out, err := c.Output()
// 	if err != nil {
// 		return err
// 	}
// 	out = bytes.TrimSpace(out)
// 	var gitid blobcache.CID
// 	if _, err := hex.Decode(gitid[:], out); err != nil {
// 		return err
// 	}
// 	if cid != gitid {
// 		return fmt.Errorf("git returned different hash %v vs. %v", cid, gitid)
// 	}
// 	log.Println("posted object", hex.EncodeToString(cid[:]))
// 	return err
// }

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

// objectExists calls into git to check it the object exist.
func objectExists(cid blobcache.CID) (bool, error) {
	c := exec.Command("git", "cat-file", "-e", hex.EncodeToString(cid[:]))
	if err := c.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			if ee.ExitCode() == 1 {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}
