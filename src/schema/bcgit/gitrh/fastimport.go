package gitrh

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
)

type FastImporter struct {
	Store

	mu      sync.RWMutex
	cmd     *exec.Cmd
	stdin   *io.PipeWriter
	bw      *bufio.Writer
	written map[blobcache.CID]struct{}
}

func newFastImporter() (*FastImporter, error) {
	pr, pw := io.Pipe()
	cmd := exec.Command("git", "fast-import")
	cmd.Stdin = pr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &FastImporter{
		cmd:     cmd,
		stdin:   pw,
		bw:      bufio.NewWriter(pw),
		written: make(map[blobcache.CID]struct{}),
	}, nil
}

func (s *FastImporter) Exists(ctx context.Context, cids []blobcache.CID, dst []bool) error {
	// TODO: check map here.
	return s.Store.Exists(ctx, cids, dst)
}

func (s *FastImporter) Post(ctx context.Context, data []byte) (blobcache.CID, error) {
	cid := blobcache.CID(sha256.Sum256(data))
	s.mu.RLock()
	if _, exists := s.written[cid]; exists {
		s.mu.RUnlock()
		return cid, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.sendRaw(data); err != nil {
		return blobcache.CID{}, err
	}
	s.bw.Flush()
	return cid, nil
}

func (s *FastImporter) Close() error {
	if err := s.bw.Flush(); err != nil {
		return err
	}
	if err := s.stdin.Close(); err != nil {
		return err
	}
	return s.cmd.Wait()
}

func (s *FastImporter) sendRaw(data []byte) error {
	hdr, rest, err := SplitHeader(data)
	if err != nil {
		return err
	}
	switch hdr.Type {
	case "tree":
		return s.sendTyped(hdr.Type, rest)
	case "blob":
		return s.sendTyped(hdr.Type, rest)
	default:
		return fmt.Errorf("fast importer: don't know how to write type %v", hdr.Type)
	}
}

func (s *FastImporter) sendTyped(ty string, data []byte) error {
	s.bw.WriteString(ty + "\n")
	fmt.Fprintf(s.bw, "data %d\n", len(data))
	s.bw.Write(data)
	s.bw.WriteString("\n")
	return nil
}
