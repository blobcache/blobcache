package blobcachecmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"

	bcclient "blobcache.io/blobcache/client/go"
	"blobcache.io/blobcache/src/blobcache"
)

var _ blobcache.Service = &Service{}

type Service struct {
	APIAddr  string
	ExecPath string
}

func (s *Service) Endpoint(ctx context.Context) (ep blobcache.Endpoint, _ error) {
	var out bytes.Buffer
	if err := s.run([]string{"endpoint"}, nil, &out); err != nil {
		return blobcache.Endpoint{}, err
	}
	return blobcache.ParseEndpoint(out.String())
}

// HandleAPI
func (s *Service) Drop(ctx context.Context, h blobcache.Handle) error {
	return s.run([]string{"drop", h.String()}, nil, nil)
}

func (s *Service) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	args := []string{"keepalive"}
	for _, h := range hs {
		args = append(args, h.String())
	}
	return s.run(args, nil, nil)
}

func (s *Service) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	var out bytes.Buffer
	if err := s.run([]string{"inspect-handle", h.String()}, nil, &out); err != nil {
		return nil, err
	}
	var hi blobcache.HandleInfo
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &hi); err != nil {
		return nil, err
	}
	return &hi, nil
}

func (s *Service) Share(ctx context.Context, h blobcache.Handle, to blobcache.PeerID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	re := regexp.MustCompile(`[A-F0-9]+\.[0-9a-f]+`)
	str, err := s.runParse([]string{"share", h.String(), to.String(), fmt.Sprint(uint64(mask))}, re)
	if err != nil {
		return nil, err
	}
	nh, err := blobcache.ParseHandle(str)
	if err != nil {
		return nil, err
	}
	return &nh, nil
}

// VolumeAPI
func (s *Service) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	// Use HTTP client for precise spec support (hash algo, sizes, etc.).
	cli := bcclient.NewClient(s.APIAddr)
	return cli.CreateVolume(ctx, host, vspec)
}

func (s *Service) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	var out bytes.Buffer
	if err := s.run([]string{"volume", "inspect", h.String()}, nil, &out); err != nil {
		return nil, err
	}
	var vi blobcache.VolumeInfo
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &vi); err != nil {
		return nil, err
	}
	return &vi, nil
}

// Backward-compat shim to satisfy any stale interface checks
func (s *Service) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	re := regexp.MustCompile(`[A-F0-9]+\.[0-9a-f]+`)
	str, err := s.runParse([]string{"open-as", x.String(), fmt.Sprint(uint64(mask))}, re)
	if err != nil {
		return nil, err
	}
	h, err := blobcache.ParseHandle(str)
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func (s *Service) OpenAs(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return s.OpenFiat(ctx, x, mask)
}

func (s *Service) OpenFrom(ctx context.Context, base blobcache.Handle, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	re := regexp.MustCompile(`[A-F0-9]+\.[0-9a-f]+`)
	str, err := s.runParse([]string{"open-from", base.String(), x.String(), fmt.Sprint(uint64(mask))}, re)
	if err != nil {
		return nil, err
	}
	h, err := blobcache.ParseHandle(str)
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func (s *Service) Await(ctx context.Context, cond blobcache.Conditions) error {
	data, err := json.Marshal(cond)
	if err != nil {
		return err
	}
	return s.run([]string{"await"}, data, nil)
}

func (s *Service) BeginTx(ctx context.Context, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	args := []string{"begin", volh.String()}
	if txp.Mutate {
		args = append(args, "--mutate")
	}
	if txp.GC {
		args = append(args, "--gc")
	}
	re := regexp.MustCompile(`[A-F0-9]+\.[0-9a-f]+`)
	str, err := s.runParse(args, re)
	if err != nil {
		return nil, err
	}
	h, err := blobcache.ParseHandle(str)
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func (s *Service) CloneVolume(ctx context.Context, caller *blobcache.PeerID, volh blobcache.Handle) (*blobcache.Handle, error) {
	re := regexp.MustCompile(`[A-F0-9]+\.[0-9a-f]+`)
	str, err := s.runParse([]string{"volume", "clone", volh.String()}, re)
	if err != nil {
		return nil, err
	}
	h, err := blobcache.ParseHandle(str)
	if err != nil {
		return nil, err
	}
	_ = caller // currently unused by CLI
	return &h, nil
}

// TxAPI
func (s *Service) Abort(ctx context.Context, h blobcache.Handle) error {
	return s.run([]string{"tx", "abort", h.String()}, nil, nil)
}

func (s *Service) Commit(ctx context.Context, h blobcache.Handle) error {
	return s.run([]string{"tx", "commit", h.String()}, nil, nil)
}

func (s *Service) Load(ctx context.Context, h blobcache.Handle, dst *[]byte) error {
	var out bytes.Buffer
	if err := s.run([]string{"tx", "load", h.String()}, nil, &out); err != nil {
		return err
	}
	*dst = append((*dst)[:0], out.Bytes()...)
	return nil
}

func (s *Service) Save(ctx context.Context, h blobcache.Handle, src []byte) error {
	return s.run([]string{"tx", "save", h.String()}, src, nil)
}

func (s *Service) InspectTx(ctx context.Context, h blobcache.Handle) (*blobcache.TxInfo, error) {
	var out bytes.Buffer
	if err := s.run([]string{"tx", "inspect", h.String()}, nil, &out); err != nil {
		return nil, err
	}
	var ti blobcache.TxInfo
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &ti); err != nil {
		return nil, err
	}
	return &ti, nil
}

func (s *Service) Post(ctx context.Context, h blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	var out bytes.Buffer
	if err := s.run([]string{"tx", "post", h.String()}, data, &out); err != nil {
		return blobcache.CID{}, err
	}
	// Extract base64 CID from output like "CID: <base64>" or bare base64
	cidRe := regexp.MustCompile(`[A-Za-z0-9\-_=]{40,}`)
	m := cidRe.Find(out.Bytes())
	if m == nil {
		return blobcache.CID{}, fmt.Errorf("could not parse CID from output: %q", out.String())
	}
	return blobcache.ParseCID(string(m))
}

func (s *Service) Get(ctx context.Context, h blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	var out bytes.Buffer
	if err := s.run([]string{"tx", "get", h.String(), cid.String()}, nil, &out); err != nil {
		return 0, err
	}
	n := copy(buf, out.Bytes())
	return n, nil
}

func (s *Service) Exists(ctx context.Context, h blobcache.Handle, cids []blobcache.CID, dst []bool) error {
	args := []string{"tx", "exists", h.String()}
	for _, cid := range cids {
		args = append(args, cid.String())
	}
	var out bytes.Buffer
	if err := s.run(args, nil, &out); err != nil {
		return err
	}
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte{'\n'})
	idx := 0
	for _, ln := range lines {
		if len(ln) == 0 {
			continue
		}
		if bytes.HasSuffix(ln, []byte(" YES")) {
			dst[idx] = true
			idx++
		} else if bytes.HasSuffix(ln, []byte(" NO")) {
			dst[idx] = false
			idx++
		}
	}
	return nil
}

func (s *Service) Delete(ctx context.Context, h blobcache.Handle, cids []blobcache.CID) error {
	args := []string{"tx", "delete", h.String()}
	for _, cid := range cids {
		args = append(args, cid.String())
	}
	return s.run(args, nil, nil)
}

func (s *Service) Copy(ctx context.Context, h blobcache.Handle, cids []blobcache.CID, srcTxns []blobcache.Handle, success []bool) error {
	// optional, not implemented by CLI currently
	return fmt.Errorf("copy not implemented")
}

func (s *Service) AllowLink(ctx context.Context, h blobcache.Handle, subvol blobcache.Handle) error {
	return s.run([]string{"tx", "allow-link", h.String(), subvol.String()}, nil, nil)
}

func (s *Service) Visit(ctx context.Context, h blobcache.Handle, cids []blobcache.CID) error {
	args := []string{"tx", "visit", h.String()}
	for _, cid := range cids {
		args = append(args, cid.String())
	}
	return s.run(args, nil, nil)
}

func (s *Service) IsVisited(ctx context.Context, h blobcache.Handle, cids []blobcache.CID, yesVisited []bool) error {
	args := []string{"tx", "is-visited", h.String()}
	for _, cid := range cids {
		args = append(args, cid.String())
	}
	var out bytes.Buffer
	if err := s.run(args, nil, &out); err != nil {
		return err
	}
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte{'\n'})
	idx := 0
	for _, ln := range lines {
		if len(ln) == 0 {
			continue
		}
		if bytes.HasSuffix(ln, []byte(" YES")) {
			yesVisited[idx] = true
			idx++
		} else if bytes.HasSuffix(ln, []byte(" NO")) {
			yesVisited[idx] = false
			idx++
		}
	}
	return nil
}

func (s *Service) runParse(args []string, re *regexp.Regexp) (string, error) {
	var out bytes.Buffer
	if err := s.run(args, nil, &out); err != nil {
		return "", err
	}
	if !re.Match(out.Bytes()) {
		return "", fmt.Errorf("output did not match regex: %s", re.String())
	}
	return re.FindString(out.String()), nil
}

// run runs a command and returns the output.
func (s *Service) run(args []string, in []byte, out *bytes.Buffer) error {
	log.Println("running:", s.ExecPath, args)
	cmd := exec.Command(s.ExecPath, args...)
	cmd.Env = []string{
		bcclient.EnvBlobcacheAPI + "=" + s.APIAddr,
	}
	cmd.Stdin = bytes.NewReader(in)
	// Only set Stdout if a non-nil buffer is provided. Assigning a typed-nil
	// io.Writer (e.g. (*bytes.Buffer)(nil)) causes os/exec to attempt writes
	// into a nil receiver, leading to a panic in bytes.Buffer.ReadFrom.
	if out != nil {
		cmd.Stdout = out
	} else {
		cmd.Stdout = io.Discard
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}
