package bctui

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/schema/bcns"
	_ "blobcache.io/blobcache/src/schema/jsonns"
	mlog "blobcache.io/blobcache/src/schema/merklelog"
)

var errNoSelection = errors.New("no row selected")

type component interface {
	SchemaName() blobcache.SchemaName
	SetFilter(filter string)
	MoveCursor(delta int)
	OpenSelected(ctx context.Context) (name string, next blobcache.Handle, ok bool, err error)
	CopySelected() (string, error)
	RenderRows(width, height int, focused bool) []string
}

func loadComponent(ctx context.Context, svc blobcache.Service, h blobcache.Handle) (component, error) {
	vinfo, err := svc.InspectVolume(ctx, h)
	if err != nil {
		return nil, err
	}

	if vinfo.Schema.Name == "" {
		return loadNoneComponent(ctx, svc, h, vinfo.Schema.Name)
	}

	if vinfo.Schema.Name == mlog.SchemaName {
		return loadMerklelogComponent(ctx, svc, h, vinfo.Schema.Name)
	}

	sch, err := schemareg.Factory(vinfo.Schema)
	if err == nil {
		if _, ok := sch.(bcns.Namespace); ok {
			return loadNamespaceComponent(ctx, svc, h, vinfo.Schema.Name)
		}
	}

	fqoid := componentFQOID(ctx, svc, h, vinfo)
	return newMessageComponent(
		vinfo.Schema.Name,
		fmt.Sprintf("unsupported schema %q (%s)", vinfo.Schema.Name, fqoid),
	), nil
}

func loadNoneComponent(ctx context.Context, svc blobcache.Service, h blobcache.Handle, schemaName blobcache.SchemaName) (component, error) {
	dump, err := bcsdk.View1(ctx, svc, h, func(_ bcsdk.RO, root []byte) (string, error) {
		header := fmt.Sprintf("CELL DATA (%d bytes):", len(root))
		return header + "\n" + strings.TrimSuffix(hex.Dump(root), "\n"), nil
	})
	if err != nil {
		return nil, err
	}
	return newMessageComponent(schemaName, strings.Split(dump, "\n")...), nil
}

func componentFQOID(ctx context.Context, svc blobcache.Service, h blobcache.Handle, vinfo *blobcache.VolumeInfo) string {
	if vinfo != nil && vinfo.Backend.Remote != nil {
		return fmt.Sprintf("%s:%s", vinfo.Backend.Remote.Endpoint.Peer.String(), vinfo.Backend.Remote.Volume.String())
	}
	ep, err := svc.Endpoint(ctx)
	if err != nil {
		return h.OID.String()
	}
	return fmt.Sprintf("%s:%s", ep.Peer.String(), h.OID.String())
}

func openNamespaceEntryByName(ctx context.Context, svc blobcache.Service, nsHandle blobcache.Handle, name string) (blobcache.Handle, error) {
	nsc, err := bcns.ClientForVolume(ctx, svc, nsHandle)
	if err != nil {
		return blobcache.Handle{}, err
	}
	var ent bcns.Entry
	found, err := nsc.Get(ctx, nsHandle, name, &ent)
	if err != nil {
		return blobcache.Handle{}, err
	}
	if !found {
		return blobcache.Handle{}, fmt.Errorf("namespace entry %q not found", name)
	}
	next, err := svc.OpenFrom(ctx, nsHandle, ent.LinkToken(), blobcache.Action_ALL)
	if err != nil {
		return blobcache.Handle{}, err
	}
	return *next, nil
}

type namespaceComponent struct {
	schemaName blobcache.SchemaName
	svc        blobcache.Service
	base       blobcache.Handle
	entries    []bcns.Entry

	filter   string
	filtered []int
	cursor   int
}

func loadNamespaceComponent(ctx context.Context, svc blobcache.Service, h blobcache.Handle, schemaName blobcache.SchemaName) (component, error) {
	nsc, err := bcns.ClientForVolume(ctx, svc, h)
	if err != nil {
		return nil, err
	}
	ents, err := nsc.List(ctx, h)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(ents, func(a, b bcns.Entry) int {
		return strings.Compare(a.Name, b.Name)
	})
	c := &namespaceComponent{
		schemaName: schemaName,
		svc:        svc,
		base:       h,
		entries:    ents,
	}
	c.rebuildFilter()
	return c, nil
}

func (c *namespaceComponent) SchemaName() blobcache.SchemaName {
	return c.schemaName
}

func (c *namespaceComponent) SetFilter(filter string) {
	c.filter = filter
	c.rebuildFilter()
}

func (c *namespaceComponent) MoveCursor(delta int) {
	if len(c.filtered) == 0 {
		c.cursor = 0
		return
	}
	c.cursor += delta
	if c.cursor < 0 {
		c.cursor = 0
	}
	if c.cursor >= len(c.filtered) {
		c.cursor = len(c.filtered) - 1
	}
}

func (c *namespaceComponent) OpenSelected(ctx context.Context) (string, blobcache.Handle, bool, error) {
	ent, ok := c.selectedEntry()
	if !ok {
		return "", blobcache.Handle{}, false, nil
	}
	next, err := c.svc.OpenFrom(ctx, c.base, ent.LinkToken(), blobcache.Action_ALL)
	if err != nil {
		return "", blobcache.Handle{}, false, err
	}
	return ent.Name, *next, true, nil
}

func (c *namespaceComponent) CopySelected() (string, error) {
	ent, ok := c.selectedEntry()
	if !ok {
		return "", errNoSelection
	}
	data, err := json.Marshal(ent)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *namespaceComponent) RenderRows(width, height int, _ bool) []string {
	if height <= 0 {
		return nil
	}

	lines := make([]string, 0, height)
	lines = append(lines, padOrTrim("  NAME                 TARGET                             RIGHTS               SECRET", width))

	if len(c.filtered) == 0 {
		lines = append(lines, padOrTrim("  (no entries)", width))
		return fillRows(lines, width, height)
	}

	bodyHeight := height - 1
	start := scrollStart(c.cursor, len(c.filtered), bodyHeight)
	for i := 0; i < bodyHeight && start+i < len(c.filtered); i++ {
		idx := c.filtered[start+i]
		ent := c.entries[idx]
		marker := " "
		if start+i == c.cursor {
			marker = ">"
		}
		line := fmt.Sprintf("%s %-20s %-34s %-20s %048x", marker, ent.Name, ent.Target.String(), ent.Rights.String(), ent.Secret)
		lines = append(lines, padOrTrim(line, width))
	}

	return fillRows(lines, width, height)
}

func (c *namespaceComponent) SelectName(name string) bool {
	for i, idx := range c.filtered {
		if c.entries[idx].Name == name {
			c.cursor = i
			return true
		}
	}
	return false
}

func (c *namespaceComponent) selectedEntry() (bcns.Entry, bool) {
	if len(c.filtered) == 0 {
		return bcns.Entry{}, false
	}
	if c.cursor < 0 || c.cursor >= len(c.filtered) {
		return bcns.Entry{}, false
	}
	return c.entries[c.filtered[c.cursor]], true
}

func (c *namespaceComponent) rebuildFilter() {
	c.filtered = c.filtered[:0]
	for i, ent := range c.entries {
		if c.filter == "" || strings.Contains(namespaceSearchText(ent), c.filter) {
			c.filtered = append(c.filtered, i)
		}
	}
	if len(c.filtered) == 0 {
		c.cursor = 0
		return
	}
	if c.cursor < 0 {
		c.cursor = 0
	}
	if c.cursor >= len(c.filtered) {
		c.cursor = len(c.filtered) - 1
	}
}

func namespaceSearchText(ent bcns.Entry) string {
	return fmt.Sprintf("%s %s %s %048x", ent.Name, ent.Target.String(), ent.Rights.String(), ent.Secret)
}

type merklelogRow struct {
	Slot uint64        `json:"slot"`
	Hash blobcache.CID `json:"hash"`
}

type merklelogComponent struct {
	schemaName blobcache.SchemaName
	rows       []merklelogRow
	cursor     int
}

func loadMerklelogComponent(ctx context.Context, svc blobcache.Service, h blobcache.Handle, schemaName blobcache.SchemaName) (component, error) {
	rows, err := bcsdk.View1(ctx, svc, h, func(s bcsdk.RO, root []byte) ([]merklelogRow, error) {
		state, err := mlog.Parse(root)
		if err != nil {
			return nil, err
		}
		ret := make([]merklelogRow, 0, state.Len())
		for i := mlog.Pos(0); i < state.Len(); i++ {
			cid, err := mlog.Get(ctx, s, state, i)
			if err != nil {
				return nil, err
			}
			ret = append(ret, merklelogRow{
				Slot: uint64(i),
				Hash: cid,
			})
		}
		return ret, nil
	})
	if err != nil {
		return nil, err
	}
	return &merklelogComponent{
		schemaName: schemaName,
		rows:       rows,
	}, nil
}

func (c *merklelogComponent) SchemaName() blobcache.SchemaName {
	return c.schemaName
}

func (c *merklelogComponent) SetFilter(_ string) {
}

func (c *merklelogComponent) MoveCursor(delta int) {
	if len(c.rows) == 0 {
		c.cursor = 0
		return
	}
	c.cursor += delta
	if c.cursor < 0 {
		c.cursor = 0
	}
	if c.cursor >= len(c.rows) {
		c.cursor = len(c.rows) - 1
	}
}

func (c *merklelogComponent) OpenSelected(context.Context) (string, blobcache.Handle, bool, error) {
	return "", blobcache.Handle{}, false, nil
}

func (c *merklelogComponent) CopySelected() (string, error) {
	return "", fmt.Errorf("copy is only supported for namespace entries")
}

func (c *merklelogComponent) RenderRows(width, height int, _ bool) []string {
	if height <= 0 {
		return nil
	}

	lines := make([]string, 0, height)
	lines = append(lines, padOrTrim("  SLOT        HASH", width))

	if len(c.rows) == 0 {
		lines = append(lines, padOrTrim("  (empty)", width))
		return fillRows(lines, width, height)
	}

	bodyHeight := height - 1
	start := scrollStart(c.cursor, len(c.rows), bodyHeight)
	for i := 0; i < bodyHeight && start+i < len(c.rows); i++ {
		row := c.rows[start+i]
		marker := " "
		if start+i == c.cursor {
			marker = ">"
		}
		line := fmt.Sprintf("%s %-10d %s", marker, row.Slot, row.Hash.String())
		lines = append(lines, padOrTrim(line, width))
	}

	return fillRows(lines, width, height)
}

type messageComponent struct {
	schemaName blobcache.SchemaName
	lines      []string
}

func newMessageComponent(schemaName blobcache.SchemaName, lines ...string) *messageComponent {
	return &messageComponent{
		schemaName: schemaName,
		lines:      append([]string(nil), lines...),
	}
}

func (c *messageComponent) SchemaName() blobcache.SchemaName {
	return c.schemaName
}

func (c *messageComponent) SetFilter(_ string) {
}

func (c *messageComponent) MoveCursor(_ int) {
}

func (c *messageComponent) OpenSelected(context.Context) (string, blobcache.Handle, bool, error) {
	return "", blobcache.Handle{}, false, nil
}

func (c *messageComponent) CopySelected() (string, error) {
	return "", errNoSelection
}

func (c *messageComponent) RenderRows(width, height int, _ bool) []string {
	if height <= 0 {
		return nil
	}
	if len(c.lines) == 0 {
		return fillRows([]string{padOrTrim("(no data)", width)}, width, height)
	}
	lines := make([]string, 0, height)
	for _, line := range c.lines {
		lines = append(lines, padOrTrim(line, width))
		if len(lines) >= height {
			break
		}
	}
	return fillRows(lines, width, height)
}

func scrollStart(cursor, total, window int) int {
	if total <= window || window <= 0 {
		return 0
	}
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= total {
		cursor = total - 1
	}
	start := cursor - window/2
	if start < 0 {
		start = 0
	}
	maxStart := total - window
	if start > maxStart {
		start = maxStart
	}
	return start
}

func fillRows(lines []string, width, height int) []string {
	for len(lines) < height {
		lines = append(lines, padOrTrim("", width))
	}
	if len(lines) > height {
		lines = lines[:height]
	}
	return lines
}
