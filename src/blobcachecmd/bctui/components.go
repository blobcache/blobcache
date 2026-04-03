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
	"blobcache.io/blobcache/src/schema/bcglfs"
	"blobcache.io/blobcache/src/schema/bcns"
	"blobcache.io/blobcache/src/schema/jsonns"
	"blobcache.io/blobcache/src/schema/merklelog"
)

var errNoSelection = errors.New("no row selected")

// Constructor creates a component
type Constructor = func() Component

// Component renders a blobcache Volume to the terminal
type Component interface {
	// Init is called after the volume is first inspected.
	Init(volInfo *blobcache.VolumeInfo)

	// Update is called to set the internal state of the component using a blobcache transaction
	// for the volume.
	// This will be called in a tea.Command so it is okay if it takes a while
	// It will not block the UI.
	// The Component should not retain the transaction.
	SetState(ctx context.Context, tx *bcsdk.Tx) error

	SetFilter(filter string)
	MoveCursor(delta int)
	CopySelected() (string, error)
	RenderRows(width, height int, focused bool) []string
}

func defaultConstructors() map[blobcache.SchemaName]Constructor {
	return map[blobcache.SchemaName]Constructor{
		blobcache.Schema_NONE: func() Component { return &noneComponent{} },
		bcglfs.SchemaName:     func() Component { return &GLFSComp{} },
		merklelog.SchemaName:  func() Component { return &merklelogComponent{} },
		jsonns.SchemaName:     func() Component { return &namespaceComponent{} },
	}
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
	schema     bcns.Namespace
	entries    []bcns.Entry

	filter   string
	filtered []int
	cursor   int
}

func (c *namespaceComponent) Init(volInfo *blobcache.VolumeInfo) {
	c.schemaName = volInfo.Schema.Name
	c.schema = jsonns.Schema{}
}

func (c *namespaceComponent) SchemaName() blobcache.SchemaName {
	return c.schemaName
}

func (c *namespaceComponent) SetFilter(filter string) {
	c.filter = filter
	c.rebuildFilter()
}

func (c *namespaceComponent) SetState(ctx context.Context, tx *bcsdk.Tx) error {
	nstx, err := bcns.NewFromTx(ctx, c.schema, tx)
	if err != nil {
		return err
	}
	ents, err := nstx.List(ctx)
	if err != nil {
		return err
	}
	slices.SortFunc(ents, func(a, b bcns.Entry) int {
		return strings.Compare(a.Name, b.Name)
	})
	c.entries = ents
	c.rebuildFilter()
	return nil
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
	_ = ctx
	return ent.Name, blobcache.Handle{}, true, nil
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

func (c *messageComponent) Init(_ *blobcache.VolumeInfo) {
}

func (c *messageComponent) SetFilter(_ string) {
}

func (c *messageComponent) SetState(context.Context, *bcsdk.Tx) error {
	return nil
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

type noneComponent struct {
	lines []string
}

func (c *noneComponent) Init(_ *blobcache.VolumeInfo) {
}

func (c *noneComponent) SetState(ctx context.Context, tx *bcsdk.Tx) error {
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return err
	}
	header := fmt.Sprintf("CELL DATA (%d bytes):", len(root))
	dump := strings.TrimSuffix(hex.Dump(root), "\n")
	if dump == "" {
		c.lines = []string{header}
		return nil
	}
	c.lines = append([]string{header}, strings.Split(dump, "\n")...)
	return nil
}

func (c *noneComponent) SetFilter(_ string) {
}

func (c *noneComponent) MoveCursor(_ int) {
}

func (c *noneComponent) OpenSelected(context.Context) (string, blobcache.Handle, bool, error) {
	return "", blobcache.Handle{}, false, nil
}

func (c *noneComponent) CopySelected() (string, error) {
	return "", errNoSelection
}

func (c *noneComponent) RenderRows(width, height int, _ bool) []string {
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
