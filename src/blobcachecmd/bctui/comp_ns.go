package bctui

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"blobcache.io/blobcache/src/schema/bcns"
	tea "charm.land/bubbletea/v2"
)

var _ Component = &NSComp{}

type NSComp struct {
	schemaName blobcache.SchemaName
	schema     bcns.Namespace
	entries    []bcns.Entry

	filter   string
	filtered []int
	cursor   int
	inSearch bool
}

func (c *NSComp) Init(volInfo *blobcache.VolumeInfo) {
	c.schemaName = volInfo.Schema.Name
	schema, err := schemareg.Factory(volInfo.Schema)
	if err != nil {
		panic(err)
	}
	nsschema, ok := schema.(bcns.Namespace)
	if !ok {
		panic(fmt.Errorf("not a namespace schema"))
	}
	c.schema = nsschema
}

func (c *NSComp) SchemaName() blobcache.SchemaName {
	return c.schemaName
}

func (c *NSComp) SetFilter(filter string) {
	c.filter = filter
	c.rebuildFilter()
}

func (c *NSComp) Shortcuts() []Binding {
	return []Binding{{
		KeyPress: "/",
		Action:   a_Search,
		Desc:     "search",
	}}
}

func (c *NSComp) Palette() []Binding {
	return []Binding{}
}

func (c *NSComp) DoAction(actx ActionCtx, action Action) {
	switch action {
	case a_Up:
		c.moveCursor(-1)
	case a_Left:
		actx.Exit()
	case a_Down:
		c.moveCursor(1)
	case a_Right:
		ent, ok := c.selectedEntry()
		if !ok {
			return
		}
		actx.GoTo(ent.Name, ent.LinkToken())
	case a_Copy:
		ent, ok := c.selectedEntry()
		if !ok {
			return
		}
		data, err := json.Marshal(ent)
		if err != nil {
			return
		}
		actx.ClipboardWrite(string(data))
	case a_Search:
		c.inSearch = true
		c.filter = ""
		c.rebuildFilter()
		if actx.SetMode != nil {
			actx.SetMode(mode_INSERT)
		}
	case a_No:
		c.inSearch = false
		c.filter = ""
		c.rebuildFilter()
	}
}

func (c *NSComp) SetInsertMode(active bool) {
	c.inSearch = active
}

func (c *NSComp) InsertKey(msg tea.KeyPressMsg) {
	switch msg.String() {
	case "backspace", "ctrl+h":
		if len(c.filter) == 0 {
			return
		}
		runes := []rune(c.filter)
		c.filter = string(runes[:len(runes)-1])
		c.rebuildFilter()
	default:
		if text := msg.Key().Text; text != "" {
			c.filter += text
			c.rebuildFilter()
		}
	}
}

func (c *NSComp) SetState(ctx context.Context, tx *bcsdk.Tx) error {
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

func (c *NSComp) moveCursor(delta int) {
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

func (c *NSComp) openSelected(ctx context.Context) (string, blobcache.Handle, bool, error) {
	ent, ok := c.selectedEntry()
	if !ok {
		return "", blobcache.Handle{}, false, nil
	}
	_ = ctx
	return ent.Name, blobcache.Handle{}, true, nil
}

func (c *NSComp) copySelected() (string, error) {
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

func (c *NSComp) RenderRows(width, height int, _ bool) []string {
	if height <= 0 {
		return nil
	}

	lines := make([]string, 0, height)
	if !c.inSearch && c.filter == "" {
		lines = append(lines, padOrTrim("(press / to search)", width))
	} else if c.inSearch {
		left := c.filter + "█"
		right := "(filtered)"
		if c.filter == "" {
			right = "(continue typing to filter)"
		}
		spaces := width - len(left) - len(right)
		if spaces < 1 {
			spaces = 1
		}
		lines = append(lines, padOrTrim(left+strings.Repeat(" ", spaces)+right, width))
	} else {
		left := c.filter
		right := "(filtered)"
		spaces := width - len(left) - len(right)
		if spaces < 1 {
			spaces = 1
		}
		lines = append(lines, padOrTrim(left+strings.Repeat(" ", spaces)+right, width))
	}
	lines = append(lines, padOrTrim("  NAME                 TARGET                             RIGHTS               SECRET", width))

	if len(c.filtered) == 0 {
		lines = append(lines, padOrTrim("  (no entries)", width))
		return fillRows(lines, width, height)
	}

	bodyHeight := height - len(lines)
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

func (c *NSComp) selectName(name string) bool {
	for i, idx := range c.filtered {
		if c.entries[idx].Name == name {
			c.cursor = i
			return true
		}
	}
	return false
}

func (c *NSComp) selectedEntry() (bcns.Entry, bool) {
	if len(c.filtered) == 0 {
		return bcns.Entry{}, false
	}
	if c.cursor < 0 || c.cursor >= len(c.filtered) {
		return bcns.Entry{}, false
	}
	return c.entries[c.filtered[c.cursor]], true
}

func (c *NSComp) rebuildFilter() {
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
