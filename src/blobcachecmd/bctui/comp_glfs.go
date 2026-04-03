package bctui

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcglfs"
	"blobcache.io/glfs"
	tea "charm.land/bubbletea/v2"
)

type glfsRow struct {
	Path string    `json:"path"`
	Type glfs.Type `json:"type"`
	Size uint64    `json:"size"`
	Mode string    `json:"mode"`
}

// GLFSComp is a UI component for displaying a GLFS filesystem.
type GLFSComp struct {
	schemaName blobcache.SchemaName
	rows       []glfsRow
	filter     string
	filtered   []int
	cursor     int
}

func (c *GLFSComp) Init(volInfo *blobcache.VolumeInfo) {
	c.schemaName = volInfo.Schema.Name
}

func (c *GLFSComp) SetState(ctx context.Context, tx *bcsdk.Tx) error {
	root, err := bcglfs.Load(ctx, tx)
	if err != nil {
		return err
	}

	rows := []glfsRow{{
		Path: "/",
		Type: root.Type,
		Size: root.Size,
		Mode: "",
	}}
	if root.Type == glfs.TypeTree {
		err := glfs.WalkTree(ctx, tx, *root, func(prefix string, ent glfs.TreeEntry) error {
			p := path.Join(prefix, ent.Name)
			rows = append(rows, glfsRow{
				Path: p,
				Type: ent.Ref.Type,
				Size: ent.Ref.Size,
				Mode: ent.FileMode.String(),
			})
			return nil
		})
		if err != nil {
			return err
		}
	}

	c.rows = rows
	c.rebuildFilter()
	return nil
}

func (c *GLFSComp) SetFilter(filter string) {
	c.filter = filter
	c.rebuildFilter()
}

func (c *GLFSComp) Shortcuts() []Binding {
	return []Binding{}
}

func (c *GLFSComp) Palette() []Binding {
	return []Binding{}
}

func (c *GLFSComp) DoAction(actx ActionCtx, action Action) {
	switch action {
	case a_Up:
		c.MoveCursor(-1)
	case a_Down:
		c.MoveCursor(1)
	case a_Copy:
		row, ok := c.selectedRow()
		if !ok {
			return
		}
		data, err := json.Marshal(row)
		if err != nil {
			return
		}
		actx.ClipboardWrite(string(data))
	}
}

func (c *GLFSComp) InsertKey(tea.KeyPressMsg) {
}

func (c *GLFSComp) MoveCursor(delta int) {
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

func (c *GLFSComp) OpenSelected(context.Context) (string, blobcache.Handle, bool, error) {
	return "", blobcache.Handle{}, false, nil
}

func (c *GLFSComp) CopySelected() (string, error) {
	row, ok := c.selectedRow()
	if !ok {
		return "", errNoSelection
	}
	data, err := json.Marshal(row)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *GLFSComp) RenderRows(width, height int, _ bool) []string {
	if height <= 0 {
		return nil
	}

	lines := make([]string, 0, height)
	lines = append(lines, padOrTrim("  TYPE MODE       SIZE       PATH", width))
	if len(c.filtered) == 0 {
		lines = append(lines, padOrTrim("  (empty filesystem)", width))
		return fillRows(lines, width, height)
	}

	bodyHeight := height - 1
	start := scrollStart(c.cursor, len(c.filtered), bodyHeight)
	for i := 0; i < bodyHeight && start+i < len(c.filtered); i++ {
		row := c.rows[c.filtered[start+i]]
		marker := " "
		if start+i == c.cursor {
			marker = ">"
		}
		ty := string(row.Type)
		if row.Type == glfs.TypeTree {
			ty = "dir"
		}
		line := fmt.Sprintf("%s %-4s %-10s %-10d %s", marker, ty, row.Mode, row.Size, renderGLFSPath(row))
		lines = append(lines, padOrTrim(line, width))
	}

	return fillRows(lines, width, height)
}

func (c *GLFSComp) selectedRow() (glfsRow, bool) {
	if len(c.filtered) == 0 {
		return glfsRow{}, false
	}
	if c.cursor < 0 || c.cursor >= len(c.filtered) {
		return glfsRow{}, false
	}
	return c.rows[c.filtered[c.cursor]], true
}

func (c *GLFSComp) rebuildFilter() {
	c.filtered = c.filtered[:0]
	for i, row := range c.rows {
		if c.filter == "" || strings.Contains(glfsSearchText(row), c.filter) {
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

func glfsSearchText(row glfsRow) string {
	return fmt.Sprintf("%s %s %s %d", row.Path, row.Type, row.Mode, row.Size)
}

func renderGLFSPath(row glfsRow) string {
	if row.Type == glfs.TypeTree && row.Path != "/" {
		return row.Path + "/"
	}
	return row.Path
}
