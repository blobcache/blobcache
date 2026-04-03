package bctui

import (
	"context"
	"fmt"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/merklelog"
	tea "charm.land/bubbletea/v2"
)

type merklelogRow struct {
	Slot uint64        `json:"slot"`
	Hash blobcache.CID `json:"hash"`
}

var _ Component = &merklelogComponent{}

type merklelogComponent struct {
	rows   []merklelogRow
	cursor int
}

func (c *merklelogComponent) Init(volInfo *blobcache.VolumeInfo) {}

func (c *merklelogComponent) SetState(ctx context.Context, tx *bcsdk.Tx) error {
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		return err
	}
	state, err := merklelog.Parse(root)
	if err != nil {
		return err
	}
	rows := make([]merklelogRow, 0, state.Len())
	for i := merklelog.Pos(0); i < state.Len(); i++ {
		cid, err := merklelog.Get(ctx, tx, state, i)
		if err != nil {
			return err
		}
		rows = append(rows, merklelogRow{Slot: uint64(i), Hash: cid})
	}
	c.rows = rows
	if len(c.rows) == 0 {
		c.cursor = 0
	} else if c.cursor >= len(c.rows) {
		c.cursor = len(c.rows) - 1
	}
	return nil
}

func (c *merklelogComponent) SetFilter(_ string) {
}

func (c *merklelogComponent) Shortcuts() []Binding {
	return []Binding{}
}

func (c *merklelogComponent) Palette() []Binding {
	return []Binding{}
}

func (c *merklelogComponent) DoAction(_ ActionCtx, action Action) {
	switch action {
	case a_Up:
		c.MoveCursor(-1)
	case a_Down:
		c.MoveCursor(1)
	}
}

func (c *merklelogComponent) InsertKey(tea.KeyPressMsg) {
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
