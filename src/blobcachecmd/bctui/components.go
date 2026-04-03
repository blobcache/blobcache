package bctui

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema/bcglfs"
	"blobcache.io/blobcache/src/schema/bcns"
	"blobcache.io/blobcache/src/schema/jsonns"
	"blobcache.io/blobcache/src/schema/merklelog"
	tea "charm.land/bubbletea/v2"
)

var errNoSelection = errors.New("no row selected")

// Constructor creates a component
type Constructor = func() Component

// Action is something that can be done in the application.
// It is the internal canonical name for the Action regardless
// of what the keybinding is set to.
type Action string

const (
	a_Up    = Action("up")
	a_Left  = Action("left")
	a_Right = Action("right")
	a_Down  = Action("down")

	a_Copy  = Action("copy")
	a_Paste = Action("paste")

	// a_Yes is usually the enter/return key.
	a_Yes = Action("yes")
	// a_No is usually the escape key.
	// Escape should back out of or cancel whatever is going on.
	// If a non-normal mode is active, then it will switch to normal mode.
	a_No = Action("no")
)

const (
	a_Refresh = Action("refresh")
	a_Search  = Action("search")
)

// Binding is a KeyPress -> Action pair, and a description of the action
type Binding struct {
	// KeyPress is the key pressed to perform the action.
	KeyPress string
	// Name is the internal name for the action in the application
	Action Action
	// Desc is what action is performed.
	Desc string
}

type ActionCtx struct {
	// Invariant: GoTo and Exit are always non-nil.
	// Mode is the current mode that the UI is in.
	Mode mode
	// SetMode can be used to change the UI mode.
	SetMode func(mode)
	// IO enqueues an IO operation
	IO func(fn tea.Cmd)
	// Clipboard writes to the system clipboard.
	ClipboardWrite func(string)
	// ClipboardRead reads from the system clipboard.
	ClipboardRead func() string

	// GoTo navigates to another Volume linked from the current one.
	GoTo func(name string, lt blobcache.LinkToken)
	// Exit tells the UI to close this component.
	Exit func()
}

// Component renders a blobcache Volume to the terminal
type Component interface {
	// Init is called after the volume is first inspected.
	Init(volInfo *blobcache.VolumeInfo)

	// Update is called to set the internal state of the component using a blobcache transaction
	// for the volume.
	// This will be called in a tea.Cmd so it is okay if it takes a while
	// It will not block the UI.
	// The Component should not retain the transaction.
	SetState(ctx context.Context, tx *bcsdk.Tx) error
	// Do requests that the action be taken.
	DoAction(actx ActionCtx, a Action)
	// InsertKey handles a keypress while the model is in insert mode.
	InsertKey(msg tea.KeyPressMsg)

	// Shortcuts are bindings which can be pressed at any time while the Component is focused.
	Shortcuts() []Binding
	// Palette returns Bindings which correspond to actions that can be performed by the component
	Palette() []Binding
	// RenderRows renders the component to the terminal
	RenderRows(width, height int, focused bool) []string
}

func defaultConstructors() map[blobcache.SchemaName]Constructor {
	return map[blobcache.SchemaName]Constructor{
		blobcache.Schema_NONE: func() Component { return &noneComponent{} },
		bcglfs.SchemaName:     func() Component { return &GLFSComp{} },
		merklelog.SchemaName:  func() Component { return &merklelogComponent{} },
		jsonns.SchemaName:     func() Component { return &NSComp{} },
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

func (c *messageComponent) Shortcuts() []Binding {
	return []Binding{}
}

func (c *messageComponent) Palette() []Binding {
	return []Binding{}
}

func (c *messageComponent) DoAction(actx ActionCtx, action Action) {
	switch action {
	case a_Left:
		actx.Exit()
	}
}

func (c *messageComponent) InsertKey(tea.KeyPressMsg) {
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

func (c *noneComponent) Shortcuts() []Binding {
	return []Binding{}
}

func (c *noneComponent) Palette() []Binding {
	return []Binding{}
}

func (c *noneComponent) DoAction(actx ActionCtx, action Action) {
	switch action {
	case a_Left:
		actx.Exit()
	}
}

func (c *noneComponent) InsertKey(tea.KeyPressMsg) {
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
