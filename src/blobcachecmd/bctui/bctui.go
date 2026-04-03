package bctui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

var _ tea.Model = &Model{}

// mode is a mode for the user interface.  Each mode may have different key bindings
type mode uint8

const (
	// mode_NORMAL is the default mode
	// In normal mode, some default keybindings are active
	mode_NORMAL = mode(iota)
	// mode_INSERT means that all the keys except for escape should be forwarded to the
	// active component
	mode_INSERT
	// mode_MENU means the leader key menu is active
	// Once a valid action is picked from the menu, the system will return to normal mode
	mode_MENU
	// mode_ERROR means an API error modal is active.
	// once the error is cleared then the system will return to normal mode.
	mode_ERROR
)

// Model contains all the application state
type Model struct {
	svc        blobcache.Service
	root       blobcache.Handle
	components map[blobcache.SchemaName]Constructor

	width  int
	height int
	styles uiStyles
	// pathStack holds the elements of the namespace path
	pathStack []string
	// mode is the UI mode
	mode mode

	// chain holds the chain of volumes from the initial fiat access, all the
	// way to whatever is being previewed.
	// Invariant: len(chain) >= 2, where chain[len-2] is focus and chain[len-1] is preview.
	chain []*volume

	statusLine string
	errorText  string
}

func New(svc blobcache.Service, root blobcache.Handle) *tea.Program {
	return tea.NewProgram(&Model{
		svc:        svc,
		root:       root,
		components: defaultConstructors(),
		styles:     defaultStyles(),
	})
}

func (m *Model) Init() tea.Cmd {
	if err := m.refreshAll(context.Background()); err != nil {
		m.reportError(err)
	}
	return tea.Raw(tea.RequestWindowSize())
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}

		switch m.mode {
		case mode_ERROR:
			m.updateErrorMode(msg)
			return m, nil
		case mode_MENU:
			m.updateLeaderMode(msg)
			return m, nil
		case mode_INSERT:
			m.updateInsertMode(msg)
			return m, nil
		}

		if m.handleShortcut(msg.String()) {
			return m, nil
		}

		switch msg.String() {
		case "j", "down":
			m.dispatchAction(a_Down)
		case "k", "up":
			m.dispatchAction(a_Up)
		case "l", "right":
			m.dispatchAction(a_Right)
		case "h", "left":
			m.dispatchAction(a_Left)
		case " ", "space":
			m.setMode(mode_MENU)
		}
	}
	return m, nil
}

func (m *Model) View() tea.View {
	const defaultWidth = 120
	const defaultHeight = 32

	width := m.width
	if width <= 0 {
		width = defaultWidth
	}
	height := m.height
	if height <= 0 {
		height = defaultHeight
	}

	topText := ""
	if focus := m.focusPane(); focus != nil {
		topText = focus.fqoid.String()
	}
	top := m.styles.topBar.Width(width).Render(centerText(topText, width-2))

	bodyHeight := height - 5
	if bodyHeight < 8 {
		bodyHeight = 8
	}
	body := ""
	if m.mode == mode_ERROR && m.errorText != "" {
		body = m.renderErrorModal(width, bodyHeight)
	} else {
		visible := m.visiblePanes()
		sepWidth := len(visible) - 1
		contentWidth := width - sepWidth
		if contentWidth < len(visible) {
			contentWidth = len(visible)
		}

		colWidths := make([]int, len(visible))
		if len(visible) == 3 {
			colWidths[0] = contentWidth / 4
			colWidths[1] = contentWidth / 2
			colWidths[2] = contentWidth - colWidths[0] - colWidths[1]
		} else {
			base := contentWidth / len(visible)
			extra := contentWidth % len(visible)
			for i := range visible {
				colWidths[i] = base
				if i < extra {
					colWidths[i]++
				}
			}
		}

		paneBlocks := make([]string, len(visible))
		for i := range visible {
			paneBlocks[i] = visible[i].View(m.styles, colWidths[i], bodyHeight, i == len(visible)-2)
		}
		body = lipgloss.JoinHorizontal(lipgloss.Top, paneBlocks...)
	}

	status := m.statusLine
	if status == "" {
		status = "ready"
	}
	statusLine := m.styles.statusBar.Width(width).Render(truncate(status, width-2))

	controlsLine := m.styles.controlsBar.Width(width).Render(truncate(m.controlsLine(), width-2))

	screen := lipgloss.JoinVertical(lipgloss.Left, top, body, statusLine, controlsLine)
	if m.mode == mode_MENU {
		screen = m.renderLeaderOverlay(width, height, screen)
	}
	return fullScreenView(screen)
}

func fullScreenView(content string) tea.View {
	v := tea.NewView(content)
	v.AltScreen = true
	return v
}

func (m *Model) updateLeaderMode(msg tea.KeyPressMsg) {
	switch msg.String() {
	case "esc", "space", " ":
		m.setMode(mode_NORMAL)
	case "r":
		m.setMode(mode_NORMAL)
		if err := m.refreshAll(context.Background()); err != nil {
			m.reportError(err)
		}
	case "y":
		m.setMode(mode_NORMAL)
		if err := m.copySelectionToClipboard(); err != nil {
			if errors.Is(err, errNoSelection) {
				m.statusLine = err.Error()
			} else {
				m.reportError(err)
			}
		}
	}
}

func (m *Model) updateErrorMode(msg tea.KeyPressMsg) {
	switch msg.String() {
	case "esc", "enter":
		m.errorText = ""
		m.setMode(mode_NORMAL)
	}
}

func (m *Model) updateInsertMode(msg tea.KeyPressMsg) {
	if msg.String() == "esc" {
		active := m.activeComponent()
		if active != nil {
			active.DoAction(m.actionCtx(), a_No)
		}
		if c, ok := m.activeComponent().(interface{ SetInsertMode(bool) }); ok {
			c.SetInsertMode(false)
		}
		m.setMode(mode_NORMAL)
		return
	}
	active := m.activeComponent()
	if active == nil {
		return
	}
	active.InsertKey(msg)
	m.refreshPreview(context.Background())
}

func (m *Model) dispatchAction(action Action) {
	active := m.activeComponent()
	if active == nil {
		return
	}
	active.DoAction(m.actionCtx(), action)
}

func (m *Model) enterSelection() {
	active := m.activeComponent()
	if active == nil {
		m.statusLine = "no active component"
		return
	}
	openable, ok := active.(interface {
		OpenSelected(context.Context) (string, blobcache.Handle, bool, error)
	})
	if !ok {
		m.statusLine = "selection cannot be opened"
		return
	}
	name, next, ok, err := openable.OpenSelected(context.Background())
	if err != nil {
		m.reportError(err)
		return
	}
	if !ok {
		m.statusLine = "selection cannot be opened"
		return
	}
	focus := m.focusPane()
	if next == (blobcache.Handle{}) && name != "" && focus != nil {
		next, err = openNamespaceEntryByName(context.Background(), m.svc, focus.handle, name)
		if err != nil {
			m.reportError(err)
			return
		}
	}
	m.pathStack = append(m.pathStack, name)
	if err := m.refreshAll(context.Background()); err != nil {
		m.reportError(err)
	}
}

func (m *Model) exitSelection() {
	if len(m.pathStack) == 0 {
		return
	}
	m.pathStack = m.pathStack[:len(m.pathStack)-1]
	if err := m.refreshAll(context.Background()); err != nil {
		m.reportError(err)
	}
}

func (m *Model) refreshAll(ctx context.Context) error {
	root, err := ensureHandle(ctx, m.svc, m.root)
	if err != nil {
		return err
	}
	m.root = root
	handles := []blobcache.Handle{root}
	current := root
	depth := 0
	for i, name := range m.pathStack {
		next, e := openNamespaceEntryByName(ctx, m.svc, current, name)
		if e != nil {
			m.reportError(e)
			depth = i
			break
		}
		handles = append(handles, next)
		current = next
		depth = i + 1
	}
	if depth < len(m.pathStack) {
		m.pathStack = append([]string(nil), m.pathStack[:depth]...)
	}

	lineage := make([]*volume, 0, len(handles))
	for i, h := range handles {
		errPrefix := "failed to load current"
		if i == 0 {
			errPrefix = "failed to load root"
		} else if i < len(handles)-1 {
			errPrefix = "failed to load ancestor"
		}
		lineage = append(lineage, m.loadPane(ctx, h, errPrefix))
	}
	for i := 0; i < len(lineage)-1 && i < len(m.pathStack); i++ {
		if x, ok := lineage[i].component.(interface{ SelectName(string) bool }); ok {
			x.SelectName(m.pathStack[i])
		}
	}
	if len(lineage) == 0 {
		lineage = append(lineage, m.newMessagePane("(no focus pane)"))
	}
	m.chain = append(lineage, m.newMessagePane("(no preview)"))

	m.refreshPreview(ctx)
	m.setReadStatus(m.focusPane())
	return nil
}

func (m *Model) setReadStatus(p *volume) {
	if p == nil {
		return
	}
	schema := p.schemaName
	if schema == "" {
		schema = "(none)"
	}
	m.statusLine = fmt.Sprintf("read from %s at %s, rendered using schema %s", p.oid.String(), time.Now().Format("15:04:05"), schema)
}

func (m *Model) resolveCurrent(ctx context.Context, root blobcache.Handle) (blobcache.Handle, blobcache.Handle, int, error) {
	current := root
	parent := blobcache.Handle{}
	for i, name := range m.pathStack {
		next, err := openNamespaceEntryByName(ctx, m.svc, current, name)
		if err != nil {
			return current, parent, i, err
		}
		parent = current
		current = next
	}
	return current, parent, len(m.pathStack), nil
}

func (m *Model) refreshPreview(ctx context.Context) {
	active := m.activeComponent()
	if active == nil {
		m.setPreviewPane(m.newMessagePane("(no active component)"))
		return
	}
	openable, ok := active.(interface {
		OpenSelected(context.Context) (string, blobcache.Handle, bool, error)
	})
	if !ok {
		m.setPreviewPane(m.newMessagePane("(no preview)"))
		return
	}
	name, next, ok, err := openable.OpenSelected(ctx)
	if err != nil {
		m.setPreviewPane(m.newMessagePane("preview error", err.Error()))
		m.reportError(err)
		return
	}
	if !ok {
		m.setPreviewPane(m.newMessagePane("(no preview)"))
		return
	}
	focus := m.focusPane()
	if next == (blobcache.Handle{}) && focus != nil {
		if name == "" {
			m.setPreviewPane(m.newMessagePane("(no preview)"))
			return
		}
		next, err = openNamespaceEntryByName(ctx, m.svc, focus.handle, name)
		if err != nil {
			m.setPreviewPane(m.newMessagePane("preview error", err.Error()))
			m.reportError(err)
			return
		}
	}
	m.setPreviewPane(m.loadPane(ctx, next, "preview error"))
}

func (m *Model) focusPane() *volume {
	if len(m.chain) < 2 {
		return nil
	}
	return m.chain[len(m.chain)-2]
}

func (m *Model) visiblePanes() []*volume {
	if len(m.chain) == 0 {
		return []*volume{m.newMessagePane("(no focus pane)"), m.newMessagePane("(no preview)")}
	}
	if len(m.chain) == 1 {
		return []*volume{m.chain[0], m.newMessagePane("(no preview)")}
	}
	if len(m.chain) <= 3 {
		return m.chain
	}
	return m.chain[len(m.chain)-3:]
}

func (m *Model) setPreviewPane(p *volume) {
	if p == nil {
		p = m.newMessagePane("(no preview)")
	}
	if len(m.chain) == 0 {
		m.chain = []*volume{m.newMessagePane("(no focus pane)"), p}
		return
	}
	if len(m.chain) == 1 {
		m.chain = append(m.chain, p)
		return
	}
	m.chain[len(m.chain)-1] = p
}

func (m *Model) activeComponent() Component {
	focus := m.focusPane()
	if focus == nil {
		return nil
	}
	return focus.component
}

func (m *Model) actionCtx() ActionCtx {
	return ActionCtx{
		Mode:    m.mode,
		SetMode: m.setMode,
		IO: func(tea.Cmd) {
		},
		ClipboardWrite: m.writeClipboard,
		ClipboardRead:  func() string { return "" },
		GoTo:           m.goToLink,
		SetPreview:     m.setPreviewLink,
		Exit:           m.exitSelection,
	}
}

func (m *Model) setPreviewLink(lt blobcache.LinkToken) {
	focus := m.focusPane()
	if focus == nil {
		m.setPreviewPane(m.newMessagePane("(no preview)"))
		return
	}
	if lt == (blobcache.LinkToken{}) {
		m.setPreviewPane(m.newMessagePane("(no preview)"))
		return
	}
	ctx := context.Background()
	h, err := m.svc.OpenFrom(ctx, focus.handle, lt, blobcache.Action_ALL)
	if err != nil {
		m.setPreviewPane(m.newMessagePane("preview error", err.Error()))
		m.reportError(err)
		return
	}
	m.setPreviewPane(m.loadPane(ctx, *h, "preview error"))
}

func (m *Model) goToLink(name string, lt blobcache.LinkToken) {
	ctx := context.Background()
	m.pathStack = append(m.pathStack, name)
	if err := m.refreshAll(ctx); err != nil {
		m.reportError(err)
	}
}

func (m *Model) handleShortcut(key string) bool {
	active := m.activeComponent()
	if active == nil {
		return false
	}
	for _, binding := range active.Shortcuts() {
		if binding.KeyPress != key {
			continue
		}
		active.DoAction(m.actionCtx(), binding.Action)
		return true
	}
	return false
}

func (m *Model) newMessagePane(lines ...string) *volume {
	return &volume{component: newMessageComponent("", lines...)}
}

func (m *Model) loadPane(ctx context.Context, h blobcache.Handle, errPrefix string) *volume {
	vinfo, err := m.svc.InspectVolume(ctx, h)
	schemaName := blobcache.Schema_NONE
	if err == nil {
		schemaName = vinfo.Schema.Name
	}

	ctor := m.components[schemaName]
	if ctor == nil {
		ctor = m.components[blobcache.Schema_NONE]
		schemaName = blobcache.Schema_NONE
		ctor = func() Component { return newMessageComponent(blobcache.Schema_NONE) }
		schemaName = blobcache.Schema_NONE
	}
	comp := ctor()
	if err == nil {
		comp.Init(vinfo)
	}
	if err == nil {
		tx, txErr := bcsdk.BeginTx(ctx, m.svc, h, blobcache.TxParams{})
		if txErr != nil {
			err = txErr
		} else {
			defer tx.Abort(ctx)
			err = comp.SetState(ctx, tx)
		}
	}
	if err != nil {
		comp = newMessageComponent(schemaName, errPrefix, err.Error())
	}

	fqoid, err := resolveFQOID(ctx, m.svc, h)
	oid := blobcache.OID{}
	if err != nil {
		fqoid = blobcache.FQOID{}
		oid = resolveVolumeOID(ctx, m.svc, h)
	} else {
		oid = fqoid.OID
	}

	return &volume{
		oid:        oid,
		fqoid:      fqoid,
		handle:     h,
		schemaName: string(schemaName),
		component:  comp,
	}
}

func truncate(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= width {
		return s
	}
	if width <= 3 {
		for len(s) > width {
			s = s[:len(s)-1]
		}
		return s
	}
	for lipgloss.Width(s) > width-3 {
		s = s[:len(s)-1]
	}
	return s + "..."
}

func truncateMiddle(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= width {
		return s
	}
	if width <= 3 {
		return truncate(s, width)
	}
	keep := width - 3
	left := keep / 2
	right := keep - left
	return s[:left] + "..." + s[len(s)-right:]
}

func (m *Model) controlsLine() string {
	parts := []struct {
		key  string
		desc string
	}{
		{key: "hjkl/arrows", desc: "move"},
		{key: "l/right", desc: "enter"},
		{key: "h/left", desc: "back"},
		{key: "space", desc: "menu"},
		{key: "q", desc: "quit"},
	}

	if active := m.activeComponent(); active != nil {
		for _, binding := range active.Shortcuts() {
			desc := binding.Desc
			if desc == "" {
				desc = string(binding.Action)
			}
			parts = append(parts, struct {
				key  string
				desc string
			}{
				key:  binding.KeyPress,
				desc: desc,
			})
		}
	}

	segs := make([]string, 0, len(parts))
	for _, part := range parts {
		segs = append(segs, m.keyDescSegment(part.key, part.desc))
	}
	return strings.Join(segs, "  ")
}

func (m *Model) modalLine() string {
	switch m.mode {
	case mode_NORMAL:
		return "NORM"
	case mode_ERROR:
		return "error: " + m.keyText("esc") + "/" + m.keyText("enter") + " dismiss"
	case mode_MENU:
		return "MENU: " + m.keyText("r") + " refresh, " + m.keyText("y") + " copy selected item, " + m.keyText("esc") + " cancel"
	case mode_INSERT:
		return "INS_: " + m.keyText("esc") + " exit"
	default:
		return ""
	}
}

func (m *Model) keyText(key string) string {
	return m.styles.controlsKey.Render(key)
}

func (m *Model) keyTextIn(base lipgloss.Style, key string) string {
	s := m.styles.controlsKey
	if bg := base.GetBackground(); bg != nil {
		s = s.Background(bg).ColorWhitespace(true)
	}
	return s.Render(key)
}

func (m *Model) keyDescSegment(key, desc string) string {
	return m.keyText(key) + " " + m.styles.controlsDesc.Render(desc)
}

func (m *Model) menuCommandLine(key, desc string, width int) string {
	if width <= 1 {
		return m.keyTextIn(m.styles.leaderBody, key)
	}
	keyPart := m.keyTextIn(m.styles.leaderBody, key)
	descPart := m.styles.leaderBody.Inherit(m.styles.controlsDesc).Render(desc)
	spaces := width - lipgloss.Width(keyPart) - lipgloss.Width(descPart)
	if spaces < 1 {
		descPart = m.styles.leaderBody.Inherit(m.styles.controlsDesc).Render(truncate(desc, width-lipgloss.Width(keyPart)-1))
		spaces = width - lipgloss.Width(keyPart) - lipgloss.Width(descPart)
		if spaces < 1 {
			spaces = 1
		}
	}
	return keyPart + m.styles.leaderBody.Render(strings.Repeat(" ", spaces)) + descPart
}

func (m *Model) copySelectionToClipboard() error {
	active := m.activeComponent()
	if active == nil {
		return fmt.Errorf("no active component")
	}
	active.DoAction(m.actionCtx(), a_Copy)
	return nil
}

func (m *Model) writeClipboard(value string) {
	// TODO
}

func (m *Model) reportError(err error) {
	if err == nil {
		return
	}
	m.errorText = err.Error()
	m.statusLine = ""
	if m.mode != mode_ERROR {
		m.setMode(mode_ERROR)
	}
}

func (m *Model) renderErrorModal(width, height int) string {
	w := width - 10
	if w > 96 {
		w = 96
	}
	if w < 20 {
		w = width
	}

	title := m.styles.errorTitle.Width(w - 6).Render("API Error")
	body := m.styles.errorBody.Width(w - 6).Render(m.errorText)
	hint := m.styles.errorHint.Width(w - 6).Render("Press " + m.keyText("esc") + " or " + m.keyText("enter") + " to dismiss")
	box := m.styles.errorBox.Width(w).Render(lipgloss.JoinVertical(lipgloss.Left, title, body, hint))
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, box)
}

func (m *Model) renderLeaderOverlay(width, height int, base string) string {
	w := width - 10
	if w > 72 {
		w = 72
	}
	if w < 24 {
		w = width
	}

	bodyWidth := w - 6
	bodyLines := []string{
		m.menuCommandLine("r", "refresh", bodyWidth),
		m.menuCommandLine("y", "copy selected item", bodyWidth),
		"",
		m.styles.leaderHint.Render("Press ") +
			m.keyTextIn(m.styles.leaderHint, "esc") +
			m.styles.leaderHint.Render(" or ") +
			m.keyTextIn(m.styles.leaderHint, "space") +
			m.styles.leaderHint.Render(" to close this menu"),
	}
	body := m.styles.leaderBody.Width(bodyWidth).Render(strings.Join(bodyLines, "\n"))
	box := m.styles.leaderBox.Width(w).Render(body)

	x := (width - lipgloss.Width(box)) / 2
	if x < 0 {
		x = 0
	}
	y := (height - lipgloss.Height(box)) / 2
	if y < 0 {
		y = 0
	}

	baseLayer := lipgloss.NewLayer(base).Z(0)
	modalLayer := lipgloss.NewLayer(box).X(x).Y(y).Z(1)
	return lipgloss.NewCompositor(baseLayer, modalLayer).Render()
}

func (m *Model) setMode(md mode) {
	m.mode = md
}

func ensureHandle(ctx context.Context, svc blobcache.Service, h blobcache.Handle) (blobcache.Handle, error) {
	if h.Secret != ([16]byte{}) {
		return h, nil
	}
	h2, err := svc.OpenFiat(ctx, h.OID, blobcache.Action_ALL)
	if err != nil {
		return blobcache.Handle{}, err
	}
	return *h2, nil
}

func resolveFQOID(ctx context.Context, svc blobcache.Service, h blobcache.Handle) (blobcache.FQOID, error) {
	ep, err := svc.Endpoint(ctx)
	if err == nil {
		if ep.Peer == (blobcache.PeerID{}) {
			return blobcache.FQOID{}, fmt.Errorf("endpoint returned zero peer id")
		}
		return blobcache.FQOID{Peer: ep.Peer, OID: h.OID}, nil
	}

	vinfo, err := svc.InspectVolume(ctx, h)
	if err != nil {
		return blobcache.FQOID{}, err
	}
	if vinfo.Backend.Remote != nil {
		if vinfo.Backend.Remote.Endpoint.Peer == (blobcache.PeerID{}) {
			return blobcache.FQOID{}, fmt.Errorf("remote backend returned zero peer id")
		}
		return blobcache.FQOID{
			Peer: vinfo.Backend.Remote.Endpoint.Peer,
			OID:  vinfo.Backend.Remote.Volume,
		}, nil
	}
	return blobcache.FQOID{}, fmt.Errorf("no non-zero peer id available")
}

func resolveVolumeOID(ctx context.Context, svc blobcache.Service, h blobcache.Handle) blobcache.OID {
	if h.OID != (blobcache.OID{}) {
		return h.OID
	}
	vinfo, err := svc.InspectVolume(ctx, h)
	if err != nil {
		return blobcache.OID{}
	}
	if vinfo.ID != (blobcache.OID{}) {
		return vinfo.ID
	}
	if vinfo.Backend.Remote != nil {
		return vinfo.Backend.Remote.Volume
	}
	return blobcache.OID{}
}

func padOrTrim(s string, width int) string {
	if width <= 0 {
		return ""
	}
	s = truncate(s, width)
	if lipgloss.Width(s) < width {
		s += strings.Repeat(" ", width-lipgloss.Width(s))
	}
	return s
}

func centerText(s string, width int) string {
	if width <= 0 {
		return ""
	}
	s = truncate(s, width)
	w := lipgloss.Width(s)
	if w >= width {
		return s
	}
	left := (width - w) / 2
	right := width - w - left
	return strings.Repeat(" ", left) + s + strings.Repeat(" ", right)
}

// volume holds the state for a volume, and can be rendered using view.
type volume struct {
	oid        blobcache.OID
	fqoid      blobcache.FQOID
	handle     blobcache.Handle
	schemaName string
	component  Component
}

func (p *volume) View(styles uiStyles, width, height int, focused bool) string {
	if width < 1 {
		width = 1
	}
	if height < 1 {
		height = 1
	}
	if width < 8 {
		width = 8
	}
	if height < 4 {
		height = 4
	}

	bodyHeight := height - 2
	innerWidth := width - 4
	if innerWidth < 1 {
		innerWidth = 1
	}

	var rows []string
	if p.component != nil {
		rows = p.component.RenderRows(innerWidth, bodyHeight, focused)
	}
	body := strings.Join(rows, "\n")

	headerStyle := styles.paneHeader
	paneStyle := styles.pane
	if focused {
		headerStyle = styles.paneHeaderActive
		paneStyle = styles.paneActive
	}
	titleWidth := innerWidth - headerStyle.GetHorizontalFrameSize()
	title := paneHeaderLine(p.oid.String(), p.schemaName, titleWidth)
	header := headerStyle.Render(title)
	content := lipgloss.JoinVertical(lipgloss.Left, header, body)
	return paneStyle.Width(width).Height(height).Render(content)
}

func paneHeaderLine(oid, schema string, width int) string {
	if width <= 0 {
		return ""
	}
	schema = strings.ReplaceAll(schema, "\n", " ")
	if schema == "" {
		return padOrTrim(truncateMiddle(oid, width), width)
	}

	schema = truncate(schema, width)
	if lipgloss.Width(schema) >= width {
		return padOrTrim(schema, width)
	}

	oidWidth := width - lipgloss.Width(schema) - 1
	if oidWidth < 0 {
		oidWidth = 0
	}
	oid = truncateMiddle(oid, oidWidth)
	spaces := width - lipgloss.Width(oid) - lipgloss.Width(schema)
	if spaces < 1 {
		spaces = 1
	}
	return oid + strings.Repeat(" ", spaces) + schema
}
