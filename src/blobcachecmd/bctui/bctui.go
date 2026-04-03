package bctui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"blobcache.io/blobcache/src/blobcache"

	"github.com/aymanbagabas/go-osc52/v2"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var _ tea.Model = &Model{}

// mode is a mode for the user interface.  Each mode may have different key bindings
type mode uint8

const (
	mode_UNKNOWN = mode(iota)
	// LEADER means the leader key menu is active
	mode_LEADER
	// mode_SEARCH means that the search modal is active.
	mode_SEARCH
	// mode_ERROR means an API error modal is active.
	mode_ERROR
)

// Model contains all the application state
type Model struct {
	svc          blobcache.Service
	root         blobcache.Handle
	currentFQOID blobcache.FQOID
	rootOID      blobcache.OID
	currentOID   blobcache.OID
	previewOID   blobcache.OID

	// pathStack holds the elements of the namespace path
	pathStack []string
	// modes is a stack of the different modes that have been activated.
	// modes are pushed onto the stack when they become active, and this changes the view.
	// then they are popped off the stack usually with a confirm/cancel directive.
	modes []mode

	// current is the cached value for the target volume.
	// This can always be recomputed by looking up the path elements.
	// Whenever handle is set to zero, it must be computed.
	current blobcache.Handle

	rootComponent    component
	currentComponent component
	previewComponent component
	preview          blobcache.Handle

	searchQuery string
	statusLine  string
	errorText   string

	width  int
	height int

	styles uiStyles
}

func New(svc blobcache.Service, root blobcache.Handle) *tea.Program {
	return tea.NewProgram(&Model{
		svc:    svc,
		root:   root,
		styles: defaultStyles(),
	}, tea.WithAltScreen())
}

func (m *Model) Init() tea.Cmd {
	if err := m.refreshAll(context.Background()); err != nil {
		m.reportError(err)
	}
	return tea.WindowSize()
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}

		switch m.currentMode() {
		case mode_ERROR:
			m.updateErrorMode(msg)
			return m, nil
		case mode_LEADER:
			m.updateLeaderMode(msg)
			return m, nil
		case mode_SEARCH:
			m.updateSearchMode(msg)
			return m, nil
		}

		switch msg.String() {
		case "j", "down":
			m.moveCursor(1)
		case "k", "up":
			m.moveCursor(-1)
		case "l", "right":
			m.enterSelection()
		case "h", "left":
			m.exitSelection()
		case "/":
			m.pushMode(mode_SEARCH)
			m.searchQuery = ""
			if c := m.activeComponent(); c != nil {
				c.SetFilter("")
				m.refreshPreview(context.Background())
			}
		case " ":
			m.pushMode(mode_LEADER)
		}
	}
	return m, nil
}

func (m *Model) View() string {
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
	if m.currentFQOID != (blobcache.FQOID{}) {
		topText = m.currentFQOID.String()
	}
	top := m.styles.topBar.Width(width).Render(centerText(topText, width-2))

	status := m.statusLine
	if status == "" {
		status = m.defaultStatusLine()
	}
	statusLine := m.styles.statusBar.Width(width).Render(truncate(status, width-2))

	bodyHeight := height - 5
	if bodyHeight < 8 {
		bodyHeight = 8
	}
	body := m.renderPanes(width, bodyHeight)
	if m.currentMode() == mode_ERROR && m.errorText != "" {
		body = m.renderErrorModal(width, bodyHeight)
	}

	modal := m.modalLine()
	modalStyle := m.styles.modalInactive
	if modal != "" {
		modalStyle = m.styles.modalActive
	}
	modalLine := modalStyle.Width(width).Render(truncate(modal, width-2))

	return lipgloss.JoinVertical(lipgloss.Left, top, statusLine, body, modalLine)
}

func (m *Model) updateLeaderMode(msg tea.KeyMsg) {
	switch msg.String() {
	case "esc":
		m.popMode()
	case "r":
		m.popMode()
		if err := m.refreshAll(context.Background()); err != nil {
			m.reportError(err)
		} else {
			m.statusLine = "refreshed"
		}
	case "y":
		m.popMode()
		if err := m.copySelectionToClipboard(); err != nil {
			if errors.Is(err, errNoSelection) {
				m.statusLine = err.Error()
			} else {
				m.reportError(err)
			}
		}
	}
}

func (m *Model) updateErrorMode(msg tea.KeyMsg) {
	switch msg.String() {
	case "esc", "enter":
		m.errorText = ""
		m.popMode()
	}
}

func (m *Model) updateSearchMode(msg tea.KeyMsg) {
	active := m.activeComponent()
	switch msg.String() {
	case "esc", "enter":
		if active != nil {
			active.SetFilter("")
		}
		m.searchQuery = ""
		m.popMode()
		m.refreshPreview(context.Background())
	case "backspace", "ctrl+h":
		if len(m.searchQuery) > 0 {
			runes := []rune(m.searchQuery)
			m.searchQuery = string(runes[:len(runes)-1])
			if active != nil {
				active.SetFilter(m.searchQuery)
				m.refreshPreview(context.Background())
			}
		}
	case "j", "down":
		m.moveCursor(1)
	case "k", "up":
		m.moveCursor(-1)
	default:
		if msg.Type == tea.KeyRunes {
			m.searchQuery += string(msg.Runes)
			if active != nil {
				active.SetFilter(m.searchQuery)
				m.refreshPreview(context.Background())
			}
		}
	}
}

func (m *Model) moveCursor(delta int) {
	active := m.activeComponent()
	if active == nil {
		return
	}
	active.MoveCursor(delta)
	m.refreshPreview(context.Background())
}

func (m *Model) enterSelection() {
	active := m.activeComponent()
	if active == nil {
		m.statusLine = "no active component"
		return
	}
	name, _, ok, err := active.OpenSelected(context.Background())
	if err != nil {
		m.reportError(err)
		return
	}
	if !ok {
		m.statusLine = "selection cannot be opened"
		return
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
	m.rootOID = resolveVolumeOID(ctx, m.svc, root)

	m.rootComponent, err = loadComponent(ctx, m.svc, root)
	if err != nil {
		m.rootComponent = newMessageComponent("", "failed to load root", err.Error())
	}

	if len(m.pathStack) == 0 {
		m.current = root
		m.currentComponent = m.rootComponent
		m.currentOID = m.rootOID
	} else {
		current, depth, err := m.resolveCurrent(ctx, root)
		if err != nil {
			m.reportError(err)
		}
		if depth < len(m.pathStack) {
			m.pathStack = append([]string(nil), m.pathStack[:depth]...)
		}
		m.current = current
		m.currentOID = resolveVolumeOID(ctx, m.svc, current)
		m.currentComponent, err = loadComponent(ctx, m.svc, current)
		if err != nil {
			m.currentComponent = newMessageComponent("", "failed to load current", err.Error())
		}
	}

	if len(m.pathStack) > 0 {
		if x, ok := m.rootComponent.(interface{ SelectName(string) bool }); ok {
			x.SelectName(m.pathStack[0])
		}
	}

	if m.currentMode() == mode_SEARCH {
		if active := m.activeComponent(); active != nil {
			active.SetFilter(m.searchQuery)
		}
	}

	if fqoid, err := resolveFQOID(ctx, m.svc, m.current); err == nil {
		m.currentFQOID = fqoid
	} else {
		m.currentFQOID = blobcache.FQOID{}
		m.reportError(fmt.Errorf("resolving current FQOID: %w", err))
	}

	m.refreshPreview(ctx)
	return nil
}

func (m *Model) resolveCurrent(ctx context.Context, root blobcache.Handle) (blobcache.Handle, int, error) {
	current := root
	for i, name := range m.pathStack {
		next, err := openNamespaceEntryByName(ctx, m.svc, current, name)
		if err != nil {
			return current, i, err
		}
		current = next
	}
	return current, len(m.pathStack), nil
}

func (m *Model) refreshPreview(ctx context.Context) {
	active := m.activeComponent()
	if active == nil {
		m.preview = blobcache.Handle{}
		m.previewOID = blobcache.OID{}
		m.previewComponent = newMessageComponent("", "(no active component)")
		return
	}
	_, next, ok, err := active.OpenSelected(ctx)
	if err != nil {
		m.preview = blobcache.Handle{}
		m.previewOID = blobcache.OID{}
		m.previewComponent = newMessageComponent("", "preview error", err.Error())
		m.reportError(err)
		return
	}
	if !ok {
		m.preview = blobcache.Handle{}
		m.previewOID = blobcache.OID{}
		m.previewComponent = newMessageComponent("", "(no preview)")
		return
	}
	m.preview = next
	m.previewOID = resolveVolumeOID(ctx, m.svc, next)
	preview, err := loadComponent(ctx, m.svc, next)
	if err != nil {
		m.preview = blobcache.Handle{}
		m.previewOID = blobcache.OID{}
		m.previewComponent = newMessageComponent("", "preview error", err.Error())
		m.reportError(err)
		return
	}
	m.previewComponent = preview
}

func (m *Model) activeComponent() component {
	if len(m.pathStack) == 0 {
		return m.rootComponent
	}
	return m.currentComponent
}

func (m *Model) renderPanes(width, height int) string {
	type paneSpec struct {
		oid     blobcache.OID
		comp    component
		focused bool
	}

	activeIsCurrent := len(m.pathStack) > 0
	panes := []paneSpec{}
	if len(m.pathStack) == 0 {
		panes = append(panes,
			paneSpec{oid: m.rootOID, comp: m.rootComponent, focused: !activeIsCurrent},
			paneSpec{oid: m.previewOID, comp: m.previewComponent, focused: false},
		)
	} else {
		panes = append(panes,
			paneSpec{oid: m.rootOID, comp: m.rootComponent, focused: false},
			paneSpec{oid: m.currentOID, comp: m.currentComponent, focused: true},
			paneSpec{oid: m.previewOID, comp: m.previewComponent, focused: false},
		)
	}

	sepWidth := len(panes) - 1
	contentWidth := width - sepWidth
	if contentWidth < len(panes) {
		contentWidth = len(panes)
	}

	colWidths := make([]int, len(panes))
	base := contentWidth / len(panes)
	extra := contentWidth % len(panes)
	for i := range panes {
		colWidths[i] = base
		if i < extra {
			colWidths[i]++
		}
	}

	paneBlocks := make([]string, len(panes))
	for i := range panes {
		paneBlocks[i] = m.renderPane(panes[i].oid, panes[i].comp, panes[i].focused, colWidths[i], height)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, paneBlocks...)
}

func (m *Model) renderPane(oid blobcache.OID, comp component, focused bool, width, height int) string {
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
	if comp != nil {
		rows = comp.RenderRows(innerWidth, bodyHeight, focused)
	}
	body := strings.Join(rows, "\n")

	headerStyle := m.styles.paneHeader
	paneStyle := m.styles.pane
	if focused {
		headerStyle = m.styles.paneHeaderActive
		paneStyle = m.styles.paneActive
	}
	title := m.paneTitle(oid, innerWidth)
	header := headerStyle.Width(innerWidth).Render(title)
	content := lipgloss.JoinVertical(lipgloss.Left, header, body)
	return paneStyle.Width(width).Height(height).Render(content)
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

func (m *Model) paneTitle(oid blobcache.OID, width int) string {
	if oid == (blobcache.OID{}) {
		return ""
	}
	return truncateMiddle(oid.String(), width)
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

func (m *Model) defaultStatusLine() string {
	switch m.currentMode() {
	case mode_ERROR:
		return "error mode"
	case mode_LEADER:
		return "menu mode"
	case mode_SEARCH:
		return "search mode"
	default:
		return "hjkl/arrows move  l/right enter  h/left back  / search  space menu  q quit"
	}
}

func (m *Model) modalLine() string {
	switch m.currentMode() {
	case mode_ERROR:
		return "error: esc/enter dismiss"
	case mode_LEADER:
		return "menu: r refresh, y copy selected item, esc cancel"
	case mode_SEARCH:
		return fmt.Sprintf("search (case-sensitive substring): %s", m.searchQuery)
	default:
		return ""
	}
}

func (m *Model) copySelectionToClipboard() error {
	active := m.activeComponent()
	if active == nil {
		return fmt.Errorf("no active component")
	}
	value, err := active.CopySelected()
	if err != nil {
		return err
	}
	seq := osc52.New(value)
	if os.Getenv("TMUX") != "" {
		seq = seq.Tmux()
	} else if strings.Contains(os.Getenv("TERM"), "screen") {
		seq = seq.Screen()
	}
	if _, err := seq.WriteTo(os.Stderr); err != nil {
		return err
	}
	m.statusLine = fmt.Sprintf("copied %d bytes", len(value))
	return nil
}

func (m *Model) reportError(err error) {
	if err == nil {
		return
	}
	m.errorText = err.Error()
	m.statusLine = ""
	if m.currentMode() != mode_ERROR {
		m.pushMode(mode_ERROR)
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
	hint := m.styles.errorHint.Width(w - 6).Render("Press esc or enter to dismiss")
	box := m.styles.errorBox.Width(w).Render(lipgloss.JoinVertical(lipgloss.Left, title, body, hint))
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, box)
}

func (m *Model) currentMode() mode {
	if len(m.modes) == 0 {
		return mode_UNKNOWN
	}
	return m.modes[len(m.modes)-1]
}

func (m *Model) pushMode(md mode) {
	m.modes = append(m.modes, md)
}

func (m *Model) popMode() {
	if len(m.modes) == 0 {
		return
	}
	m.modes = m.modes[:len(m.modes)-1]
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
