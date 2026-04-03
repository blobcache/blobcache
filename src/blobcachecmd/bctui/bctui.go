package bctui

import (
	"blobcache.io/blobcache/src/blobcache"
	tea "github.com/charmbracelet/bubbletea"
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
)

// Model contains all the application state
type Model struct {
	svc  blobcache.Service
	root blobcache.Handle
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
}

func New(svc blobcache.Service, root blobcache.Handle) *tea.Program {
	return tea.NewProgram(&Model{
		svc:  svc,
		root: root,
	})
}

func (m *Model) Init() tea.Cmd {
	return func() tea.Msg {
		return struct{}{}
	}
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m *Model) View() string {
	return "blobcache \n\npress q to quit\n"
}
