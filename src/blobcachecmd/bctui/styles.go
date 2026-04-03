package bctui

import "github.com/charmbracelet/lipgloss"

type uiStyles struct {
	topBar           lipgloss.Style
	statusBar        lipgloss.Style
	pane             lipgloss.Style
	paneActive       lipgloss.Style
	paneHeader       lipgloss.Style
	paneHeaderActive lipgloss.Style
	modalActive      lipgloss.Style
	modalInactive    lipgloss.Style
	errorBox         lipgloss.Style
	errorTitle       lipgloss.Style
	errorBody        lipgloss.Style
	errorHint        lipgloss.Style
}

func defaultStyles() uiStyles {
	return uiStyles{
		topBar: lipgloss.NewStyle().
			Background(lipgloss.Color("236")).
			Foreground(lipgloss.Color("252")).
			Padding(0, 1),

		statusBar: lipgloss.NewStyle().
			Foreground(lipgloss.Color("249")).
			Background(lipgloss.Color("238")).
			Padding(0, 1),

		pane: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(0, 1),

		paneActive: lipgloss.NewStyle().
			Border(lipgloss.ThickBorder()).
			BorderForeground(lipgloss.Color("81")).
			Padding(0, 1),

		paneHeader: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("250")).
			BorderBottom(true).
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("239")),

		paneHeaderActive: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("230")).
			Background(lipgloss.Color("24")).
			Padding(0, 1),

		modalActive: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("230")).
			Background(lipgloss.Color("63")).
			Padding(0, 1),

		modalInactive: lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Background(lipgloss.Color("236")).
			Padding(0, 1),

		errorBox: lipgloss.NewStyle().
			Border(lipgloss.DoubleBorder()).
			BorderForeground(lipgloss.Color("203")).
			Background(lipgloss.Color("52")).
			Foreground(lipgloss.Color("230")).
			Padding(1, 2),

		errorTitle: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("230")).
			Background(lipgloss.Color("124")).
			Padding(0, 1),

		errorBody: lipgloss.NewStyle().
			Foreground(lipgloss.Color("230")).
			Padding(1, 0),

		errorHint: lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")).
			Italic(true),
	}
}
