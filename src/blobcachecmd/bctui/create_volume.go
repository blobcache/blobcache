package bctui

import (
	"context"
	"fmt"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

type volumeBackendType int

const (
	volumeBackendLocal volumeBackendType = iota
	volumeBackendRemote
	volumeBackendPeer
	volumeBackendVault
)

var backendNames = []string{"Local", "Remote", "Peer", "Vault"}

type createVolumeState struct {
	activeBackend volumeBackendType
	activeField   int
	status        string

	volumeName string
	nsHandle   blobcache.Handle

	localHashAlgo *HashAlgoPicker
	localMaxSize  string
	localSalted   bool

	remoteNodeID   string
	remoteIPPort   string
	remoteOID      string
	remoteHashAlgo *HashAlgoPicker

	peerNodeID   string
	peerOID      string
	peerHashAlgo *HashAlgoPicker

	vaultHandle   string
	vaultSecret   string
	vaultHashAlgo *HashAlgoPicker
}

func newCreateVolumeState(nsHandle blobcache.Handle) *createVolumeState {
	params := blobcache.DefaultVolumeParams()
	return &createVolumeState{
		activeBackend: volumeBackendLocal,
		activeField:   -2,
		nsHandle:      nsHandle,
		localHashAlgo: NewHashAlgoPicker(params.HashAlgo),
		localMaxSize:  fmt.Sprintf("%d", params.MaxSize),
		localSalted:   params.Salted,
	}
}

func (m *Model) updateCreateVolume(msg tea.KeyPressMsg) {
	cs := m.createVolState
	if cs == nil {
		m.setMode(mode_NORMAL)
		return
	}

	textField := cs.isTextField()
	picker := cs.activePicker()
	isPickerField := picker != nil

	key := msg.String()
	switch key {
	case "esc":
		m.setMode(mode_NORMAL)
		m.createVolState = nil
		return
	case "enter":
		m.cvHandleEnter()
		return
	case "tab":
		cs.status = ""
		maxField := cs.fieldCount() + 1
		if cs.activeField < maxField {
			cs.activeField++
		} else {
			cs.activeField = -2
		}
		return
	case "shift+tab":
		cs.status = ""
		if cs.activeField > -2 {
			cs.activeField--
		} else {
			cs.activeField = cs.fieldCount() + 1
		}
		return
	case "backspace":
		cs.cvDeleteChar()
		return
	}

	switch key {
	case "up":
		if cs.activeField > -2 {
			cs.activeField--
		}
		return
	case "down":
		maxField := cs.fieldCount() + 1
		if cs.activeField < maxField {
			cs.activeField++
		}
		if cs.activeField > maxField {
			cs.activeField = maxField
		}
		return
	case "left":
		if cs.activeField == -1 {
			if cs.activeBackend > 0 {
				cs.activeBackend--
				cs.ensurePicker()
			}
			return
		}
		if isPickerField {
			cs.status = ""
			picker.Prev()
			return
		}
		if cs.activeField > -2 {
			cs.activeField--
		}
		return
	case "right":
		if cs.activeField == -1 {
			if cs.activeBackend < volumeBackendVault {
				cs.activeBackend++
				cs.ensurePicker()
			}
			return
		}
		if isPickerField {
			cs.status = ""
			picker.Next()
			return
		}
		maxField := cs.fieldCount() + 1
		if cs.activeField < maxField {
			cs.activeField++
		}
		if cs.activeField > maxField {
			cs.activeField = maxField
		}
		return
	}

	if !textField {
		switch key {
		case "h":
			if cs.activeField == -1 {
				if cs.activeBackend > 0 {
					cs.activeBackend--
					cs.ensurePicker()
				}
				return
			}
			if isPickerField {
				cs.status = ""
				picker.Prev()
				return
			}
			if cs.activeField > -2 {
				cs.activeField--
			}
			return
		case "l":
			if cs.activeField == -1 {
				if cs.activeBackend < volumeBackendVault {
					cs.activeBackend++
					cs.ensurePicker()
				}
				return
			}
			if isPickerField {
				cs.status = ""
				picker.Next()
				return
			}
			maxField := cs.fieldCount() + 1
			if cs.activeField < maxField {
				cs.activeField++
			}
			if cs.activeField > maxField {
				cs.activeField = maxField
			}
			return
		case "j":
			maxField := cs.fieldCount() + 1
			if cs.activeField < maxField {
				cs.activeField++
			}
			if cs.activeField > maxField {
				cs.activeField = maxField
			}
			return
		case "k":
			if cs.activeField > -2 {
				cs.activeField--
			}
			return
		}
	}

	cs.status = ""
	if text := msg.Key().Text; text != "" {
		cs.cvAppendText(text)
	}
}

func (m *Model) cvHandleEnter() {
	cs := m.createVolState
	if cs == nil {
		return
	}

	n := cs.fieldCount()
	btnCancel := n
	btnCreate := n + 1

	switch {
	case cs.activeField == btnCancel:
		m.setMode(mode_NORMAL)
		m.createVolState = nil
	case cs.activeField == btnCreate:
		m.doCreateVolume()
	default:
		cs.cvToggleBool()
	}
}

func (cs *createVolumeState) isBoolField() bool {
	switch cs.activeBackend {
	case volumeBackendLocal:
		return cs.activeField == 2
	}
	return false
}

func (cs *createVolumeState) isNameField() bool {
	return cs.activeField == -2
}

func (cs *createVolumeState) isTextField() bool {
	if cs.isNameField() {
		return true
	}
	if cs.isBoolField() {
		return false
	}
	if cs.activePicker() != nil {
		return false
	}
	if cs.activeField == -1 {
		return false
	}
	n := cs.fieldCount()
	if cs.activeField >= n {
		return false
	}
	return cs.cvFieldValuePtr() != nil
}

func (cs *createVolumeState) activePicker() *HashAlgoPicker {
	if cs.activeField < 0 {
		return nil
	}
	return cs.pickerAt(cs.activeField)
}

func (cs *createVolumeState) pickerAt(index int) *HashAlgoPicker {
	switch cs.activeBackend {
	case volumeBackendLocal:
		if index == 0 {
			return cs.localHashAlgo
		}
	case volumeBackendRemote:
		if index == 3 {
			return cs.remoteHashAlgo
		}
	case volumeBackendPeer:
		if index == 2 {
			return cs.peerHashAlgo
		}
	case volumeBackendVault:
		if index == 2 {
			return cs.vaultHashAlgo
		}
	}
	return nil
}

func (cs *createVolumeState) ensurePicker() {
	params := blobcache.DefaultVolumeParams()
	switch cs.activeBackend {
	case volumeBackendLocal:
		if cs.localHashAlgo == nil {
			cs.localHashAlgo = NewHashAlgoPicker(params.HashAlgo)
		}
	case volumeBackendRemote:
		if cs.remoteHashAlgo == nil {
			cs.remoteHashAlgo = NewHashAlgoPicker(params.HashAlgo)
		}
	case volumeBackendPeer:
		if cs.peerHashAlgo == nil {
			cs.peerHashAlgo = NewHashAlgoPicker(params.HashAlgo)
		}
	case volumeBackendVault:
		if cs.vaultHashAlgo == nil {
			cs.vaultHashAlgo = NewHashAlgoPicker(params.HashAlgo)
		}
	}
}

func (cs *createVolumeState) cvDeleteChar() {
	ptr := cs.cvFieldValuePtr()
	if ptr == nil {
		return
	}
	runes := []rune(*ptr)
	if len(runes) == 0 {
		return
	}
	*ptr = string(runes[:len(runes)-1])
}

func (cs *createVolumeState) cvAppendText(text string) {
	ptr := cs.cvFieldValuePtr()
	if ptr == nil {
		return
	}
	cs.status = ""
	*ptr += text
}

func (cs *createVolumeState) cvToggleBool() {
	if !cs.isBoolField() {
		return
	}
	switch cs.activeBackend {
	case volumeBackendLocal:
		if cs.activeField == 2 {
			cs.localSalted = !cs.localSalted
		}
	}
}

func (cs *createVolumeState) cvFieldValuePtr() *string {
	if cs.isNameField() {
		return &cs.volumeName
	}
	n := cs.fieldCount()
	if cs.activeField < 0 || cs.activeField >= n {
		return nil
	}
	if cs.activePicker() != nil || cs.isBoolField() {
		return nil
	}

	switch cs.activeBackend {
	case volumeBackendLocal:
		switch cs.activeField {
		case 1:
			return &cs.localMaxSize
		}
	case volumeBackendRemote:
		switch cs.activeField {
		case 0:
			return &cs.remoteNodeID
		case 1:
			return &cs.remoteIPPort
		case 2:
			return &cs.remoteOID
		}
	case volumeBackendPeer:
		switch cs.activeField {
		case 0:
			return &cs.peerNodeID
		case 1:
			return &cs.peerOID
		}
	case volumeBackendVault:
		switch cs.activeField {
		case 0:
			return &cs.vaultHandle
		case 1:
			return &cs.vaultSecret
		}
	}
	return nil
}

func (cs *createVolumeState) fieldCount() int {
	switch cs.activeBackend {
	case volumeBackendLocal:
		return 3
	case volumeBackendRemote:
		return 4
	case volumeBackendPeer:
		return 3
	case volumeBackendVault:
		return 3
	default:
		return 0
	}
}

func (cs *createVolumeState) fieldNames() []string {
	switch cs.activeBackend {
	case volumeBackendLocal:
		return []string{"Hash Algo:", "Max Size:", "Salted:"}
	case volumeBackendRemote:
		return []string{"Node ID:", "IP:Port:", "Volume OID:", "Hash Algo:"}
	case volumeBackendPeer:
		return []string{"Peer:", "Volume OID:", "Hash Algo:"}
	case volumeBackendVault:
		return []string{"Handle:", "Secret:", "Hash Algo:"}
	default:
		return nil
	}
}

func (cs *createVolumeState) fieldValueAt(index int) string {
	switch cs.activeBackend {
	case volumeBackendLocal:
		switch index {
		case 0:
			return string(cs.localHashAlgo.Selected())
		case 1:
			return cs.localMaxSize
		case 2:
			if cs.localSalted {
				return "yes"
			}
			return "no"
		}
	case volumeBackendRemote:
		switch index {
		case 0:
			return cs.remoteNodeID
		case 1:
			return cs.remoteIPPort
		case 2:
			return cs.remoteOID
		case 3:
			if cs.remoteHashAlgo != nil {
				return string(cs.remoteHashAlgo.Selected())
			}
		}
	case volumeBackendPeer:
		switch index {
		case 0:
			return cs.peerNodeID
		case 1:
			return cs.peerOID
		case 2:
			if cs.peerHashAlgo != nil {
				return string(cs.peerHashAlgo.Selected())
			}
		}
	case volumeBackendVault:
		switch index {
		case 0:
			return cs.vaultHandle
		case 1:
			return cs.vaultSecret
		case 2:
			if cs.vaultHashAlgo != nil {
				return string(cs.vaultHashAlgo.Selected())
			}
		}
	}
	return ""
}

func (m *Model) doCreateVolume() {
	cs := m.createVolState
	if cs == nil {
		return
	}

	spec, err := cs.buildVolumeSpec()
	if err != nil {
		cs.status = "error: " + err.Error()
		return
	}

	ctx := context.Background()
	h, err := m.nsc.CreateVolumeFrom(ctx, cs.nsHandle, cs.volumeName, spec)
	if err != nil {
		cs.status = "error: " + err.Error()
		return
	}

	cs.status = "created: " + h.String()
	m.statusLine = "volume created: " + h.String()
	m.setMode(mode_NORMAL)
	m.createVolState = nil

	if err := m.refreshAll(ctx); err != nil {
		m.reportError(err)
	}
}

func (cs *createVolumeState) buildVolumeSpec() (blobcache.VolumeSpec, error) {
	switch cs.activeBackend {
	case volumeBackendLocal:
		return cs.buildLocalSpec()
	case volumeBackendRemote:
		return cs.buildRemoteSpec()
	case volumeBackendPeer:
		return cs.buildPeerSpec()
	case volumeBackendVault:
		return cs.buildVaultSpec()
	default:
		return blobcache.VolumeSpec{}, fmt.Errorf("unknown backend")
	}
}

func (cs *createVolumeState) buildLocalSpec() (blobcache.VolumeSpec, error) {
	var maxSize int64
	if cs.localMaxSize != "" {
		if _, err := fmt.Sscanf(cs.localMaxSize, "%d", &maxSize); err != nil {
			return blobcache.VolumeSpec{}, fmt.Errorf("invalid max-size: %w", err)
		}
	}
	var hashAlgo blobcache.HashAlgo
	if cs.localHashAlgo != nil {
		hashAlgo = cs.localHashAlgo.Selected()
	}
	return blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			Schema:   blobcache.SchemaSpec{Name: blobcache.Schema_NONE},
			HashAlgo: hashAlgo,
			MaxSize:  maxSize,
			Salted:   cs.localSalted,
		},
	}, nil
}

func (cs *createVolumeState) buildRemoteSpec() (blobcache.VolumeSpec, error) {
	endpointStr := cs.remoteNodeID
	if cs.remoteIPPort != "" {
		endpointStr = cs.remoteNodeID + ":" + cs.remoteIPPort
	}
	ep, err := blobcache.ParseEndpoint(endpointStr)
	if err != nil {
		return blobcache.VolumeSpec{}, fmt.Errorf("invalid endpoint: %w", err)
	}
	oid, err := blobcache.ParseOID(cs.remoteOID)
	if err != nil {
		return blobcache.VolumeSpec{}, fmt.Errorf("invalid volume OID: %w", err)
	}
	var hashAlgo blobcache.HashAlgo
	if cs.remoteHashAlgo != nil {
		hashAlgo = cs.remoteHashAlgo.Selected()
	}
	return blobcache.VolumeSpec{
		Remote: &blobcache.VolumeBackend_Remote{
			Endpoint: ep,
			Volume:   oid,
			HashAlgo: hashAlgo,
		},
	}, nil
}

func (cs *createVolumeState) buildPeerSpec() (blobcache.VolumeSpec, error) {
	oid, err := blobcache.ParseOID(cs.peerOID)
	if err != nil {
		return blobcache.VolumeSpec{}, fmt.Errorf("invalid volume OID: %w", err)
	}
	var hashAlgo blobcache.HashAlgo
	if cs.peerHashAlgo != nil {
		hashAlgo = cs.peerHashAlgo.Selected()
	}
	return blobcache.VolumeSpec{
		Peer: &blobcache.VolumeBackend_Peer{
			Peer:     blobcache.NodeID{},
			Volume:   oid,
			HashAlgo: hashAlgo,
		},
	}, nil
}

func (cs *createVolumeState) buildVaultSpec() (blobcache.VolumeSpec, error) {
	h, err := blobcache.ParseHandle(cs.vaultHandle)
	if err != nil {
		return blobcache.VolumeSpec{}, fmt.Errorf("invalid handle: %w", err)
	}
	var secret blobcache.Secret
	if cs.vaultSecret != "" {
		if err := secret.UnmarshalText([]byte(cs.vaultSecret)); err != nil {
			return blobcache.VolumeSpec{}, fmt.Errorf("invalid secret: %w", err)
		}
	}
	var hashAlgo blobcache.HashAlgo
	if cs.vaultHashAlgo != nil {
		hashAlgo = cs.vaultHashAlgo.Selected()
	}
	return blobcache.VolumeSpec{
		Vault: &blobcache.VolumeBackend_Vault[blobcache.Handle]{
			X:        h,
			Secret:   secret,
			HashAlgo: hashAlgo,
		},
	}, nil
}

func (m *Model) renderCreateVolumeOverlay(width, height int, base string) string {
	cs := m.createVolState
	if cs == nil {
		return base
	}

	w := width - 10
	if w > 72 {
		w = 72
	}
	if w < 30 {
		w = width
	}
	bodyWidth := w - 6
	if bodyWidth < 1 {
		bodyWidth = 1
	}

	var lines []string

	title := m.styles.cvTitle.Width(bodyWidth).Render("Create Volume")
	lines = append(lines, title)

	nameLine := m.cvRenderNameField(cs, bodyWidth)
	lines = append(lines, nameLine)

	lines = append(lines, "")

	tabsStr := m.cvRenderTabs(cs, bodyWidth)
	lines = append(lines, tabsStr)

	lines = append(lines, "")

	fields := m.cvRenderFields(cs, bodyWidth)
	lines = append(lines, fields...)

	if cs.status != "" {
		lines = append(lines, "")
		statusLine := m.styles.cvStatus.Width(bodyWidth).Render(truncate(cs.status, bodyWidth))
		lines = append(lines, statusLine)
	}

	lines = append(lines, "")

	buttons := m.cvRenderButtons(cs, bodyWidth)
	lines = append(lines, buttons)
	lines = append(lines, "")

	hint := m.styles.cvHint.Render("Press ") +
		m.keyTextIn(m.styles.cvHint, "esc") +
		m.styles.cvHint.Render(" to cancel, ") +
		m.keyTextIn(m.styles.cvHint, "enter") +
		m.styles.cvHint.Render(" to create")
	lines = append(lines, hint)

	body := m.styles.cvBody.Width(bodyWidth).Render(strings.Join(lines, "\n"))
	box := m.styles.cvBox.Width(w).Render(body)

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

func (m *Model) cvRenderNameField(cs *createVolumeState, width int) string {
	labelStyle := m.styles.cvFieldLabel
	labelRendered := labelStyle.Render("Name:")
	labelW := lipgloss.Width(labelRendered)
	valueW := width - labelW - 1
	if valueW < 1 {
		valueW = 1
	}

	val := cs.volumeName
	if cs.activeField == -2 {
		valStyled := " " + m.styles.cvFieldValueFoc.Width(valueW-1).Render(truncate(val, valueW-1))
		return labelRendered + " " + valStyled
	}
	valStyled := m.styles.cvFieldValue.Width(valueW).Render(truncate(val, valueW))
	return labelRendered + " " + valStyled
}

func (m *Model) cvRenderTabs(cs *createVolumeState, width int) string {
	if width < 1 {
		return ""
	}
	var parts []string
	for i, name := range backendNames {
		style := m.styles.cvTabInactive
		if volumeBackendType(i) == cs.activeBackend {
			style = m.styles.cvTabActive
		}
		parts = append(parts, style.Render(name))
	}
	line := strings.Join(parts, " ")
	return padOrTrim(line, width)
}

func (m *Model) cvRenderFields(cs *createVolumeState, width int) []string {
	names := cs.fieldNames()
	n := cs.fieldCount()
	labelStyle := m.styles.cvFieldLabel
	valueStyle := m.styles.cvFieldValue
	focusStyle := m.styles.cvFieldValueFoc

	var lines []string
	for i := 0; i < n; i++ {
		label := names[i]
		labelRendered := labelStyle.Render(label)
		labelW := lipgloss.Width(labelRendered)
		valueW := width - labelW - 1
		if valueW < 1 {
			valueW = 1
		}

		var valStyled string
		focused := cs.activeField == i
		if p := cs.pickerAt(i); p != nil {
			valStyled = " " + p.Render(valueW-1, focused, m.styles)
		} else {
			val := cs.fieldValueAt(i)
			valStyled = valueStyle.Width(valueW).Render(truncate(val, valueW))
			if focused {
				valStyled = " " + focusStyle.Width(valueW-1).Render(truncate(val, valueW-1))
			}
		}
		lines = append(lines, labelRendered+" "+valStyled)
	}

	return lines
}

func (m *Model) cvRenderButtons(cs *createVolumeState, width int) string {
	n := cs.fieldCount()
	btnCancel := n
	btnCreate := n + 1

	cancelStyle := m.styles.cvButtonInactive
	createStyle := m.styles.cvButtonInactive
	if cs.activeField == btnCancel {
		cancelStyle = m.styles.cvButtonActive
	}
	if cs.activeField == btnCreate {
		createStyle = m.styles.cvButtonActive
	}

	cancelText := cancelStyle.Render("Cancel")
	createText := createStyle.Render("Create")
	combined := cancelText + "  " + createText
	return padOrTrim(combined, width)
}