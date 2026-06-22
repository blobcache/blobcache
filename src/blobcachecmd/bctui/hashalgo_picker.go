package bctui

import (
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"charm.land/lipgloss/v2"
)

var allHashAlgos = []blobcache.HashAlgo{
	blobcache.HashAlgo_BLAKE3_256,
	blobcache.HashAlgo_BLAKE2b_256,
	blobcache.HashAlgo_SHA2_256,
	blobcache.HashAlgo_SHA3_256,
	blobcache.HashAlgo_CSHAKE256,
}

type HashAlgoPicker struct {
	algos    []blobcache.HashAlgo
	selected int
}

func NewHashAlgoPicker(initial blobcache.HashAlgo) *HashAlgoPicker {
	p := &HashAlgoPicker{
		algos: allHashAlgos,
	}
	for i, ha := range p.algos {
		if ha == initial {
			p.selected = i
			break
		}
	}
	return p
}

func (p *HashAlgoPicker) Selected() blobcache.HashAlgo {
	return p.algos[p.selected]
}

func (p *HashAlgoPicker) Next() {
	p.selected = (p.selected + 1) % len(p.algos)
}

func (p *HashAlgoPicker) Prev() {
	p.selected = (p.selected - 1 + len(p.algos)) % len(p.algos)
}

func (p *HashAlgoPicker) Render(width int, focused bool, styles uiStyles) string {
	var parts []string
	activeStyle := styles.controlsKey
	inactiveStyle := styles.controlsDesc

	if width < 1 {
		return ""
	}

	for i, ha := range p.algos {
		s := inactiveStyle
		if i == p.selected {
			if focused {
				s = activeStyle
			} else {
				s = activeStyle.Inherit(inactiveStyle)
			}
		}
		parts = append(parts, s.Render(string(ha)))
	}

	line := strings.Join(parts, " ")
	if lipgloss.Width(line) > width {
		line = strings.Join(parts, "  ")
		if lipgloss.Width(line) > width {
			line = parts[p.selected]
		}
	}

	return padOrTrim(line, width)
}
