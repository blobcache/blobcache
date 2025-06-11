package dbmig

import (
	"embed"
	"io/fs"
	"slices"
	"strings"
)

//go:embed *.sql
var migfs embed.FS

func ListMigrations() []string {
	migs, err := loadMigrations()
	if err != nil {
		panic(err)
	}
	return migs
}

func loadMigrations() ([]string, error) {
	ents, err := migfs.ReadDir(".")
	if err != nil {
		return nil, err
	}
	slices.SortFunc(ents, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})
	var ret []string
	for _, ent := range ents {
		data, err := migfs.ReadFile(ent.Name())
		if err != nil {
			return nil, err
		}
		ret = append(ret, string(data))
	}
	return ret, nil
}
