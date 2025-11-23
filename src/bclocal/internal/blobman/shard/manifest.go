package shard

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// ManifestFilename is the filename for the manifest.
// This does not include the shard id, which will be in the directory path.
const ManifestFilename = "MF"

// ManifestSize is the size of the manifest in bytes.
const ManifestSize = 32

// Manifest is the source of truth for a Shard's state.
type Manifest struct {
	// Nonce is the number of times the manifest has been saved.
	Nonce uint64
	// Gen is the Shard's current generation.
	Gen uint32
	// Count is the number of entries in the table with the current generation.
	TableLen uint32
}

func (mf *Manifest) load(data *[ManifestSize]byte) {
	mf.Nonce = binary.LittleEndian.Uint64(data[:8])
	mf.Gen = binary.LittleEndian.Uint32(data[8:12])
	mf.TableLen = binary.LittleEndian.Uint32(data[12:16])
}

func (mf *Manifest) save(buf *[ManifestSize]byte) {
	binary.LittleEndian.PutUint64(buf[:8], mf.Nonce)
	binary.LittleEndian.PutUint32(buf[8:12], mf.Gen)
	binary.LittleEndian.PutUint32(buf[12:16], mf.TableLen)
}

func SaveManifest(shardRoot *os.Root, manifest Manifest) error {
	const fileSize = 2 * ManifestSize
	f, err := shardRoot.OpenFile(ManifestFilename, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	finfo, err := f.Stat()
	if err != nil {
		return err
	}
	if finfo.Size() < fileSize {
		if err := f.Truncate(fileSize); err != nil {
			return err
		}
	}
	var mfData [ManifestSize]byte
	manifest.save(&mfData)

	if manifest.Nonce&1 == 0 {
		if _, err := f.WriteAt(mfData[:], 0); err != nil {
			return err
		}
	} else {
		if _, err := f.WriteAt(mfData[:], fileSize/2); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func LoadManifest(shardRoot *os.Root) (Manifest, error) {
	const fileSize = 2 * ManifestSize
	f, err := shardRoot.OpenFile(ManifestFilename, os.O_RDWR, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Manifest{}, os.ErrNotExist
		}
		return Manifest{}, err
	}
	defer f.Close()
	finfo, err := f.Stat()
	if err != nil {
		return Manifest{}, err
	}
	if finfo.Size() < fileSize {
		return Manifest{}, fmt.Errorf("manifest file size is wrong %d", finfo.Size())
	}
	var data [fileSize]byte
	if _, err := io.ReadFull(f, data[:]); err != nil {
		return Manifest{}, err
	}
	var left, right Manifest
	left.load((*[ManifestSize]byte)(data[:ManifestSize]))
	right.load((*[ManifestSize]byte)(data[ManifestSize:]))
	if right.Nonce > left.Nonce {
		return right, nil
	} else {
		return left, nil
	}
}
