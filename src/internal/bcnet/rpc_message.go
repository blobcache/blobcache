package bcnet

import (
	"encoding/binary"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

type InspectHandleReq struct {
	Handle blobcache.Handle
}

func (ir InspectHandleReq) Marshal(out []byte) []byte {
	return ir.Handle.Marshal(out)
}

func (ir *InspectHandleReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal InspectHandleReq, too short: %d", len(data))
	}
	return ir.Handle.Unmarshal(data)
}

type InspectHandleResp struct {
	Info blobcache.HandleInfo
}

func (ir InspectHandleResp) Marshal(out []byte) []byte {
	return ir.Info.Marshal(out)
}

func (ir *InspectHandleResp) Unmarshal(data []byte) error {
	return ir.Info.Unmarshal(data)
}

type DropReq struct {
	Handle blobcache.Handle
}

func (dr DropReq) Marshal(out []byte) []byte {
	return dr.Handle.Marshal(out)
}

func (dr *DropReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal DropReq, too short: %d", len(data))
	}
	return dr.Handle.Unmarshal(data)
}

type DropResp struct{}

func (dr DropResp) Marshal(out []byte) []byte {
	return out
}

func (dr *DropResp) Unmarshal(data []byte) error {
	return nil
}

type KeepAliveReq struct {
	Handles []blobcache.Handle
}

func (kr KeepAliveReq) Marshal(out []byte) []byte {
	for _, h := range kr.Handles {
		out = h.Marshal(out)
	}
	return out
}

func (kr *KeepAliveReq) Unmarshal(data []byte) error {
	kr.Handles = make([]blobcache.Handle, len(data)/blobcache.HandleSize)
	for i := range kr.Handles {
		data := data[i*blobcache.HandleSize : (i+1)*blobcache.HandleSize]
		if err := kr.Handles[i].Unmarshal(data); err != nil {
			return err
		}
	}
	return nil
}

type KeepAliveResp struct{}

func (kr KeepAliveResp) Marshal(out []byte) []byte {
	return out
}

func (kr *KeepAliveResp) Unmarshal(data []byte) error {
	return nil
}

type OpenAsReq struct {
	Target blobcache.OID
	Mask   blobcache.ActionSet
}

func (oa OpenAsReq) Marshal(out []byte) []byte {
	out = oa.Target.Marshal(out)
	out = binary.BigEndian.AppendUint64(out, uint64(oa.Mask))
	return out
}

func (oa *OpenAsReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.OIDSize+8 {
		return fmt.Errorf("cannot unmarshal OpenAsReq, too short: %d", len(data))
	}
	oa.Target = blobcache.OID(data[:blobcache.OIDSize])
	oa.Mask = blobcache.ActionSet(binary.BigEndian.Uint64(data[blobcache.OIDSize:]))
	return nil
}

type OpenAsResp struct {
	Handle blobcache.Handle
	Info   blobcache.VolumeInfo
}

func (oa OpenAsResp) Marshal(out []byte) []byte {
	out = oa.Handle.Marshal(out)
	return oa.Info.Marshal(out)
}

func (oa *OpenAsResp) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal OpenAsResp, too short: %d", len(data))
	}
	if err := oa.Handle.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	if err := oa.Info.Unmarshal(data); err != nil {
		return fmt.Errorf("cannot unmarshal OpenAsResp.Info: %w", err)
	}
	return nil
}

type OpenFromReq struct {
	Base   blobcache.Handle
	Target blobcache.OID
	Mask   blobcache.ActionSet
}

func (of OpenFromReq) Marshal(out []byte) []byte {
	out = of.Base.Marshal(out)
	out = of.Target.Marshal(out)
	out = binary.BigEndian.AppendUint64(out, uint64(of.Mask))
	return out
}

func (of *OpenFromReq) Unmarshal(data []byte) error {
	var maskBuf [8]byte
	if err := unmarshalSections(data, [][]byte{
		of.Base.OID[:], of.Base.Secret[:],
		of.Target[:],
		maskBuf[:],
	}); err != nil {
		return err
	}
	of.Mask = blobcache.ActionSet(binary.BigEndian.Uint64(maskBuf[:]))
	return nil
}

type OpenFromResp struct {
	Handle blobcache.Handle
	Info   blobcache.VolumeInfo
}

func (of OpenFromResp) Marshal(out []byte) []byte {
	out = of.Handle.Marshal(out)
	return of.Info.Marshal(out)
}

func (of *OpenFromResp) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal OpenFromResp, too short: %d", len(data))
	}
	if err := of.Handle.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	return of.Info.Unmarshal(data)
}

type InspectVolumeReq struct {
	Volume blobcache.Handle
}

func (iv InspectVolumeReq) Marshal(out []byte) []byte {
	return iv.Volume.Marshal(out)
}

func (iv *InspectVolumeReq) Unmarshal(data []byte) error {
	return iv.Volume.Unmarshal(data)
}

type InspectVolumeResp struct {
	Info blobcache.VolumeInfo
}

func (iv InspectVolumeResp) Marshal(out []byte) []byte {
	return iv.Info.Marshal(out)
}

func (iv *InspectVolumeResp) Unmarshal(data []byte) error {
	return iv.Info.Unmarshal(data)
}

type AwaitReq struct {
	Cond blobcache.Conditions
}

func (ar AwaitReq) Marshal(out []byte) []byte {
	return ar.Cond.Marshal(out)
}

func (ar *AwaitReq) Unmarshal(data []byte) error {
	return ar.Cond.Unmarshal(data)
}

type AwaitResp struct{}

func (ar AwaitResp) Marshal(out []byte) []byte {
	return out
}

func (ar *AwaitResp) Unmarshal(data []byte) error {
	return nil
}

type BeginTxReq struct {
	Volume blobcache.Handle
	Params blobcache.TxParams
}

func (btx BeginTxReq) Marshal(out []byte) []byte {
	out = btx.Volume.Marshal(out)
	return btx.Params.Marshal(out)
}

func (btx *BeginTxReq) Unmarshal(data []byte) error {
	if err := btx.Volume.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	return btx.Params.Unmarshal(data)
}

type BeginTxResp struct {
	// Tx is the handle for the transaction.
	Tx blobcache.Handle
	// VolumeInfo is the volume info for the transaction.
	VolumeInfo blobcache.VolumeInfo
}

func (btx BeginTxResp) Marshal(out []byte) []byte {
	out = btx.Tx.Marshal(out)
	return btx.VolumeInfo.Marshal(out)
}

func (btx *BeginTxResp) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal BeginTxResp, too short: %d", len(data))
	}
	if err := btx.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	return btx.VolumeInfo.Unmarshal(data)
}

type InspectTxReq struct {
	Tx blobcache.Handle
}

func (r InspectTxReq) Marshal(out []byte) []byte {
	return r.Tx.Marshal(out)
}

func (r *InspectTxReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal InspectTxReq, too short: %d", len(data))
	}
	return r.Tx.Unmarshal(data)
}

type InspectTxResp struct {
	Info blobcache.TxInfo
}

func (r InspectTxResp) Marshal(out []byte) []byte {
	return r.Info.Marshal(out)
}

func (r *InspectTxResp) Unmarshal(data []byte) error {
	return r.Info.Unmarshal(data)
}

type CommitReq struct {
	Tx blobcache.Handle
	// Root can be optionally set to call Save before Commit.
	Root *[]byte
}

func (cr CommitReq) Marshal(out []byte) []byte {
	out = cr.Tx.Marshal(out)
	if cr.Root != nil {
		out = append(out, *cr.Root...)
	}
	return out
}

func (cr *CommitReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal CommitReq, too short: %d", len(data))
	}
	if err := cr.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	if len(data) > blobcache.HandleSize {
		if cr.Root == nil {
			cr.Root = new([]byte)
		}
		*cr.Root = append((*cr.Root)[:0], data[blobcache.HandleSize:]...)
	}
	return nil
}

type CommitResp struct{}

func (cr CommitResp) Marshal(out []byte) []byte {
	return out
}

func (cr *CommitResp) Unmarshal(data []byte) error {
	return nil
}

type AbortReq struct {
	Tx blobcache.Handle
}

func (ar AbortReq) Marshal(out []byte) []byte {
	return ar.Tx.Marshal(out)
}

func (ar *AbortReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal AbortReq, too short: %d", len(data))
	}
	return ar.Tx.Unmarshal(data)
}

type AbortResp struct{}

func (ar AbortResp) Marshal(out []byte) []byte {
	return out
}

func (ar *AbortResp) Unmarshal(data []byte) error {
	return nil
}

type LoadReq struct {
	Tx blobcache.Handle
}

func (lr LoadReq) Marshal(out []byte) []byte {
	out = lr.Tx.Marshal(out)
	return out
}

func (lr *LoadReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal LoadReq, too short: %d", len(data))
	}
	return lr.Tx.Unmarshal(data)
}

type LoadResp struct {
	Root []byte
}

func (lr LoadResp) Marshal(out []byte) []byte {
	return append(out, lr.Root...)
}

func (lr *LoadResp) Unmarshal(data []byte) error {
	lr.Root = append(lr.Root[:0], data...)
	return nil
}

type SaveReq struct {
	Tx   blobcache.Handle
	Root []byte
}

func (r SaveReq) Marshal(out []byte) []byte {
	out = r.Tx.Marshal(out)
	return append(out, r.Root...)
}

func (r *SaveReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal SaveReq, too short: %d", len(data))
	}
	if err := r.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	r.Root = append(r.Root[:0], data[blobcache.HandleSize:]...)
	return nil
}

type SaveResp struct{}

func (r SaveResp) Marshal(out []byte) []byte {
	return out
}

func (r *SaveResp) Unmarshal(data []byte) error {
	return nil
}

type ExistsReq struct {
	Tx   blobcache.Handle
	CIDs []blobcache.CID
}

func (er ExistsReq) Marshal(out []byte) []byte {
	out = er.Tx.Marshal(out)
	for _, cid := range er.CIDs {
		out = append(out, cid[:]...)
	}
	return out
}

func (er *ExistsReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal ExistsReq, too short: %d", len(data))
	}
	if err := er.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	cidData := data[blobcache.HandleSize:]
	if len(cidData)%blobcache.CIDSize != 0 {
		return fmt.Errorf("cannot unmarshal ExistsReq, CID data length is not a multiple of %d: %d", blobcache.CIDSize, len(cidData))
	}
	er.CIDs = make([]blobcache.CID, len(cidData)/blobcache.CIDSize)
	for i := range er.CIDs {
		beg := i * blobcache.CIDSize
		end := beg + blobcache.CIDSize
		copy(er.CIDs[i][:], cidData[beg:end])
	}
	return nil
}

type ExistsResp struct {
	Exists []bool
}

func (er ExistsResp) Marshal(out []byte) []byte {
	for i := range er.Exists {
		if i%8 == 0 {
			out = append(out, 0)
		}
		if er.Exists[i] {
			out[len(out)-1] |= 1 << (i % 8)
		}
	}
	return out
}

func (er *ExistsResp) Unmarshal(data []byte) error {
	er.Exists = er.Exists[:0]
	for i := range data {
		for j := 0; j < 8; j++ {
			if (data[i] & (1 << j)) != 0 {
				er.Exists = append(er.Exists, true)
			} else {
				er.Exists = append(er.Exists, false)
			}
		}
	}
	return nil
}

type DeleteReq struct {
	Tx  blobcache.Handle
	CID blobcache.CID
}

func (dr DeleteReq) Marshal(out []byte) []byte {
	out = dr.Tx.Marshal(out)
	out = append(out, dr.CID[:]...)
	return out
}

func (dr *DeleteReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize+blobcache.CIDSize {
		return fmt.Errorf("cannot unmarshal DeleteReq, too short: %d", len(data))
	}
	if err := dr.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	dr.CID = blobcache.CID(data[blobcache.HandleSize : blobcache.HandleSize+blobcache.CIDSize])
	return nil
}

type DeleteResp struct{}

func (dr DeleteResp) Marshal(out []byte) []byte {
	return out
}

func (dr *DeleteResp) Unmarshal(data []byte) error {
	return nil
}

type GetReq struct {
	Tx   blobcache.Handle
	CID  blobcache.CID
	Salt *blobcache.CID
}

func (gr GetReq) Marshal(out []byte) []byte {
	out = gr.Tx.Marshal(out)
	out = append(out, gr.CID[:]...)
	if gr.Salt != nil {
		out = append(out, gr.Salt[:]...)
	}
	return out
}

func (gr *GetReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize+blobcache.CIDSize {
		return fmt.Errorf("cannot unmarshal GetReq, too short: %d", len(data))
	}
	if err := gr.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	gr.CID = blobcache.CID(data[blobcache.HandleSize : blobcache.HandleSize+blobcache.CIDSize])
	if len(data) > blobcache.HandleSize+blobcache.CIDSize+blobcache.CIDSize {
		gr.Salt = new(blobcache.CID)
		copy(gr.Salt[:], data[blobcache.HandleSize+blobcache.CIDSize:])
	}
	return nil
}

type GetResp struct {
	Data []byte
}

func (gr GetResp) Marshal(out []byte) []byte {
	return append(out, gr.Data...)
}

func (gr *GetResp) Unmarshal(data []byte) error {
	gr.Data = append(gr.Data[:0], data...)
	return nil
}

type AllowLinkReq struct {
	Tx     blobcache.Handle
	Subvol blobcache.Handle
}

func (ar AllowLinkReq) Marshal(out []byte) []byte {
	out = ar.Tx.Marshal(out)
	out = ar.Subvol.Marshal(out)
	return out
}

func (ar *AllowLinkReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize*2 {
		return fmt.Errorf("cannot unmarshal AllowLinkReq, too short: %d", len(data))
	}
	if err := ar.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	if err := ar.Subvol.Unmarshal(data[blobcache.HandleSize:]); err != nil {
		return err
	}
	return nil
}

type AllowLinkResp struct{}

func (ar AllowLinkResp) Marshal(out []byte) []byte {
	return out
}

func (ar *AllowLinkResp) Unmarshal(data []byte) error {
	return nil
}

type CreateVolumeReq struct {
	Spec blobcache.VolumeSpec
}

func (cr CreateVolumeReq) Marshal(out []byte) []byte {
	return cr.Spec.Marshal(out)
}

func (cr CreateVolumeReq) Unmarshal(data []byte) error {
	return cr.Spec.Unmarshal(data)
}

type CreateVolumeResp struct {
	Handle blobcache.Handle
	Info   blobcache.VolumeInfo
}

func (cr CreateVolumeResp) Marshal(out []byte) []byte {
	out = cr.Handle.Marshal(out)
	return cr.Info.Marshal(out)
}

func (cr *CreateVolumeResp) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal CreateVolumeReq, too short: %d", len(data))
	}
	if err := cr.Handle.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	return cr.Info.Unmarshal(data)
}

// unmarshalSections unmarshals according to the buffers passed in sections.
// The data must contain fixed sized sections.
func unmarshalSections(data []byte, sections [][]byte) error {
	for i := range sections {
		if len(data) < len(sections[i]) {
			return fmt.Errorf("cannot unmarshal sections, data too short: %d < %d", len(data), len(sections[i]))
		}
		copy(sections[i], data[:len(sections[i])])
		data = data[len(sections[i]):]
	}
	return nil
}
