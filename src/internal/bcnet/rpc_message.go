package bcnet

import (
	"encoding/json"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
)

type InspectHandleReq struct {
	Handle blobcache.Handle `json:"handle"`
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
	Info blobcache.HandleInfo `json:"info"`
}

type DropReq struct {
	Handle blobcache.Handle `json:"handle"`
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
	Handles []blobcache.Handle `json:"handles"`
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
	Target blobcache.OID       `json:"target"`
	Mask   blobcache.ActionSet `json:"mask"`
}

type OpenAsResp struct {
	Handle blobcache.Handle     `json:"handle"`
	Info   blobcache.VolumeInfo `json:"info"`
}

type OpenFromReq struct {
	Base   blobcache.Handle    `json:"base"`
	Target blobcache.OID       `json:"target"`
	Mask   blobcache.ActionSet `json:"mask"`
}

type OpenFromResp struct {
	Handle blobcache.Handle     `json:"handle"`
	Info   blobcache.VolumeInfo `json:"info"`
}

type InspectVolumeReq struct {
	Volume blobcache.Handle `json:"volume"`
}

type InspectVolumeResp struct {
	Info blobcache.VolumeInfo `json:"info"`
}

type AwaitReq struct {
	Cond blobcache.Conditions `json:"cond"`
}

type AwaitResp struct{}

type BeginTxReq struct {
	Volume blobcache.Handle   `json:"volume"`
	Params blobcache.TxParams `json:"params"`
}

type BeginTxResp struct {
	// Tx is the handle for the transaction.
	Tx blobcache.Handle `json:"tx"`
	// VolumeInfo is the volume info for the transaction.
	VolumeInfo blobcache.VolumeInfo `json:"volume_info"`
}

type InspectTxReq struct {
	Tx blobcache.Handle `json:"tx"`
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
	Info blobcache.TxInfo `json:"info"`
}

func (r InspectTxResp) Marshal(out []byte) []byte {
	data, err := json.Marshal(r.Info)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (r *InspectTxResp) Unmarshal(data []byte) error {
	return json.Unmarshal(data, r)
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
	Tx  blobcache.Handle `json:"tx"`
	CID blobcache.CID    `json:"cid"`
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
	Tx   blobcache.Handle `json:"tx"`
	CID  blobcache.CID    `json:"cid"`
	Salt *blobcache.CID   `json:"salt,omitempty"`
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
	Data []byte `json:"data"`
}

func (gr GetResp) Marshal(out []byte) []byte {
	return append(out, gr.Data...)
}

func (gr *GetResp) Unmarshal(data []byte) error {
	gr.Data = append(gr.Data[:0], data...)
	return nil
}

type AllowLinkReq struct {
	Tx     blobcache.Handle `json:"tx"`
	Subvol blobcache.Handle `json:"subvol"`
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
	Spec blobcache.VolumeSpec `json:"spec"`
}

type CreateVolumeResp struct {
	Handle blobcache.Handle     `json:"handle"`
	Info   blobcache.VolumeInfo `json:"info"`
}
