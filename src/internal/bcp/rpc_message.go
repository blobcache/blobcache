package bcp

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobstream"
	"go.brendoncarroll.net/exp/sbe"
)

type EndpointReq struct{}

func (req EndpointReq) Marshal(out []byte) []byte {
	return out
}

func (req *EndpointReq) Unmarshal(out []byte) error {
	return nil
}

type EndpointResp struct {
	Endpoint blobcache.Endpoint
}

func (ep EndpointResp) Marshal(out []byte) []byte {
	return ep.Endpoint.Marshal(out)
}

func (ep *EndpointResp) Unmarshal(out []byte) error {
	return ep.Endpoint.Unmarshal(out)
}

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

type ShareOutReq struct {
	Handle blobcache.Handle
	Peer   blobcache.NodeID
	Mask   blobcache.ActionSet
}

func (sr ShareOutReq) Marshal(out []byte) []byte {
	out = sr.Handle.Marshal(out)
	out = append(out, sr.Peer[:]...)
	out = binary.BigEndian.AppendUint64(out, uint64(sr.Mask))
	return out
}

func (sr *ShareOutReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize+blobcache.NodeIDSize+8 {
		return fmt.Errorf("cannot unmarshal ShareReq, too short: %d", len(data))
	}
	if err := sr.Handle.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	sr.Peer = blobcache.NodeID(data[blobcache.HandleSize : blobcache.HandleSize+blobcache.NodeIDSize])
	sr.Mask = blobcache.ActionSet(binary.BigEndian.Uint64(data[blobcache.HandleSize+blobcache.NodeIDSize:]))
	return nil
}

type ShareOutResp struct {
	Handle blobcache.Handle
}

func (sr ShareOutResp) Marshal(out []byte) []byte {
	out = sr.Handle.Marshal(out)
	return out
}

func (sr *ShareOutResp) Unmarshal(data []byte) error {
	return sr.Handle.Unmarshal(data)
}

type ShareInReq struct {
	Host   blobcache.NodeID
	Handle blobcache.Handle
}

func (ar ShareInReq) Marshal(out []byte) []byte {
	out = append(out, ar.Host[:]...)
	out = ar.Handle.Marshal(out)
	return out
}

func (ar *ShareInReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.NodeIDSize+blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal ShareInReq, too short: %d", len(data))
	}
	ar.Host = blobcache.NodeID(data[:blobcache.NodeIDSize])
	return ar.Handle.Unmarshal(data[blobcache.NodeIDSize:])
}

type ShareInResp struct {
	Handle blobcache.Handle
}

func (ar ShareInResp) Marshal(out []byte) []byte {
	return ar.Handle.Marshal(out)
}

func (ar *ShareInResp) Unmarshal(data []byte) error {
	return ar.Handle.Unmarshal(data)
}

type InspectReq struct {
	Handle blobcache.Handle
}

func (r InspectReq) Marshal(out []byte) []byte {
	return r.Handle.Marshal(out)
}

func (r *InspectReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal InspectReq, too short: %d", len(data))
	}
	return r.Handle.Unmarshal(data)
}

type InspectResp struct {
	Info blobcache.Info
}

func (r InspectResp) Marshal(out []byte) []byte {
	data, err := json.Marshal(r.Info)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (r *InspectResp) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &r.Info)
}

type OpenFiatReq struct {
	Target blobcache.OID
	Mask   blobcache.ActionSet
}

func (oa OpenFiatReq) Marshal(out []byte) []byte {
	out = oa.Target.Marshal(out)
	out = binary.BigEndian.AppendUint64(out, uint64(oa.Mask))
	return out
}

func (oa *OpenFiatReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.OIDSize+8 {
		return fmt.Errorf("cannot unmarshal OpenFiatReq, too short: %d", len(data))
	}
	oa.Target = blobcache.OID(data[:blobcache.OIDSize])
	oa.Mask = blobcache.ActionSet(binary.BigEndian.Uint64(data[blobcache.OIDSize:]))
	return nil
}

type OpenFiatResp struct {
	Handle blobcache.Handle
	Info   blobcache.VolumeInfo
}

func (oa OpenFiatResp) Marshal(out []byte) []byte {
	out = oa.Handle.Marshal(out)
	return oa.Info.Marshal(out)
}

func (oa *OpenFiatResp) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal OpenFiatResp, too short: %d", len(data))
	}
	if err := oa.Handle.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	if err := oa.Info.Unmarshal(data); err != nil {
		return fmt.Errorf("cannot unmarshal OpenFiatResp.Info: %w", err)
	}
	return nil
}

type OpenFromReq struct {
	Base  blobcache.Handle
	Token blobcache.LinkToken
	Mask  blobcache.ActionSet
}

func (of OpenFromReq) Marshal(out []byte) []byte {
	out = of.Base.Marshal(out)
	out = of.Token.Marshal(out)
	out = binary.BigEndian.AppendUint64(out, uint64(of.Mask))
	return out
}

func (of *OpenFromReq) Unmarshal(data []byte) error {
	var maskBuf [8]byte
	var token [blobcache.LinkTokenSize]byte
	if err := unmarshalSections(data, [][]byte{
		of.Base.OID[:], of.Base.Secret[:],
		token[:],
		maskBuf[:],
	}); err != nil {
		return err
	}
	of.Mask = blobcache.ActionSet(binary.BigEndian.Uint64(maskBuf[:]))
	if err := of.Token.Unmarshal(token[:]); err != nil {
		return err
	}
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

type CloneVolumeReq struct {
	Volume blobcache.Handle
}

func (cr CloneVolumeReq) Marshal(out []byte) []byte {
	return cr.Volume.Marshal(out)
}

func (cr *CloneVolumeReq) Unmarshal(data []byte) error {
	return cr.Volume.Unmarshal(data)
}

type CloneVolumeResp struct {
	Handle blobcache.Handle
}

func (cr CloneVolumeResp) Marshal(out []byte) []byte {
	return cr.Handle.Marshal(out)
}

func (cr *CloneVolumeResp) Unmarshal(data []byte) error {
	return cr.Handle.Unmarshal(data)
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
	// TxInfo is the info for the transaction.
	Info blobcache.TxInfo
	// PreemptData contains preemptively sent data in the blobstream protocol.
	PreemptData [][]byte
}

func (btx BeginTxResp) Marshal(out []byte) []byte {
	out = btx.Tx.Marshal(out)
	out = sbe.AppendLP16(out, btx.Info.Marshal(nil))
	out = blobstream.AppendBytes(out, slices.Values(btx.PreemptData))
	return out
}

func (btx *BeginTxResp) Unmarshal(data []byte) error {
	h, data, err := readHandle(data)
	if err != nil {
		return err
	}
	btx.Tx = *h
	infoData, data, err := sbe.ReadLP16(data)
	if err != nil {
		return err
	}
	if err := btx.Info.Unmarshal(infoData); err != nil {
		return err
	}
	if btx.PreemptData, err = blobstream.ReadBytes(data); err != nil {
		return err
	}
	return nil
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
}

func (cr CommitReq) Marshal(out []byte) []byte {
	out = cr.Tx.Marshal(out)
	return out
}

func (cr *CommitReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal CommitReq, too short: %d", len(data))
	}
	if err := cr.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
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
	Tx   blobcache.Handle
	CIDs []blobcache.CID
}

func (dr DeleteReq) Marshal(out []byte) []byte {
	out = dr.Tx.Marshal(out)
	out = binary.AppendUvarint(out, uint64(len(dr.CIDs)))
	for _, cid := range dr.CIDs {
		out = append(out, cid[:]...)
	}
	return out
}

func (dr *DeleteReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize+blobcache.CIDSize {
		return fmt.Errorf("cannot unmarshal DeleteReq, too short: %d", len(data))
	}
	if err := dr.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	numCIDs, data, err := sbe.ReadUVarint(data[blobcache.HandleSize:])
	if err != nil {
		return err
	}
	dr.CIDs = make([]blobcache.CID, numCIDs)
	for i := range dr.CIDs {
		if len(data) < blobcache.CIDSize {
			return fmt.Errorf("cannot unmarshal DeleteReq, too short: %d", len(data))
		}
		dr.CIDs[i] = blobcache.CID(data[:blobcache.CIDSize])
		data = data[blobcache.CIDSize:]
	}
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
	Tx  blobcache.Handle
	CID blobcache.CID
}

func (gr GetReq) Marshal(out []byte) []byte {
	out = gr.Tx.Marshal(out)
	out = append(out, gr.CID[:]...)
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
	return nil
}

type GetSaltReq struct {
	Tx   blobcache.Handle
	CID  blobcache.CID
	Salt blobcache.CID
}

func (gsr GetSaltReq) Marshal(out []byte) []byte {
	out = gsr.Tx.Marshal(out)
	out = append(out, gsr.CID[:]...)
	out = append(out, gsr.Salt[:]...)
	return out
}

func (gsr *GetSaltReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize+blobcache.CIDSize+blobcache.CIDSize {
		return fmt.Errorf("cannot unmarshal GetSaltReq, too short: %d", len(data))
	}
	if err := gsr.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	gsr.CID = blobcache.CID(data[:blobcache.CIDSize])
	gsr.Salt = blobcache.CID(data[blobcache.CIDSize : blobcache.CIDSize*2])
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

type LinkReq struct {
	Tx     blobcache.Handle
	Subvol blobcache.Handle
	Mask   blobcache.ActionSet
}

func (ar LinkReq) Marshal(out []byte) []byte {
	out = ar.Tx.Marshal(out)
	out = ar.Subvol.Marshal(out)
	out = binary.BigEndian.AppendUint64(out, uint64(ar.Mask))
	return out
}

func (ar *LinkReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize*2+8 {
		return fmt.Errorf("cannot unmarshal LinkReq, too short: %d", len(data))
	}
	if err := ar.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	if err := ar.Subvol.Unmarshal(data[blobcache.HandleSize : blobcache.HandleSize*2]); err != nil {
		return err
	}
	ar.Mask = blobcache.ActionSet(binary.BigEndian.Uint64(data[blobcache.HandleSize*2:]))
	return nil
}

type LinkResp struct {
	Token blobcache.LinkToken
}

func (ar LinkResp) Marshal(out []byte) []byte {
	out = ar.Token.Marshal(out)
	return out
}

func (ar *LinkResp) Unmarshal(data []byte) error {
	if err := ar.Token.Unmarshal(data); err != nil {
		return err
	}
	return nil
}

type UnlinkReq struct {
	Tx      blobcache.Handle
	Targets []blobcache.LinkToken
}

func (ur UnlinkReq) Marshal(out []byte) []byte {
	out = ur.Tx.Marshal(out)
	out = binary.AppendUvarint(out, uint64(len(ur.Targets)))
	for _, target := range ur.Targets {
		out = target.Marshal(out)
	}
	return out
}

func (ur *UnlinkReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal UnlinkReq, too short: %d", len(data))
	}
	if err := ur.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	numTargets, data, err := sbe.ReadUVarint(data[blobcache.HandleSize:])
	if err != nil {
		return err
	}
	ur.Targets = make([]blobcache.LinkToken, numTargets)
	for i := range ur.Targets {
		if len(data) < blobcache.LinkTokenSize {
			return fmt.Errorf("cannot unmarshal UnlinkReq, too short: %d", len(data))
		}
		if err := ur.Targets[i].Unmarshal(data[:blobcache.OIDSize]); err != nil {
			return err
		}
		data = data[blobcache.OIDSize:]
	}
	return nil
}

type UnlinkResp struct{}

func (ur UnlinkResp) Marshal(out []byte) []byte {
	return out
}

func (ur *UnlinkResp) Unmarshal(data []byte) error {
	if len(data) != 0 {
		return fmt.Errorf("empty data expected for UnlinkResp")
	}
	return nil
}

type VisitLinksReq struct {
	Tx      blobcache.Handle
	Targets []blobcache.LinkToken
}

func (vr VisitLinksReq) Marshal(out []byte) []byte {
	out = vr.Tx.Marshal(out)
	out = binary.AppendUvarint(out, uint64(len(vr.Targets)))
	for _, target := range vr.Targets {
		out = target.Marshal(out)
	}
	return out
}

func (vr *VisitLinksReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal VisitLinksReq, too short: %d", len(data))
	}
	if err := vr.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	numTargets, data, err := sbe.ReadUVarint(data[blobcache.HandleSize:])
	if err != nil {
		return err
	}
	vr.Targets = make([]blobcache.LinkToken, numTargets)
	for i := range vr.Targets {
		if len(data) < blobcache.OIDSize {
			return fmt.Errorf("cannot unmarshal VisitLinksReq, too short: %d", len(data))
		}
		if err := vr.Targets[i].Unmarshal(data[:blobcache.OIDSize]); err != nil {
			return err
		}
		data = data[blobcache.OIDSize:]
	}
	return nil
}

type VisitLinksResp struct{}

func (vr VisitLinksResp) Marshal(out []byte) []byte {
	return out
}

func (vr *VisitLinksResp) Unmarshal(data []byte) error {
	return nil
}

type AddFromReq struct {
	Tx   blobcache.Handle
	CIDs []blobcache.CID
	Srcs []blobcache.Handle
}

func (ar AddFromReq) Marshal(out []byte) []byte {
	out = ar.Tx.Marshal(out)
	out = binary.AppendUvarint(out, uint64(len(ar.CIDs)))
	for _, cid := range ar.CIDs {
		out = append(out, cid[:]...)
	}
	out = binary.AppendUvarint(out, uint64(len(ar.Srcs)))
	for _, src := range ar.Srcs {
		out = src.Marshal(out)
	}
	return out
}

func (ar *AddFromReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal AddFromReq, too short: %d", len(data))
	}
	if err := ar.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	numCIDs, data, err := sbe.ReadUVarint(data[blobcache.HandleSize:])
	if err != nil {
		return err
	}
	ar.CIDs = make([]blobcache.CID, numCIDs)
	for i := range ar.CIDs {
		if len(data) < blobcache.CIDSize {
			return fmt.Errorf("cannot unmarshal AddFromReq, too short: %d", len(data))
		}
		ar.CIDs[i] = blobcache.CID(data[:blobcache.CIDSize])
		data = data[blobcache.CIDSize:]
	}
	numSrcs, data, err := sbe.ReadUVarint(data)
	if err != nil {
		return err
	}
	ar.Srcs = make([]blobcache.Handle, numSrcs)
	for i := range ar.Srcs {
		if len(data) < blobcache.HandleSize {
			return fmt.Errorf("cannot unmarshal AddFromReq, too short: %d", len(data))
		}
		if err := ar.Srcs[i].Unmarshal(data); err != nil {
			return err
		}
		data = data[blobcache.HandleSize:]
	}
	return nil
}

type AddFromResp struct {
	Added []bool
}

func (ar AddFromResp) Marshal(out []byte) []byte {
	for i := range ar.Added {
		if i%8 == 0 {
			out = append(out, 0)
		}
		if ar.Added[i] {
			out[len(out)-1] |= 1 << (i % 8)
		}
	}
	return out
}

func (ar *AddFromResp) Unmarshal(data []byte) error {
	ar.Added = make([]bool, len(data)*8)
	for i := range data {
		for j := 0; j < 8; j++ {
			if (data[i] & (1 << j)) != 0 {
				ar.Added[i*8+j] = true
			} else {
				ar.Added[i*8+j] = false
			}
		}
	}
	return nil
}

type VisitReq struct {
	Tx   blobcache.Handle
	CIDs []blobcache.CID
}

func (vr VisitReq) Marshal(out []byte) []byte {
	out = vr.Tx.Marshal(out)
	out = binary.AppendUvarint(out, uint64(len(vr.CIDs)))
	for _, cid := range vr.CIDs {
		out = append(out, cid[:]...)
	}
	return out
}

func (vr *VisitReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal VisitReq, too short: %d", len(data))
	}
	if err := vr.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	numCIDs, data, err := sbe.ReadUVarint(data[blobcache.HandleSize:])
	if err != nil {
		return err
	}
	vr.CIDs = make([]blobcache.CID, numCIDs)
	for i := range vr.CIDs {
		if len(data) < blobcache.CIDSize {
			return fmt.Errorf("cannot unmarshal VisitReq, too short: %d", len(data))
		}
		vr.CIDs[i] = blobcache.CID(data[:blobcache.CIDSize])
		data = data[blobcache.CIDSize:]
	}
	return nil
}

type VisitResp struct{}

func (vr VisitResp) Marshal(out []byte) []byte {
	return out
}

func (vr *VisitResp) Unmarshal(data []byte) error {
	if len(data) != 0 {
		return fmt.Errorf("empty data expected for VisitResp")
	}
	return nil
}

type IsVisitedReq struct {
	Tx   blobcache.Handle
	CIDs []blobcache.CID
}

func (ir IsVisitedReq) Marshal(out []byte) []byte {
	out = ir.Tx.Marshal(out)
	out = binary.AppendUvarint(out, uint64(len(ir.CIDs)))
	for _, cid := range ir.CIDs {
		out = append(out, cid[:]...)
	}
	return out
}

func (ir *IsVisitedReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal IsVisitedReq, too short: %d", len(data))
	}
	if err := ir.Tx.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	// read number of CIDs, then parse fixed-size CID entries
	numCIDs, rest, err := sbe.ReadUVarint(data[blobcache.HandleSize:])
	if err != nil {
		return err
	}
	ir.CIDs = make([]blobcache.CID, numCIDs)
	for i := range ir.CIDs {
		if len(rest) < blobcache.CIDSize {
			return fmt.Errorf("cannot unmarshal IsVisitedReq, too short: %d", len(rest))
		}
		ir.CIDs[i] = blobcache.CID(rest[:blobcache.CIDSize])
		rest = rest[blobcache.CIDSize:]
	}
	return nil
}

type IsVisitedResp struct {
	Visited []bool
}

func (ir IsVisitedResp) Marshal(out []byte) []byte {
	for i := range ir.Visited {
		if i%8 == 0 {
			out = append(out, 0)
		}
		if ir.Visited[i] {
			out[len(out)-1] |= 1 << (i % 8)
		}
	}
	return out
}

func (ir *IsVisitedResp) Unmarshal(data []byte) error {
	ir.Visited = make([]bool, len(data)*8)
	for i := range data {
		for j := 0; j < 8; j++ {
			if (data[i] & (1 << j)) != 0 {
				ir.Visited[i*8+j] = true
			}
		}
	}
	return nil
}

type CreateVolumeReq struct {
	Host blobcache.Endpoint
	Spec blobcache.VolumeSpec
}

func (cr CreateVolumeReq) Marshal(out []byte) []byte {
	out = sbe.AppendLP16(out, cr.Host.Marshal(nil))
	out = sbe.AppendLP16(out, cr.Spec.Marshal(nil))
	return out
}

func (cr *CreateVolumeReq) Unmarshal(data []byte) error {
	hostData, data, err := sbe.ReadLP16(data)
	if err != nil {
		return err
	}
	specData, data, err := sbe.ReadLP16(data)
	if err != nil {
		return err
	}
	if err := cr.Host.Unmarshal(hostData); err != nil {
		return err
	}
	if err := cr.Spec.Unmarshal(specData); err != nil {
		return err
	}
	return nil
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

type CreateQueueReq struct {
	Spec blobcache.QueueSpec
}

func (cq CreateQueueReq) Marshal(out []byte) []byte {
	return cq.Spec.Marshal(out)
}

func (cq *CreateQueueReq) Unmarshal(data []byte) error {
	return cq.Spec.Unmarshal(data)
}

type CreateQueueResp struct {
	Handle blobcache.Handle
}

func (cq CreateQueueResp) Marshal(out []byte) []byte {
	return cq.Handle.Marshal(out)
}

func (cq *CreateQueueResp) Unmarshal(data []byte) error {
	return cq.Handle.Unmarshal(data)
}

type DequeueReq struct {
	Queue blobcache.Handle
	Opts  blobcache.DequeueOpts
	Max   int
}

func (dr DequeueReq) Marshal(out []byte) []byte {
	out = dr.Queue.Marshal(out)
	out = sbe.AppendUint32(out, uint32(dr.Max))
	return dr.Opts.Marshal(out)
}

func (dr *DequeueReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal DequeueReq, too short: %d", len(data))
	}
	if err := dr.Queue.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	max, rest, err := sbe.ReadUint32(data)
	if err != nil {
		return err
	}
	dr.Max = int(max)
	return dr.Opts.Unmarshal(rest)
}

type DequeueResp struct {
	Messages []blobcache.Message
}

func (nr DequeueResp) Marshal(out []byte) []byte {
	out = sbe.AppendUint32(out, uint32(len(nr.Messages)))
	for _, msg := range nr.Messages {
		out = sbe.AppendLP(out, msg.Marshal(nil))
	}
	return out
}

func (nr *DequeueResp) Unmarshal(data []byte) error {
	numMessages, data, err := sbe.ReadUint32(data)
	if err != nil {
		return err
	}
	nr.Messages = make([]blobcache.Message, numMessages)
	for i := range nr.Messages {
		msgData, rest, err := sbe.ReadLP(data)
		if err != nil {
			return err
		}
		if err := nr.Messages[i].Unmarshal(msgData); err != nil {
			return err
		}
		data = rest
	}
	return nil
}

type InspectQueueReq struct {
	Queue blobcache.Handle
}

func (iq InspectQueueReq) Marshal(out []byte) []byte {
	return iq.Queue.Marshal(out)
}

func (iq *InspectQueueReq) Unmarshal(data []byte) error {
	return iq.Queue.Unmarshal(data)
}

type InspectQueueResp struct {
	Info blobcache.QueueInfo
}

func (iq InspectQueueResp) Marshal(out []byte) []byte {
	return iq.Info.Marshal(out)
}

func (iq *InspectQueueResp) Unmarshal(data []byte) error {
	return iq.Info.Unmarshal(data)
}

type EnqueueReq struct {
	Queue    blobcache.Handle
	Messages []blobcache.Message
}

func (er EnqueueReq) Marshal(out []byte) []byte {
	out = er.Queue.Marshal(out)
	out = sbe.AppendUint32(out, uint32(len(er.Messages)))
	for _, msg := range er.Messages {
		out = sbe.AppendLP(out, msg.Marshal(nil))
	}
	return out
}

func (er *EnqueueReq) Unmarshal(data []byte) error {
	if len(data) < blobcache.HandleSize+4 {
		return fmt.Errorf("cannot unmarshal EnqueueReq, too short: %d", len(data))
	}
	if err := er.Queue.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]

	numMessages, data, err := sbe.ReadUint32(data)
	if err != nil {
		return err
	}
	er.Messages = make([]blobcache.Message, numMessages)
	for i := range er.Messages {
		msgData, rest, err := sbe.ReadLP(data)
		if err != nil {
			return err
		}
		if err := er.Messages[i].Unmarshal(msgData); err != nil {
			return err
		}
		data = rest
	}
	return nil
}

type EnqueueResp struct {
	Success uint32
}

func (ir EnqueueResp) Marshal(out []byte) []byte {
	return sbe.AppendUint32(out, ir.Success)
}

func (ir *EnqueueResp) Unmarshal(data []byte) error {
	success, _, err := sbe.ReadUint32(data)
	if err != nil {
		return err
	}
	ir.Success = success
	return nil
}

type SubToVolumeReq struct {
	Queue  blobcache.Handle
	Volume blobcache.Handle
	Spec   blobcache.VolSubSpec
}

func (sr SubToVolumeReq) Marshal(out []byte) []byte {
	out = sr.Queue.Marshal(out)
	out = sr.Volume.Marshal(out)
	return sbe.AppendLP(out, sr.Spec.Marshal(nil))
}

func (sr *SubToVolumeReq) Unmarshal(data []byte) error {
	if len(data) < 2*blobcache.HandleSize {
		return fmt.Errorf("cannot unmarshal SubToVolumeReq, too short: %d", len(data))
	}
	if err := sr.Queue.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	if err := sr.Volume.Unmarshal(data[:blobcache.HandleSize]); err != nil {
		return err
	}
	data = data[blobcache.HandleSize:]
	specData, _, err := sbe.ReadLP(data)
	if err != nil {
		return err
	}
	return sr.Spec.Unmarshal(specData)
}

type SubToVolumeResp struct{}

func (sr SubToVolumeResp) Marshal(out []byte) []byte {
	return out
}

func (sr *SubToVolumeResp) Unmarshal(data []byte) error {
	return nil
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
