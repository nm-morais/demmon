package membership

import (
	"bytes"
	"encoding/binary"
	"time"

	. "github.com/nm-morais/go-babel/pkg/peer"
)

const IdSegmentLen = 8

type PeerVersion uint64

type PeerID [IdSegmentLen]byte

type PeerIDChain []PeerID

func (c PeerIDChain) IsDescendentOf(otherPeerChain PeerIDChain) bool {
	// IsDescendant retuns true if chain <c> is contained in chain <otherPeerChain>

	if len(c) == 0 || len(otherPeerChain) == 0 {
		return false
	}

	if len(c) < len(otherPeerChain) {
		return false
	}

	for i := 0; i < len(otherPeerChain); i++ {
		if !bytes.Equal(c[i][:], otherPeerChain[i][:]) {
			return false
		}
	}

	return true
}

func (c PeerIDChain) IsParentOf(otherPeerChain PeerIDChain) bool {
	if len(c) == 0 || len(otherPeerChain) == 0 || (len(c) != len(otherPeerChain)-1) {
		return false
	}

	for i := 0; i < len(c); i++ {
		if !bytes.Equal(c[i][:], otherPeerChain[i][:]) {
			return false
		}
	}

	return true
}

func (c PeerIDChain) getParentChain() PeerIDChain {
	if len(c) < 1 {
		return nil
	}

	return c[len(c)-1:]
}

type PeerWithIdChain struct {
	Peer
	PeerIDChain
	nChildren uint16
	version   PeerVersion
}

func NewPeerWithIdChain(peerIdChain PeerIDChain, self Peer, nChildren uint16, version PeerVersion) *PeerWithIdChain {
	return &PeerWithIdChain{
		Peer:        self,
		version:     version,
		nChildren:   nChildren,
		PeerIDChain: peerIdChain,
	}
}

func (p *PeerWithIdChain) SetChildrenNr(nChildren uint16) {
	p.nChildren = nChildren
}

func (p *PeerWithIdChain) Chain() PeerIDChain {
	return p.PeerIDChain
}

func (p *PeerWithIdChain) SetChain(newChain PeerIDChain) {
	p.PeerIDChain = newChain
}

func (p *PeerWithIdChain) NrChildren() uint16 {
	return p.nChildren
}

func (p *PeerWithIdChain) Version() PeerVersion {
	return p.version
}

func (p *PeerWithIdChain) SetVersion(v PeerVersion) {
	p.version = v
}

func (p *PeerWithIdChain) IsDescendentOf(otherPeerChain PeerIDChain) bool {
	return p.PeerIDChain.IsDescendentOf(otherPeerChain)
}

func (p *PeerWithIdChain) IsParentOf(otherPeer *PeerWithIdChain) bool {
	return p.PeerIDChain.IsParentOf(otherPeer.PeerIDChain)
}

func (p *PeerWithIdChain) MarshalWithFields() []byte {
	nrChildrenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(nrChildrenBytes, p.NrChildren())
	nrChildrenBytes = append(nrChildrenBytes, p.Marshal()...)

	chainBytes := SerializePeerIDChain(p.PeerIDChain)
	nrChildrenBytes = append(nrChildrenBytes, chainBytes...)

	versionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(versionBytes, uint64(p.Version()))
	nrChildrenBytes = append(nrChildrenBytes, versionBytes...)
	return nrChildrenBytes
}

func UnmarshalPeerWithIdChain(bytes []byte) (int, *PeerWithIdChain) {
	nrChildren := binary.BigEndian.Uint16(bytes[0:2])
	p := &IPeer{}
	nrPeerBytes := p.Unmarshal(bytes[2:])
	nrChainBytes, peerChain := DeserializePeerIDChain(bytes[nrPeerBytes+2:])
	version := binary.BigEndian.Uint64(bytes[nrPeerBytes+2+nrChainBytes:])
	return nrPeerBytes + nrChainBytes + 2 + 8, NewPeerWithIdChain(peerChain, p, nrChildren, PeerVersion(version))
}

func DeserializePeerWithIDChainArray(buf []byte) (int, []*PeerWithIdChain) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]*PeerWithIdChain, nrPeers)
	bufPos := 4
	for i := 0; i < nrPeers; i++ {
		read, peer := UnmarshalPeerWithIdChain(buf[bufPos:])
		peers[i] = peer
		bufPos += read
	}
	return bufPos, peers
}

func SerializePeerWithIDChainArray(peers []*PeerWithIdChain) []byte {
	totalBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalBytes, uint32(len(peers)))
	for _, p := range peers {
		totalBytes = append(totalBytes, p.MarshalWithFields()...)
	}
	return totalBytes
}

func SerializePeerIDChain(id PeerIDChain) []byte {
	nrSegmentBytes := make([]byte, 2)
	nrSegments := uint16(len(id))
	binary.BigEndian.PutUint16(nrSegmentBytes, nrSegments)
	toReturn := make([]byte, nrSegments*IdSegmentLen)
	var currSegment uint16
	for currSegment = 0; currSegment < nrSegments; currSegment++ {
		copy(toReturn[int(currSegment)*IdSegmentLen:], id[currSegment][:])
	}
	return append(nrSegmentBytes, toReturn...)
}

func DeserializePeerIDChain(idBytes []byte) (int, PeerIDChain) {
	nrSegments := int(binary.BigEndian.Uint16(idBytes[0:2]))
	bufPos := 2
	toReturn := make(PeerIDChain, nrSegments)
	for i := 0; i < nrSegments; i++ {
		currSegment := PeerID{}
		n := copy(currSegment[:], idBytes[bufPos:])
		bufPos += n
		toReturn[i] = currSegment
	}
	return bufPos, toReturn
}

func (c PeerIDChain) Equal(chain2 PeerIDChain) bool {
	if len(c) != len(chain2) {
		return false
	}

	for i := 0; i < len(c); i++ {
		if !bytes.Equal(c[i][:], chain2[i][:]) {
			return false
		}
	}

	return true
}

type MeasuredPeer struct {
	*PeerWithIdChain
	MeasuredLatency time.Duration
}

type MeasuredPeersByLat []*MeasuredPeer

func (p MeasuredPeersByLat) Len() int {
	return len(p)
}
func (p MeasuredPeersByLat) Less(i, j int) bool {
	return p[i].MeasuredLatency < p[j].MeasuredLatency
}
func (p MeasuredPeersByLat) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func NewMeasuredPeer(peer *PeerWithIdChain, measuredLatency time.Duration) *MeasuredPeer {
	return &MeasuredPeer{
		PeerWithIdChain: peer,
		MeasuredLatency: measuredLatency,
	}
}

func (p *MeasuredPeer) MarshalWithFieldsAndLatency() []byte {
	latencyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(latencyBytes, uint64(p.MeasuredLatency))
	return append(latencyBytes, p.MarshalWithFields()...)
}

func (p *MeasuredPeer) UnmarshalMeasuredPeer(buf []byte) (int, *MeasuredPeer) {
	latency := time.Duration(binary.BigEndian.Uint64(buf))
	n, aux := UnmarshalPeerWithIdChain(buf[8:])
	return n + 8, &MeasuredPeer{
		PeerWithIdChain: aux,
		MeasuredLatency: latency,
	}
}

func DeserializeMeasuredPeerArray(buf []byte) (int, []*MeasuredPeer) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]*MeasuredPeer, nrPeers)
	bufPos := 4
	for i := 0; i < nrPeers; i++ {
		p := &MeasuredPeer{}
		read, peer := p.UnmarshalMeasuredPeer(buf[bufPos:])
		peers[i] = peer
		bufPos += read
	}
	return bufPos, peers
}

func SerializeMeasuredPeerArray(peers []*MeasuredPeer) []byte {
	totalBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalBytes, uint32(len(peers)))
	for _, p := range peers {
		totalBytes = append(totalBytes, p.MarshalWithFieldsAndLatency()...)
	}
	return totalBytes
}
