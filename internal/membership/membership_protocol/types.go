package membership_protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

const IdSegmentLen = 8

type PeerVersion uint64

type Coordinates []uint64

func EuclideanDist(coords1, coords2 Coordinates) (float64, error) {
	if len(coords1) != len(coords2) {
		fmt.Println("coords1", fmt.Sprintf("%+v", coords1))
		fmt.Println("coords2", fmt.Sprintf("%+v", coords2))
		return -1, errors.New("different size coordinates")
	}
	var dist float64 = 0
	for i := 0; i < len(coords1); i++ {
		dist += math.Pow(float64(coords2[i]-coords1[i]), 2)
	}
	return math.Sqrt(dist), nil
}

func DeserializeCoordsFromBinary(bytes []byte) (int, Coordinates) {
	bufPos := 0
	nrSegments := binary.BigEndian.Uint16(bytes)
	bufPos += 2

	coords := make(Coordinates, nrSegments)
	for i := uint16(0); i < nrSegments; i++ {
		coords[i] = binary.BigEndian.Uint64(bytes[bufPos:])
		bufPos += 8
	}
	return bufPos, coords
}

func (coords Coordinates) SerializeToBinary() []byte {
	toReturn := make([]byte, 2)
	binary.BigEndian.PutUint16(toReturn, uint16(len(coords)))
	for _, coord := range coords {
		coordBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(coordBytes, coord)
		toReturn = append(toReturn, coordBytes...)
	}
	return toReturn
}

type PeerID [IdSegmentLen]byte

func (c PeerID) String() string {
	toReturn := ""
	for _, segment := range c {
		toReturn += string(segment)
	}
	return toReturn
}

type PeerIDChain []PeerID

func (c PeerIDChain) Level() uint16 {
	return uint16(len(c)) - 1
}

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

func (c PeerIDChain) ID() *PeerID {
	if len(c) < 1 {
		return nil
	}
	return &c[len(c)-1]
}

func (c PeerIDChain) String() string {
	str := ""
	for _, c := range c {
		str += "/" + c.String()
	}
	return str
}

type PeerWithIdChain struct {
	Coordinates
	peer.Peer
	chain     PeerIDChain
	nChildren uint16
	version   PeerVersion
}

func NewPeerWithIdChain(peerIdChain PeerIDChain, self peer.Peer, nChildren uint16, version PeerVersion, coords Coordinates) *PeerWithIdChain {
	return &PeerWithIdChain{
		Peer:        self,
		version:     version,
		nChildren:   nChildren,
		chain:       peerIdChain,
		Coordinates: coords,
	}
}

func (p *PeerWithIdChain) Chain() PeerIDChain {
	return p.chain
}

func (p *PeerWithIdChain) StringWithFields() string {
	return fmt.Sprintf("%s:%+v:%+v:v_%d:c(%d)", p.String(), p.Coordinates, p.chain, p.version, p.nChildren)
}

func (p *PeerWithIdChain) NrChildren() uint16 {
	return p.nChildren
}

func (p *PeerWithIdChain) Version() PeerVersion {
	return p.version
}

func (p *PeerWithIdChain) IsDescendentOf(otherPeerChain PeerIDChain) bool {
	return p.chain.IsDescendentOf(otherPeerChain)
}

func (p *PeerWithIdChain) IsParentOf(otherPeer *PeerWithIdChain) bool {
	return p.chain.IsParentOf(otherPeer.chain)
}

func (p *PeerWithIdChain) IsHigherVersionThan(otherPeer *PeerWithIdChain) bool {
	return p.Version() > otherPeer.Version()
}

func (p *PeerWithIdChain) MarshalWithFields() []byte {
	nrChildrenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(nrChildrenBytes, p.NrChildren())
	nrChildrenBytes = append(nrChildrenBytes, p.Marshal()...)

	chainBytes := SerializePeerIDChain(p.chain)
	nrChildrenBytes = append(nrChildrenBytes, chainBytes...)

	coordBytes := p.Coordinates.SerializeToBinary()
	nrChildrenBytes = append(nrChildrenBytes, coordBytes...)

	versionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(versionBytes, uint64(p.Version()))
	nrChildrenBytes = append(nrChildrenBytes, versionBytes...)
	return nrChildrenBytes
}

func UnmarshalPeerWithIdChain(bytes []byte) (int, *PeerWithIdChain) {
	bufPos := 0
	nrChildren := binary.BigEndian.Uint16(bytes[bufPos:])
	bufPos += 2
	p := &peer.IPeer{}
	n := p.Unmarshal(bytes[bufPos:])
	bufPos += n

	n, peerChain := DeserializePeerIDChain(bytes[bufPos:])
	bufPos += n

	n, peerCoords := DeserializeCoordsFromBinary(bytes[bufPos:])
	bufPos += n

	version := binary.BigEndian.Uint64(bytes[bufPos:])
	bufPos += 8
	return bufPos, NewPeerWithIdChain(peerChain, p, nrChildren, PeerVersion(version), peerCoords)
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
	for idx, p := range peers {
		if p == nil {
			panic(fmt.Sprintf("peer at index %d is nil", idx))
		}
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

func (p MeasuredPeersByLat) String() string {
	toPrint := ""
	for _, measuredPeer := range p {
		toPrint = toPrint + "; " + fmt.Sprintf("%s : %s", measuredPeer.String(), measuredPeer.MeasuredLatency)
	}
	return toPrint
}

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

func PeerContainedIn(v peer.Peer, arr ...peer.Peer) bool {
	for _, p := range arr {
		if peer.PeersEqual(v, p) {
			return true
		}
	}
	return false
}

func PeerWithIdChainContainedIn(v peer.Peer, arr ...peer.Peer) bool {
	for _, p := range arr {
		if peer.PeersEqual(v, p) {
			return true
		}
	}
	return false
}

func TrimMeasuredPeersToSize(arr MeasuredPeersByLat, size int) MeasuredPeersByLat {
	if len(arr) > size {
		for i := size; i < len(arr); i++ {
			arr[i] = nil
		}
		arr = arr[:size]
	}
	return arr
}
