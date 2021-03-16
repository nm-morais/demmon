package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

const IDSegmentLen = 8

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

func DeserializeCoordsFromBinary(byteArr []byte) (int, Coordinates) {
	bufPos := 0
	nrSegments := binary.BigEndian.Uint16(byteArr)
	bufPos += 2

	coords := make(Coordinates, nrSegments)
	for i := uint16(0); i < nrSegments; i++ {
		coords[i] = binary.BigEndian.Uint64(byteArr[bufPos:])
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

type PeerID [IDSegmentLen]byte

func (c PeerID) String() string {
	return hex.EncodeToString(c[:])
}

type PeerIDChain []PeerID

func (c PeerIDChain) Level() int {
	return len(c) - 1
}

// IsDescendant returns true if chain <c> is contained in chain <otherPeerChain>.
func (c PeerIDChain) IsDescendentOf(otherPeerChain PeerIDChain) bool {

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

type PeerWithIDChain struct {
	Coordinates
	chain PeerIDChain
	peer.Peer
	version       PeerVersion
	nChildren     uint16
	outConnActive bool
	inConnActive  bool
}

func NewPeerWithIDChain(
	peerIDChain PeerIDChain,
	self peer.Peer,
	nChildren uint16,
	version PeerVersion,
	coords Coordinates,
) *PeerWithIDChain {
	return &PeerWithIDChain{
		outConnActive: false,
		inConnActive:  true,
		Peer:          self,
		version:       version,
		nChildren:     nChildren,
		chain:         peerIDChain,
		Coordinates:   coords,
	}
}

func (p *PeerWithIDChain) Chain() PeerIDChain {
	return p.chain
}

func (p *PeerWithIDChain) StringWithFields() string {
	coordinatesStr := "("
	for _, c := range p.Coordinates {
		coordinatesStr += fmt.Sprintf("%d,", c)
	}
	coordinatesStr = coordinatesStr[:len(coordinatesStr)-1] + ")"
	return fmt.Sprintf("%s:%s:%s:v_%d:c(%d)", p.String(), coordinatesStr, p.chain.String(), p.version, p.nChildren)
}

func (p *PeerWithIDChain) NrChildren() uint16 {
	return p.nChildren
}

func (p *PeerWithIDChain) Version() PeerVersion {
	return p.version
}

func (p *PeerWithIDChain) IsDescendentOf(otherPeerChain PeerIDChain) bool {
	return p.chain.IsDescendentOf(otherPeerChain)
}

func (p *PeerWithIDChain) IsParentOf(otherPeer *PeerWithIDChain) bool {
	return p.chain.IsParentOf(otherPeer.chain)
}

func (p *PeerWithIDChain) IsHigherVersionThan(otherPeer *PeerWithIDChain) bool {
	return p.Version() > otherPeer.Version()
}

func (p *PeerWithIDChain) MarshalWithFields() []byte {
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

func UnmarshalPeerWithIDChain(byteArr []byte) (int, *PeerWithIDChain) {
	bufPos := 0
	nrChildren := binary.BigEndian.Uint16(byteArr[bufPos:])
	bufPos += 2
	p := &peer.IPeer{}
	n := p.Unmarshal(byteArr[bufPos:])
	bufPos += n

	n, peerChain := DeserializePeerIDChain(byteArr[bufPos:])
	bufPos += n

	n, peerCoords := DeserializeCoordsFromBinary(byteArr[bufPos:])
	bufPos += n

	version := binary.BigEndian.Uint64(byteArr[bufPos:])
	bufPos += 8
	return bufPos, NewPeerWithIDChain(peerChain, p, nrChildren, PeerVersion(version), peerCoords)
}

func DeserializePeerWithIDChainArray(buf []byte) (int, []*PeerWithIDChain) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]*PeerWithIDChain, nrPeers)
	bufPos := 4

	if nrPeers > 0 && len(buf)-4 < 0 {
		panic(fmt.Sprintf("have %d more peers to deserialize but buf size is too little (%d)", nrPeers, len(buf)-bufPos))
	}

	for i := 0; i < nrPeers; i++ {
		read, p := UnmarshalPeerWithIDChain(buf[bufPos:])
		peers[i] = p
		bufPos += read
	}
	return bufPos, peers
}

func SerializePeerWithIDChainArray(peers []*PeerWithIDChain) []byte {
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
	toReturn := make([]byte, nrSegments*IDSegmentLen)
	var currSegment uint16

	for currSegment = 0; currSegment < nrSegments; currSegment++ {
		copy(toReturn[int(currSegment)*IDSegmentLen:], id[currSegment][:])
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
	*PeerWithIDChain
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

func NewMeasuredPeer(p *PeerWithIDChain, measuredLatency time.Duration) *MeasuredPeer {
	return &MeasuredPeer{
		PeerWithIDChain: p,
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
	n, aux := UnmarshalPeerWithIDChain(buf[8:])
	return n + 8, &MeasuredPeer{
		PeerWithIDChain: aux,
		MeasuredLatency: latency,
	}
}

func DeserializeMeasuredPeerArray(buf []byte) (int, []*MeasuredPeer) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]*MeasuredPeer, nrPeers)
	bufPos := 4

	for i := 0; i < nrPeers; i++ {
		p := &MeasuredPeer{}
		read, p := p.UnmarshalMeasuredPeer(buf[bufPos:])
		peers[i] = p
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
