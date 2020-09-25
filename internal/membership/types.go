package membership

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

const IdSegmentLen = 8

type PeerID = [IdSegmentLen]byte

type PeerIDChain = []PeerID

type PeerWithId interface {
	IP() net.IP
	ProtosPort() uint16
	AnalyticsPort() uint16
	Equals(other peer.Peer) bool
	SerializeToBinary() []byte
	ToString() string
	ID() PeerID
	NrChildren() uint16
	SetChildrenNr(uint16)
}

type peerWithId struct {
	nChildren uint16
	id        PeerID
	self      peer.Peer
}

func getParentChainFrom(idChain PeerIDChain) PeerIDChain {
	if len(idChain) == 0 {
		return nil
	}
	return idChain[:len(idChain)-1]
}

func NewPeerWithId(peerID PeerID, peer peer.Peer, nChildren uint16) PeerWithId {
	return &peerWithId{
		nChildren: nChildren,
		id:        peerID,
		self:      peer,
	}
}

func (p *peerWithId) SetChildrenNr(nChildren uint16) {
	p.nChildren = nChildren
}

func (p *peerWithId) ID() PeerID {
	return p.id
}

func (p *peerWithId) NrChildren() uint16 {
	return p.nChildren
}

func (p *peerWithId) IP() net.IP {
	return p.self.IP()
}

func (p *peerWithId) ProtosPort() uint16 {
	return p.self.ProtosPort()
}

func (p *peerWithId) AnalyticsPort() uint16 {
	return p.self.AnalyticsPort()
}

func (p *peerWithId) ToString() string {

	if p == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s:%d", p.self.IP(), p.self.ProtosPort())
}

func (p *peerWithId) Equals(otherPeer peer.Peer) bool {

	if p == nil {
		return false
	}

	if otherPeer == nil {
		return false
	}

	return p.ToString() == otherPeer.ToString()
}

type PeerWithIdChain interface {
	IP() net.IP
	ProtosPort() uint16
	AnalyticsPort() uint16
	Equals(other peer.Peer) bool
	SerializeToBinary() []byte
	ToString() string
	NrChildren() uint16
	SetChildrenNr(uint16)
	Chain() PeerIDChain
	SetChain(PeerIDChain)
	IsParentOf(otherPeer PeerIDChain) bool
	IsDescendentOf(otherPeer PeerIDChain) bool
}

type peerWithIdChain struct {
	chain     PeerIDChain
	nChildren uint16
	self      peer.Peer
}

func NewPeerWithIdChain(peerIdChain PeerIDChain, self peer.Peer, nChildren uint16) PeerWithIdChain {
	return &peerWithIdChain{
		nChildren: nChildren,
		chain:     peerIdChain,
		self:      peer.NewPeer(self.IP(), self.ProtosPort(), self.AnalyticsPort()),
	}
}

func (p *peerWithIdChain) SetChildrenNr(nChildren uint16) {
	p.nChildren = nChildren
}

func (p *peerWithIdChain) Chain() PeerIDChain {
	return p.chain
}

func (p *peerWithIdChain) SetChain(newChain PeerIDChain) {
	p.chain = newChain
}

func (p *peerWithIdChain) NrChildren() uint16 {
	return p.nChildren
}

func (p *peerWithIdChain) IP() net.IP {
	return p.self.IP()
}

func (p *peerWithIdChain) ProtosPort() uint16 {
	return p.self.ProtosPort()
}

func (p *peerWithIdChain) AnalyticsPort() uint16 {
	return p.self.AnalyticsPort()
}

func (p *peerWithIdChain) ToString() string {

	if p == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s:%d", p.self.IP(), p.self.ProtosPort())
}

func (p *peerWithIdChain) Equals(otherPeer peer.Peer) bool {

	if p == nil {
		return false
	}

	if otherPeer == nil {
		return false
	}

	return p.ToString() == otherPeer.ToString()
}

func (p *peerWithIdChain) IsDescendentOf(otherPeerChain PeerIDChain) bool {
	// IsDescendant retuns true if id1 contains the whole id 2 (which means that peer with id1 is higher up in id2's tree)

	selfChain := p.chain

	if len(selfChain) == 0 || len(otherPeerChain) == 0 {
		return false
	}

	if len(selfChain) > len(otherPeerChain) {
		return false
	}

	for i := 0; i < len(selfChain); i++ {
		if !bytes.Equal(selfChain[i][:], otherPeerChain[i][:]) {
			return false
		}
	}
	return true
}

func (p *peerWithIdChain) IsParentOf(otherPeerChain PeerIDChain) bool {

	selfChain := p.chain

	if len(selfChain) == 0 || len(otherPeerChain) == 0 || (len(selfChain) != len(otherPeerChain)-1) {
		return false
	}

	for i := 0; i < len(selfChain); i++ {
		if !bytes.Equal(selfChain[i][:], otherPeerChain[i][:]) {
			return false
		}
	}

	return true
}

func (p *peerWithIdChain) SerializeToBinary() []byte {
	nrChildrenBytes := make([]byte, 2)
	peerBytes := make([]byte, 8)
	for idx, b := range p.IP().To4() {
		peerBytes[idx] = b
	}
	binary.BigEndian.PutUint16(peerBytes[4:], p.ProtosPort())
	binary.BigEndian.PutUint16(peerBytes[6:], p.AnalyticsPort())
	binary.BigEndian.PutUint16(nrChildrenBytes, p.NrChildren())

	chainBytes := SerializePeerIDChain(p.chain)
	nrChildrenBytes = append(nrChildrenBytes, peerBytes...)
	nrChildrenBytes = append(nrChildrenBytes, chainBytes...)

	return nrChildrenBytes

}

func DeserializePeerWithIdChain(bytes []byte) (int, *peerWithIdChain) {

	nrChildren := binary.BigEndian.Uint16(bytes[0:2])
	nrPeerBytes, peer := peer.DeserializePeer(bytes[2:])
	nrChainBytes, peerChain := DeserializePeerIDChain(bytes[nrPeerBytes+2:])

	if len(peerChain) == 0 {
		fmt.Printf("peer %s with ID=%+v has chain with len == 0\n", peer.ToString(), peerChain)
		panic("peer chain is 0\n")
	}
	return nrPeerBytes + nrChainBytes + 2, &peerWithIdChain{
		chain: peerChain,
		self:  NewPeerWithId(peerChain[len(peerChain)-1], peer, nrChildren),
	}
}

func DeserializePeerWithIDChainArray(buf []byte) (int, []PeerWithIdChain) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]PeerWithIdChain, nrPeers)
	bufPos := 4
	for i := 0; i < nrPeers; i++ {
		read, peer := DeserializePeerWithIdChain(buf[bufPos:])
		peers[i] = peer
		bufPos += read
	}
	return bufPos, peers
}

func SerializePeerWithIDChainArray(peers []PeerWithIdChain) []byte {
	totalBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalBytes, uint32(len(peers)))
	for _, p := range peers {
		totalBytes = append(totalBytes, p.SerializeToBinary()...)
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

func DeserializePeerWithId(bytes []byte) (int, PeerWithId) {
	var peerID PeerID
	bufPos := 0
	nrChildren := binary.BigEndian.Uint16(bytes[bufPos:])
	bufPos += 2
	n := copy(peerID[:], bytes[bufPos:])
	bufPos += n
	nrPeerBytes, peer := peer.DeserializePeer(bytes[bufPos:])
	bufPos += nrPeerBytes
	return bufPos, &peerWithId{
		nChildren: nrChildren,
		self:      peer,
		id:        peerID,
	}
}

func (p *peerWithId) SerializeToBinary() []byte {
	idBytes := p.id
	nrChildrenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(nrChildrenBytes, p.nChildren)
	peerBytes := p.self.SerializeToBinary()
	nrChildrenBytes = append(nrChildrenBytes, idBytes[:]...)
	nrChildrenBytes = append(nrChildrenBytes, peerBytes...)
	return nrChildrenBytes
}

func DeserializePeerWithIDArray(buf []byte) (int, []PeerWithId) {
	bufPos := 0
	nrPeers := int(binary.BigEndian.Uint32(buf[bufPos:]))
	bufPos += 4
	peers := make([]PeerWithId, nrPeers)
	for i := 0; i < nrPeers; i++ {
		read, peer := DeserializePeerWithId(buf[bufPos:])
		peers[i] = peer
		bufPos += read
	}
	return bufPos, peers
}

func SerializePeerWithIDArray(peers []PeerWithId) []byte {
	totalBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalBytes, uint32(len(peers)))
	for _, p := range peers {
		totalBytes = append(totalBytes, p.SerializeToBinary()...)
	}
	return totalBytes
}

func ChainsEqual(chain1 PeerIDChain, chain2 PeerIDChain) bool {
	if len(chain1) != len(chain2) {
		return false
	}

	for i := 0; i < len(chain1); i++ {
		if !bytes.Equal(chain1[i][:], chain2[i][:]) {
			return false
		}
	}

	return true
}

type measuredPeer struct {
	peer            PeerWithIdChain
	measuredLatency time.Duration
}

type MeasuredPeer = interface {
	IP() net.IP
	ProtosPort() uint16
	AnalyticsPort() uint16
	Equals(other peer.Peer) bool
	SerializeToBinary() []byte
	ToString() string
	ID() PeerID
	NrChildren() uint16
	SetChildrenNr(uint16)
	Chain() PeerIDChain
	IsParentOf(otherPeer PeerIDChain) bool
	IsDescendentOf(otherPeer PeerIDChain) bool
	MeasuredLatency() time.Duration
}

type MeasuredPeersByLat []MeasuredPeer

func (p MeasuredPeersByLat) Len() int {
	return len(p)
}
func (p MeasuredPeersByLat) Less(i, j int) bool {
	return p[i].MeasuredLatency() < p[j].MeasuredLatency()
}
func (p MeasuredPeersByLat) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func NewMeasuredPeer(peer PeerWithIdChain, measuredLatency time.Duration) MeasuredPeer {
	return &measuredPeer{
		peer:            peer,
		measuredLatency: measuredLatency,
	}
}

func (p *measuredPeer) SetChildrenNr(nChildren uint16) {
	p.peer.SetChildrenNr(nChildren)
}

func (p *measuredPeer) ID() PeerID {
	return p.peer.Chain()[len(p.peer.Chain())-1]
}

func (p *measuredPeer) Chain() PeerIDChain {
	return p.peer.Chain()
}

func (p *measuredPeer) NrChildren() uint16 {
	return p.peer.NrChildren()
}

func (p *measuredPeer) IP() net.IP {
	return p.peer.IP()
}

func (p *measuredPeer) ProtosPort() uint16 {
	return p.peer.ProtosPort()
}

func (p *measuredPeer) AnalyticsPort() uint16 {
	return p.peer.AnalyticsPort()
}

func (p *measuredPeer) MeasuredLatency() time.Duration {
	return p.measuredLatency
}

func (p *measuredPeer) ToString() string {

	if p == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s:%d", p.peer.IP(), p.peer.ProtosPort())
}

func (p *measuredPeer) Equals(otherPeer peer.Peer) bool {

	if p == nil {
		return false
	}

	if otherPeer == nil {
		return false
	}

	return p.ToString() == otherPeer.ToString()
}

func (p *measuredPeer) IsDescendentOf(otherPeerChain PeerIDChain) bool {
	// IsDescendant retuns true if id1 contains the whole id 2 (which means that peer with id1 is higher up in id2's tree)

	selfChain := p.peer.Chain()

	if len(selfChain) == 0 || len(otherPeerChain) == 0 {
		return false
	}

	if len(selfChain) > len(otherPeerChain) {
		return false
	}

	for i := 0; i < len(selfChain); i++ {
		if !bytes.Equal(selfChain[i][:], otherPeerChain[i][:]) {
			return false
		}
	}

	return true
}

func (p *measuredPeer) IsParentOf(otherPeerChain PeerIDChain) bool {

	selfChain := p.peer.Chain()

	if len(selfChain) == 0 || len(otherPeerChain) == 0 || (len(selfChain) != len(otherPeerChain)-1) {
		return false
	}

	for i := 0; i < len(selfChain); i++ {
		if !bytes.Equal(selfChain[i][:], otherPeerChain[i][:]) {
			return false
		}
	}

	return true
}

func (p *measuredPeer) SerializeToBinary() []byte {
	peerBytes := p.peer.SerializeToBinary()
	latencyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(latencyBytes, uint64(p.measuredLatency))
	return append(peerBytes, latencyBytes...)

}
