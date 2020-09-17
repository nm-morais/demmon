package membership

import (
	"bytes"
	"encoding/binary"

	"github.com/nm-morais/go-babel/pkg/peer"
)

const IdSegmentLen = 8

type PeerID = [IdSegmentLen]byte

type PeerIDChain = []PeerID

// IsDescendant retuns true if id1 contains the whole id 2 (which means that peer with id1 is higher up in id2's tree)
func IsDescendant(ascendant PeerIDChain, possibleDescendant PeerIDChain) bool {

	if len(ascendant) == 0 || len(possibleDescendant) == 0 {
		return false
	}

	if len(ascendant) > len(possibleDescendant) {
		return false
	}

	for i := 0; i < len(ascendant); i++ {
		if !bytes.Equal(ascendant[i][:], possibleDescendant[i][:]) {
			return false
		}
	}

	return true
}

type PeerWithId interface {
	ID() PeerID
	Peer() peer.Peer
	NrChildren() uint16
	SetChildrenNr(uint16)
	SerializeToBinary() []byte
}

type peerWithId struct {
	nChildren uint16
	id        PeerID
	self      peer.Peer
}

func NewPeerWithId(peerID PeerID, peer peer.Peer, nChildren int) PeerWithId {
	return &peerWithId{
		nChildren: uint16(nChildren),
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

func (p *peerWithId) Peer() peer.Peer {
	return p.self
}

type PeerWithIdChain interface {
	Chain() PeerIDChain
	Peer() peer.Peer
	NrChildren() uint16
	SetChildrenNr(uint16)
	SerializeToBinary() []byte
}

type peerWithIdChain struct {
	nChildren uint16
	chain     PeerIDChain
	self      peer.Peer
}

func NewPeerWithIdChain(peerIdChain PeerIDChain, peer peer.Peer, nChildren uint16) PeerWithIdChain {
	return &peerWithIdChain{
		nChildren: uint16(nChildren),
		chain:     peerIdChain,
		self:      peer,
	}
}

func (p *peerWithIdChain) SetChildrenNr(nChildren uint16) {
	p.nChildren = nChildren
}

func (p *peerWithIdChain) ID() PeerID {
	return p.chain[len(p.chain)-1]
}

func (p *peerWithIdChain) Chain() PeerIDChain {
	return p.chain
}

func (p *peerWithIdChain) NrChildren() uint16 {
	return p.nChildren
}

func (p *peerWithIdChain) Peer() peer.Peer {
	return p.self
}

func DeserializePeerWithIdChain(bytes []byte) (int, *peerWithIdChain) {
	nrChildren := binary.BigEndian.Uint16(bytes[0:2])
	nrPeerBytes, peer := peer.DeserializePeer(bytes[2:])
	nrChainBytes, peerChain := DeserializePeerIDChain(bytes[nrPeerBytes+2:])
	return nrPeerBytes + nrChainBytes + 2, &peerWithIdChain{
		nChildren: nrChildren,
		self:      peer,
		chain:     peerChain,
	}
}

func (p *peerWithIdChain) SerializeToBinary() []byte {
	nrChildrenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(nrChildrenBytes, p.nChildren)
	peerBytes := p.self.SerializeToBinary()
	chainBytes := SerializePeerIDChain(p.chain)
	return append(nrChildrenBytes, append(peerBytes, chainBytes...)...)

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
