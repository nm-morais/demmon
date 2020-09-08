package membership

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/nm-morais/go-babel/pkg/peer"
)

const IdSegmentLen = 8

type PeerID = [IdSegmentLen]byte
type PeerIDChain = []PeerID

type PeerWithId interface {
	ID() PeerID
	Peer() peer.Peer
	SerializeToBinary() []byte
}

type peerWithId struct {
	id   PeerID
	self peer.Peer
}

func NewPeerWithId(peerID PeerID, peer peer.Peer) PeerWithId {
	return &peerWithId{
		id:   peerID,
		self: peer,
	}
}

func (p *peerWithId) ID() PeerID {
	return p.id
}

func (p *peerWithId) Peer() peer.Peer {
	return p.self
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
	fmt.Println("Segments:", nrSegments)
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
	n := copy(peerID[:], bytes[0:IdSegmentLen])
	nrPeerBytes, peer := peer.DeserializePeer(bytes[n:])
	return nrPeerBytes + n, &peerWithId{
		self: peer,
		id:   peerID,
	}
}

func (p *peerWithId) SerializeToBinary() []byte {
	idBytes := p.id
	peerBytes := p.self.SerializeToBinary()
	return append(idBytes[:], peerBytes...)
}

func DeserializePeerWithIDArray(buf []byte) (int, []PeerWithId) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]PeerWithId, nrPeers)
	bufPos := 4
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
