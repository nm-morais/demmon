package membership

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
	"math"
)

const PeerCollectionCaller = "PeerCollection"

type PeerCollection interface {
	AddPeer(peer peer.Peer) errors.Error
	RemovePeer(peer peer.Peer) errors.Error
	GetPeer(peer peer.Peer) (*peer.Peer, bool)
	Contains(peer peer.Peer) bool
	ContainsAll(peers []peer.Peer) bool
}

type PeerCollectionWithLatency interface {
	AddPeer(peer peer.Peer) errors.Error
	RemovePeer(peer peer.Peer) errors.Error
	GetPeer(peer peer.Peer) (*peer.Peer, bool)
	Contains(peer peer.Peer) bool
	ContainsAll(peers []peer.Peer) bool

	AddPeerWithLatency(peer peer.Peer, latency uint64)
	GetLowestLatencyPeer() (*peerWithLatency, errors.Error)
	GetPeersWithLatencyUnder(threshold uint64, exclusions ...peer.Peer) []peer.Peer
}

type peerMap struct {
	peers map[string]*peerWithLatency
}

type peerWithLatency struct {
	latency uint64
	peer    peer.Peer
}

func NewPeerCollectionWithLatency(estimatedSize int) PeerCollectionWithLatency {
	return &peerMap{
		peers: make(map[string]*peerWithLatency, estimatedSize),
	}
}

func NewPeerCollection(estimatedSize int) PeerCollection {
	return &peerMap{
		peers: make(map[string]*peerWithLatency, estimatedSize),
	}
}

func (p *peerMap) GetPeer(peer peer.Peer) (*peer.Peer, bool) {
	if !p.Contains(peer) {
		return nil, false
	}
	return &p.peers[peer.ToString()].peer, true
}

func (p *peerMap) AddPeerWithLatency(peer peer.Peer, latency uint64) {
	p.peers[peer.ToString()] = &peerWithLatency{
		latency: latency,
		peer:    peer,
	}
}

func (p *peerMap) AddPeer(peer peer.Peer) errors.Error {
	if p.Contains(peer) {
		return errors.NonFatalError(409, "Peer already in collection", PeerCollectionCaller)
	}
	p.peers[peer.ToString()] = &peerWithLatency{
		latency: -1,
		peer:    peer,
	}
	return nil
}

func (p *peerMap) RemovePeer(peer peer.Peer) errors.Error {
	if !p.Contains(peer) {
		return errors.NonFatalError(404, "Peer not in collection", PeerCollectionCaller)
	}
	delete(p.peers, peer.ToString())
	return nil
}

func (p *peerMap) Contains(peer peer.Peer) bool {
	_, ok := p.peers[peer.ToString()]
	return ok
}

func (p *peerMap) ContainsAll(peers []peer.Peer) bool {
	for _, curr := range peers {
		if !p.Contains(curr) {
			return false
		}
	}
	return true
}

func (p *peerMap) AllPeersHaveLatency() bool {
	for _, curr := range p.peers {
		if curr.latency == math.MaxUint64 {
			return false
		}
	}
	return true
}

func (p *peerMap) GetLowestLatencyPeer() (*peerWithLatency, errors.Error) {

	if len(p.peers) == 0 {
		return nil, errors.NonFatalError(404, "peer collection is empty", PeerCollectionCaller)
	}

	lowest := &peerWithLatency{
		latency: math.MaxUint64,
		peer:    nil,
	}
	for _, peerWithLatency := range p.peers {
		if peerWithLatency.latency < lowest.latency {
			lowest = peerWithLatency
		}
	}
	return lowest, nil
}

func (p *peerMap) GetPeersWithLatencyUnder(threshold uint64, exclusions ...peer.Peer) []peer.Peer {
	toReturn := make([]peer.Peer, 0, len(p.peers))
	added := 0
	for _, currPeer := range p.peers {
		if currPeer.latency < threshold {
			excluded := false
			for _, exclusion := range exclusions {
				if exclusion.Equals(currPeer.peer) {
					excluded = true
					break
				}
			}
			if !excluded {
				toReturn[added] = currPeer.peer
				added++
			}
		}
	}
	return toReturn
}
