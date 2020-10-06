package membership

import (
	"math/big"
	"math/rand"
	"net"

	"github.com/nm-morais/go-babel/pkg/peer"
)

func getRandSample(nrPeersToSelect int, peers ...*PeerWithIdChain) []*PeerWithIdChain {
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	nrPeersToReturn := nrPeersToSelect
	if nrPeersToReturn > len(peers) {
		nrPeersToReturn = len(peers)
	}

	toReturn := make([]*PeerWithIdChain, nrPeersToReturn)
	for i := 0; i < len(peers) && i < nrPeersToReturn; i++ {
		toReturn[i] = peers[i]
	}
	return toReturn
}

func getPeersExcluding(toFilter []*PeerWithIdChain, exclusions ...peer.Peer) []*PeerWithIdChain {
	toReturn := make([]*PeerWithIdChain, 0)
	for _, p := range toFilter {
		excluded := false
		for _, exclusion := range exclusions {
			if peer.PeersEqual(exclusion, p) {
				excluded = true
				break
			}
		}
		if !excluded {
			toReturn = append(toReturn, p)
		}
	}
	return toReturn
}

func getRandomExcluding(toFilter []*PeerWithIdChain, exclusions ...peer.Peer) *PeerWithIdChain {
	filtered := getPeersExcluding(toFilter, exclusions...)
	filteredLen := len(filtered)
	if filteredLen == 0 {
		return nil
	}
	return filtered[rand.Intn(filteredLen)]
}

func getBiasedPeerExcluding(toFilter []*PeerWithIdChain, biasTowards *PeerWithIdChain, exclusions ...peer.Peer) *PeerWithIdChain {
	filtered := getPeersExcluding(toFilter, exclusions...)
	var minDist int64 = -1
	var bestPeer *PeerWithIdChain
	for _, peer := range filtered {
		currDist := xorDistance(peer.IP(), biasTowards.IP())
		if currDist.Int64() < minDist {
			minDist = currDist.Int64()
			bestPeer = peer
		}
	}
	return bestPeer
}

func getExcludingDescendantsOf(toFilter []*PeerWithIdChain, ascendantChain PeerIDChain) []*PeerWithIdChain {
	toReturn := make([]*PeerWithIdChain, 0)
	for _, peer := range toFilter {
		if !peer.IsDescendentOf(ascendantChain) {
			toReturn = append(toReturn, peer)
		}
	}
	return toReturn
}

func removeNFromSample(sample []*PeerWithIdChain, nrPeersToRemove int, excludeDescendantsOf PeerIDChain) (remaining, removed []*PeerWithIdChain) {
	for i := 0; i < nrPeersToRemove && i < len(sample); i++ {
		peer := sample[i]
		if !peer.IsDescendentOf(excludeDescendantsOf) {
			copy(sample[i:], sample[i+1:])
			sample[len(sample)-1] = nil
			sample = sample[:len(sample)-1]
			removed = append(removed, peer)
		}
	}
	return sample, removed
}

func xorDistance(ip1 net.IP, ip2 net.IP) *big.Int {
	var rawBytes [32]byte
	ip1_4 := ip1.To4()
	ip2_4 := ip2.To4()

	for i := 0; i < len(ip1_4); i++ {
		rawBytes[i] = ip1_4[i] ^ ip2_4[i]
	}
	return big.NewInt(0).SetBytes(rawBytes[:])
}
