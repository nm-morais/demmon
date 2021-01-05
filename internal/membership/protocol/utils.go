package protocol

import (
	"math/rand"

	"github.com/nm-morais/demmon/internal/utils"
)

func getRandSample(nrPeersToSelect int, peers ...*PeerWithIDChain) map[string]*PeerWithIDChain {
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	nrPeersToReturn := nrPeersToSelect
	if nrPeersToReturn > len(peers) {
		nrPeersToReturn = len(peers)
	}
	toReturn := make(map[string]*PeerWithIDChain, nrPeersToSelect)
	for i := 0; i < len(peers) && i < nrPeersToReturn; i++ {
		toReturn[peers[i].String()] = peers[i]
	}
	return toReturn
}

func getPeersExcluding(toFilter []*PeerWithIDChain, exclusions map[string]interface{}) []*PeerWithIDChain {
	toReturn := make([]*PeerWithIDChain, 0)

	for _, p := range toFilter {
		_, excluded := exclusions[p.String()]
		if !excluded {
			toReturn = append(toReturn, p)
		}
	}
	return toReturn
}

func getRandomExcluding(toFilter []*PeerWithIDChain, exclusions map[string]interface{}) *PeerWithIDChain {
	filtered := getPeersExcluding(toFilter, exclusions)
	filteredLen := len(filtered)
	if filteredLen == 0 {
		return nil
	}
	return filtered[utils.GetRandInt(filteredLen)]
}

// func getBiasedPeerExcluding(toFilter []*PeerWithIdChain, biasTowards *PeerWithIdChain, exclusions ...peer.Peer) *PeerWithIdChain {
// 	filtered := getPeersExcluding(toFilter, exclusions...)
// 	var minDist int64 = -1
// 	var bestPeer *PeerWithIdChain
// 	for _, peer := range filtered {
// 		currDist := xorDistance(peer.IP(), biasTowards.IP())
// 		if currDist.Int64() < minDist {
// 			minDist = currDist.Int64()
// 			bestPeer = peer
// 		}
// 	}
// 	return bestPeer
// }

func getExcludingDescendantsOf(toFilter []*PeerWithIDChain, ascendantChain PeerIDChain) []*PeerWithIDChain {
	toReturn := make([]*PeerWithIDChain, 0)

	for _, peer := range toFilter {
		if !peer.IsDescendentOf(ascendantChain) {
			toReturn = append(toReturn, peer)
		}
	}
	return toReturn
}

// func xorDistance(ip1 net.IP, ip2 net.IP) *big.Int {
// 	var rawBytes [32]byte
// 	ip1_4 := ip1.To4()
// 	ip2_4 := ip2.To4()

// 	for i := 0; i < len(ip1_4); i++ {
// 		rawBytes[i] = ip1_4[i] ^ ip2_4[i]
// 	}
// 	return big.NewInt(0).SetBytes(rawBytes[:])
// }
