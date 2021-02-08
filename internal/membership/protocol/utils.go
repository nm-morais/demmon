package protocol

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/nm-morais/demmon/internal/utils"
	"github.com/nm-morais/go-babel/pkg/nodeWatcher"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/timer"
)

func (d *DemmonTree) printLatencyCollectionStats() {
	sb := strings.Builder{}

	toStrWithLat := func(p peer.Peer) string {
		if p == nil {
			return ""
		}

		nodeInfo, err := d.nodeWatcher.GetNodeInfo(p)

		if err != nil {
			return ""
		}
		return fmt.Sprintf("%s:%d;", p.IP().String(), nodeInfo.LatencyCalc().CurrValue().Milliseconds())
	}

	for _, child := range d.myChildren {
		if _, err := sb.WriteString(toStrWithLat(child)); err != nil {
			panic(err)
		}
	}

	for _, sibling := range d.mySiblings {
		if _, err := sb.WriteString(toStrWithLat(sibling)); err != nil {
			panic(err)
		}
	}

	if d.myParent != nil {
		if _, err := sb.WriteString(toStrWithLat(d.myParent)); err != nil {
			panic(err)
		}
	}
	d.logger.Infof("<latency_collection> %s", sb.String())
}

func (d *DemmonTree) printInViewStats() {
	type peerWithLatency struct {
		Name    string
		Latency int
	}

	type viewWithLatencies struct {
		Parent   *peerWithLatency   `json:"parent,omitempty"`
		Children []*peerWithLatency `json:"children,omitempty"`
		Siblings []*peerWithLatency `json:"siblings,omitempty"`
	}
	tmp := viewWithLatencies{
		Parent:   nil,
		Children: []*peerWithLatency{},
		Siblings: []*peerWithLatency{},
	}
	getNodeWithLat := func(p *PeerWithIDChain) *peerWithLatency {
		if p == nil {
			return nil
		}

		nodeInfo, err := d.nodeWatcher.GetNodeInfo(p)

		if err != nil {
			return nil
		}
		return &peerWithLatency{
			Name:    p.String(),
			Latency: int(nodeInfo.LatencyCalc().CurrValue()),
		}
	}

	for _, child := range d.myChildren {
		if aux := getNodeWithLat(child); aux != nil {
			tmp.Children = append(tmp.Children, aux)
		}
	}

	for _, sibling := range d.mySiblings {
		if aux := getNodeWithLat(sibling); aux != nil {
			tmp.Siblings = append(tmp.Siblings, aux)
		}
	}

	if aux := getNodeWithLat(d.myParent); aux != nil {
		tmp.Parent = aux
	}

	res, err := json.Marshal(tmp)
	if err != nil {
		panic(err)
	}

	d.logger.Infof("<inView> %s", string(res))
}

func (d *DemmonTree) handleDebugTimer(joinTimer timer.Timer) {
	d.printLatencyCollectionStats()
	d.printInViewStats()
}

func (d *DemmonTree) generateChildID() PeerID {
	i := 0
outer:
	for {
		i++
		if i == 5 { // for safety
			panic("could not generate child ID in 5 iterations")
		}
		var peerID PeerID
		n, err := rand.Read(peerID[:])
		if err != nil {
			panic(err)
		}
		if n != len(peerID) {
			panic("rand did not write all peerID array")
		}

		for _, c := range d.myChildren {
			if peerID.String() == c.chain[len(c.chain)-1].String() {
				continue outer
			}
		}

		d.logger.Infof("Generated peerID: %+v", peerID)
		return peerID
	}
}

func peerMapToArr(peers map[string]*PeerWithIDChain) []*PeerWithIDChain {
	toReturn := make([]*PeerWithIDChain, 0, len(peers))
	for _, p := range peers {
		toReturn = append(toReturn, p)
	}
	return toReturn
}

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

func (d *DemmonTree) getPeerMapAsPeerMeasuredArr(peerMap map[string]*PeerWithIDChain, exclusions ...*PeerWithIDChain) MeasuredPeersByLat {
	measuredPeers := make(MeasuredPeersByLat, 0, len(peerMap))

	for _, p := range peerMap {

		found := false
		for _, exclusion := range exclusions {
			if peer.PeersEqual(exclusion, p) {
				found = true
				break
			}
		}
		if found {
			continue
		}
		nodeStats, err := d.nodeWatcher.GetNodeInfo(p.Peer)
		var currLat time.Duration
		if err != nil {
			d.logger.Warnf("Do not have latency measurement for %s", p.String())
			currLat = math.MaxInt64
		} else {
			currLat = nodeStats.LatencyCalc().CurrValue()
		}
		measuredPeers = append(measuredPeers, NewMeasuredPeer(p, currLat))
	}
	sort.Sort(measuredPeers)

	return measuredPeers
}

func (d *DemmonTree) isNodeDown(n nodeWatcher.NodeInfo) bool {
	return !n.Detector().IsAvailable()
}
