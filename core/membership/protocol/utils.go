package protocol

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/nm-morais/demmon/core/utils"
	"github.com/nm-morais/go-babel/pkg/errors"
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
	type InViewPeer struct {
		IP string `json:"ip,omitempty"`
	}

	type viewWithLatencies struct {
		Parent   *InViewPeer   `json:"parent,omitempty"`
		Children []*InViewPeer `json:"children,omitempty"`
		Siblings []*InViewPeer `json:"siblings,omitempty"`
	}
	tmp := viewWithLatencies{
		Parent:   nil,
		Children: []*InViewPeer{},
		Siblings: []*InViewPeer{},
	}
	convert := func(p *PeerWithIDChain) *InViewPeer {
		return &InViewPeer{
			IP: p.IP().String(),
		}
	}

	for _, child := range d.myChildren {
		tmp.Children = append(tmp.Children, convert(child))
	}

	for _, sibling := range d.mySiblings {
		tmp.Siblings = append(tmp.Siblings, convert(sibling))
	}
	if d.myParent != nil {
		tmp.Parent = convert(d.myParent)
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
		peerID := PeerID{}
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		n, err := r.Read(peerID[:])
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

func PeerWithIDChainMapToArr(peers map[string]*PeerWithIDChain) []*PeerWithIDChain {
	toReturn := make([]*PeerWithIDChain, 0)
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
	toReturn := make(map[string]*PeerWithIDChain)
	for i := 0; i < len(peers) && i < nrPeersToReturn; i++ {
		toReturn[peers[i].String()] = peers[i]
	}
	return toReturn
}

func getPeersExcluding(toFilter []*PeerWithIDChain, exclusions map[string]bool) []*PeerWithIDChain {
	toReturn := make([]*PeerWithIDChain, 0)

	for _, p := range toFilter {
		_, excluded := exclusions[p.String()]
		if excluded {
			continue
		}
		toReturn = append(toReturn, p)
	}
	return toReturn
}

func getRandomExcluding(toFilter []*PeerWithIDChain, exclusions map[string]bool) *PeerWithIDChain {
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
		if peer.IsDescendentOf(ascendantChain) {
			continue
		}
		toReturn = append(toReturn, peer)
	}
	return toReturn
}

func (d *DemmonTree) getPeerWithIDChainMapAsPeerMeasuredArr(peerMap map[string]*PeerWithIDChain, exclusions ...*PeerWithIDChain) MeasuredPeersByLat {
	measuredPeers := make(MeasuredPeersByLat, 0)
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
		measuredPeer, err := d.getPeerWithChainAsMeasuredPeer(p)
		if err != nil {
			continue
		}
		measuredPeers = append(measuredPeers, measuredPeer)
	}
	sort.Sort(measuredPeers)
	return measuredPeers
}

func (d *DemmonTree) getPeerWithChainAsMeasuredPeer(p *PeerWithIDChain) (*MeasuredPeer, errors.Error) {
	info, err := d.nodeWatcher.GetNodeInfo(p)
	if err != nil {
		return nil, err
	}
	return NewMeasuredPeer(p, info.LatencyCalc().CurrValue()), nil
}

func (d *DemmonTree) isNodeDown(n nodeWatcher.NodeInfo) bool {
	return !n.Detector().IsAvailable()
}
