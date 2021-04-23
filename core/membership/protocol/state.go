package protocol

import (
	"time"

	"github.com/nm-morais/demmon/core/utils"
	"github.com/nm-morais/go-babel/pkg/peer"
)

func getMapAsPeerWithIDChainArray(peers map[string]*PeerWithIDChain, exclusions ...*PeerWithIDChain) []*PeerWithIDChain {
	toReturn := make([]*PeerWithIDChain, 0, len(peers))
	for _, child := range peers {

		excluded := false
		for _, exclusion := range exclusions {

			if peer.PeersEqual(exclusion, child.Peer) {
				excluded = true
				break
			}
		}
		if !excluded {
			toReturn = append(toReturn, child)
		}
	}
	return toReturn
}

func (d *DemmonTree) getPeerRelationshipType(p peer.Peer) (isSibling, isChildren, isParent bool) {
	for _, sibling := range d.mySiblings {
		if peer.PeersEqual(sibling, p) {
			return true, false, false
		}
	}

	for _, children := range d.myChildren {
		if peer.PeersEqual(children, p) {
			return false, true, false
		}
	}

	if peer.PeersEqual(p, d.myParent) {
		return false, false, true
	}

	return false, false, false
}

func (d *DemmonTree) addParent(
	newParent *PeerWithIDChain,
	newGrandParent *PeerWithIDChain,
	myNewChain PeerIDChain) {

	haveCause := false
	hasInConnection := false
	hasOutConnection := false
	var existingLatencyMeasurement *time.Duration

	// TODO add to measured peers
	if peer.PeersEqual(newParent, d.myPendingParentInRecovery) {
		d.myPendingParentInRecovery = nil
		haveCause = true
	}

	if peer.PeersEqual(newParent, d.myPendingParentInImprovement) {
		existingLatencyMeasurement = &d.myPendingParentInImprovement.Latency
		d.myPendingParentInImprovement = nil
		haveCause = true
	}

	if d.myPendingParentInJoin != nil && peer.PeersEqual(newParent, d.myPendingParentInJoin.peer) {
		existingLatencyMeasurement = &d.myPendingParentInJoin.peer.Latency
		d.joined = true
		d.joinMap = nil // TODO cleanup join function
		d.bestPeerlastLevel = nil
		d.myPendingParentInJoin = nil
		haveCause = true
	}

	if peer.PeersEqual(newParent, d.myPendingParentInAbsorb) {
		hasOutConnection = d.myPendingParentInAbsorb.outConnActive
		hasInConnection = d.myPendingParentInAbsorb.inConnActive
		d.myPendingParentInAbsorb = nil
		haveCause = true
	}

	if peer.PeersEqual(newParent, d.myPendingParentInClimb) {
		d.logger.Info("climbed successfully")
		d.myPendingParentInClimb = nil
		haveCause = true
	}

	if !haveCause {
		d.logger.Panicf("adding parent but peer is not in possible pending parents")
	}

	oldParent := d.myParent
	if d.myParent != nil && !peer.PeersEqual(d.myParent, newParent) {
		toSend := NewDisconnectAsChildMessage()
		d.myParent = nil
		d.babel.SendNotification(NewNodeDownNotification(oldParent, d.getInView(), false))
		d.sendMessageAndDisconnect(toSend, oldParent)
		d.nodeWatcher.Unwatch(oldParent, d.ID())
	}
	if !myNewChain.Equal(d.self.chain) {
		d.babel.SendNotification(NewIDChangeNotification(myNewChain))
	}

	d.myGrandParent = newGrandParent
	d.myParent = newParent

	d.logger.Infof(
		"My level changed: (%d -> %d)",
		d.self.Chain().Level(),
		newParent.Chain().Level()+1,
	) // IMPORTANT FOR VISUALIZER
	d.logger.Infof("My chain changed: (%+v -> %+v)", d.self.Chain(), myNewChain)
	d.logger.Infof("My parent changed: (%+v -> %+v)", oldParent, d.myParent)
	d.logger.Infof("HasOutConn: %+v", hasOutConnection)
	d.logger.Infof("HasInConn: %+v", hasInConnection)

	d.self = NewPeerWithIDChain(myNewChain, d.self.Peer, d.self.nChildren, d.self.Version()+1, d.self.Coordinates, d.config.BandwidthScore, d.getAvgChildrenBW())

	if existingLatencyMeasurement != nil {
		d.nodeWatcher.WatchWithInitialLatencyValue(newParent, d.ID(), *existingLatencyMeasurement)
	} else {
		d.nodeWatcher.Watch(newParent, d.ID())
	}
	d.removeFromMeasuredPeers(newParent)
	d.myParent.outConnActive = hasOutConnection
	d.myParent.inConnActive = hasInConnection
	if hasOutConnection {
		d.babel.SendNotification(NewNodeUpNotification(d.myParent, d.getInView()))

	} else {
		d.babel.Dial(d.ID(), newParent, newParent.ToTCPAddr())
	}

	for childStr, child := range d.myChildren {
		childID := child.Chain()[len(child.Chain())-1]
		newChildrenPtr := NewPeerWithIDChain(
			append(myNewChain, childID),
			child.Peer,
			child.nChildren,
			child.version,
			child.Coordinates,
			child.bandwidth,
			child.avgChildrenBW,
		)
		newChildrenPtr.inConnActive = child.inConnActive
		newChildrenPtr.outConnActive = child.outConnActive
		d.myChildren[childStr] = newChildrenPtr
	}
}

func (d *DemmonTree) getNeighborsAsPeerWithIDChainArray() []*PeerWithIDChain {
	possibilitiesToSend := make([]*PeerWithIDChain, 0) // parent and me

	if len(d.self.Chain()) > 0 {
		for _, child := range d.myChildren {
			possibilitiesToSend = append(possibilitiesToSend, child)
		}

		for _, sibling := range d.mySiblings {
			possibilitiesToSend = append(possibilitiesToSend, sibling)
		}

		if !d.landmark && d.myParent != nil {
			possibilitiesToSend = append(possibilitiesToSend, d.myParent)
		}
	}
	return possibilitiesToSend
}

func (d *DemmonTree) addChild(newChild *PeerWithIDChain, bwScore int, childrenLatency time.Duration, outConnActive, inConnActive bool) PeerID {
	proposedID := d.generateChildID()
	newChildWithID := NewPeerWithIDChain(
		append(d.self.Chain(), proposedID),
		newChild.Peer,
		newChild.nChildren,
		newChild.Version(),
		newChild.Coordinates,
		newChild.bandwidth,
		newChild.avgChildrenBW,
	)

	newChildWithID.inConnActive = inConnActive
	newChildWithID.outConnActive = outConnActive

	if !outConnActive {
		if childrenLatency != 0 {
			d.nodeWatcher.WatchWithInitialLatencyValue(newChild, d.ID(), childrenLatency)
		} else {
			d.nodeWatcher.Watch(newChild, d.ID())
		}
		d.babel.Dial(d.ID(), newChild, newChild.ToTCPAddr())
	} else {
		d.myChildren[newChild.String()] = newChildWithID
		d.babel.SendNotification(NewNodeUpNotification(newChild, d.getInView()))
	}

	d.myChildren[newChild.String()] = newChildWithID
	d.updateSelfVersion()
	d.logger.Infof("added children: %s", newChildWithID.String())
	d.removeFromMeasuredPeers(newChild)

	return proposedID
}

func (d *DemmonTree) updateSelfVersion() {
	d.self = NewPeerWithIDChain(
		d.self.Chain(),
		d.self.Peer,
		uint16(len(d.myChildren)),
		d.self.Version()+1,
		d.self.Coordinates,
		d.config.BandwidthScore,
		d.getAvgChildrenBW(),
	)
}

func (d *DemmonTree) removeChild(toRemove peer.Peer, crash bool) {
	d.logger.Infof("removing child: %s", toRemove.String())

	child, ok := d.myChildren[toRemove.String()]
	if !ok {
		d.logger.Panic("Removing child not in myChildren or myPendingChildren")
	}

	delete(d.myChildrenLatencies, toRemove.String())
	delete(d.myChildren, toRemove.String())
	d.updateSelfVersion()
	d.babel.SendNotification(NewNodeDownNotification(child, d.getInView(), crash))

	d.babel.Disconnect(d.ID(), toRemove)
	d.nodeWatcher.Unwatch(toRemove, d.ID())
	d.logger.Infof("Removed child: %s", toRemove.String())
}

func (d *DemmonTree) addSibling(newSibling *PeerWithIDChain) {
	d.logger.Infof("Adding sibling: %s", newSibling.String())
	_, ok := d.mySiblings[newSibling.String()]
	if ok {
		d.logger.Errorf("Added sibling: %s but it was already on the map", newSibling.String())
		return
	}

	d.mySiblings[newSibling.String()] = newSibling
	d.nodeWatcher.Watch(newSibling.Peer, d.ID())
	d.babel.Dial(d.ID(), newSibling.Peer, newSibling.Peer.ToTCPAddr())
	d.removeFromMeasuredPeers(newSibling)
	d.logger.Infof("Added sibling: %s", newSibling.String())
}

func (d *DemmonTree) removeSibling(toRemove peer.Peer, crash bool) {
	d.logger.Infof("Removing sibling: %s", toRemove.String())
	sibling, ok := d.mySiblings[toRemove.String()]
	if !ok {
		d.logger.Errorf("Removing sibling %s not in mySiblings", toRemove.String())
		return
	}
	delete(d.mySiblings, toRemove.String())
	d.babel.SendNotification(NewNodeDownNotification(sibling, d.getInView(), crash))

	d.nodeWatcher.Unwatch(sibling, d.ID())
	d.babel.Disconnect(d.ID(), toRemove)
	d.logger.Infof("Removed sibling: %s", toRemove.String())
}

func (d *DemmonTree) isNeighbour(toTest peer.Peer) bool {
	if peer.PeersEqual(toTest, d.babel.SelfPeer()) {
		d.logger.Panic("is self")
	}

	if peer.PeersEqual(toTest, d.myParent) {
		return true
	}

	if _, ok := d.mySiblings[toTest.String()]; ok {
		return true
	}

	if _, isChildren := d.myChildren[toTest.String()]; isChildren {
		return true
	}
	return false
}

func (d *DemmonTree) getInView() InView {
	var parent *PeerWithIDChain
	childArr := make([]*PeerWithIDChain, 0)
	for _, child := range d.myChildren {
		if !child.outConnActive {
			continue
		}

		childArr = append(childArr, child)
	}

	siblingArr := make([]*PeerWithIDChain, 0)
	for _, sibling := range d.mySiblings {
		if !sibling.outConnActive {
			continue
		}

		siblingArr = append(siblingArr, sibling)
	}

	if d.myParent != nil {
		if d.myParent.outConnActive {
			parent = d.myParent
		} else {
			d.logger.Infof("Have parent %s but no active connection to it", d.myParent.String())

		}
	}

	return InView{
		Parent:      parent,
		Grandparent: d.myGrandParent,
		Children:    childArr,
		Siblings:    siblingArr,
	}
}

func (d *DemmonTree) removeFromMeasuredPeers(p peer.Peer) {
	delete(d.measuredPeers, p.String())
}

func (d *DemmonTree) addToMeasuredPeers(p *MeasuredPeer) {
	if p.IsDescendentOf(d.self.chain) {
		return
	}

	_, alreadyMeasured := d.measuredPeers[p.String()]
	if alreadyMeasured || len(d.measuredPeers) < d.config.MaxMeasuredPeers {
		d.measuredPeers[p.String()] = p
		return
	}

	var maxLatPeerStr string = ""
	maxLat := time.Duration(0)
	for measuredPeerID, alreadyPresentPeer := range d.measuredPeers {
		if alreadyPresentPeer.Latency > maxLat {
			maxLat = alreadyPresentPeer.Latency
			maxLatPeerStr = measuredPeerID
		}
	}

	if maxLatPeerStr == "" {
		panic("Shouldnt happen")
	}

	if maxLat > p.Latency {
		delete(d.measuredPeers, maxLatPeerStr)
		d.measuredPeers[p.String()] = p
		return
	}
}

func (d *DemmonTree) removeFromEView(p peer.Peer) {
	delete(d.eView, p.String())
}

func (d *DemmonTree) addToEView(p *PeerWithIDChain) {
	if p == nil {
		d.logger.Panic("Adding nil peer to eView")
		return
	}

	_, ok := d.eView[p.String()]
	if ok {
		d.logger.Panicf("eView already contains peer")
	}

	if len(d.eView) >= d.config.MaxPeersInEView { // eView is full
		toRemoveIdx := int(utils.GetRandInt(len(d.eView)))
		i := 0
		for _, p := range d.eView {
			if i == toRemoveIdx {
				delete(d.eView, p.String())
				break
			}
			i++
		}
	} else {
		d.eView[p.String()] = p
	}
}

func (d *DemmonTree) mergeSampleWithEview(
	sample []*PeerWithIDChain,
	sender *PeerWithIDChain,
	nrPeersToMerge, nrPeersToAdd int) (sampleToSend,
	neighboursWithoutSenderDescendants []*PeerWithIDChain) {

	selfInSample := false
	sampleAsMap := make(map[string]bool)

	for _, peerWithIDChain := range sample {
		sampleAsMap[peerWithIDChain.String()] = true
		if peer.PeersEqual(peerWithIDChain, d.babel.SelfPeer()) {
			selfInSample = true
		}
	}

	d.updateAndMergeSampleEntriesWithEView(sample, nrPeersToMerge)
	neighbors := d.getNeighborsAsPeerWithIDChainArray()
	neighboursWithoutSenderDescendants = getExcludingDescendantsOf(neighbors, sender.Chain())
	neighboursWithoutSenderDescendantsAndNotInSample := getPeersExcluding(
		neighboursWithoutSenderDescendants,
		sampleAsMap,
	)

	knownPeers := PeerWithIDChainMapToArr(d.eView)
	for _, v := range d.measuredPeers {
		if _, ok := d.eView[v.String()]; ok {
			continue
		}
		knownPeers = append(knownPeers, v.PeerWithIDChain)
	}

	exclusions := map[string]bool{}
	for _, aux := range neighboursWithoutSenderDescendantsAndNotInSample {
		exclusions[aux.String()] = true
	}
	for _, aux := range sample {
		exclusions[aux.String()] = true
	}

	knownPeersNotInNeighbors := getPeersExcluding(
		knownPeers,
		exclusions,
	)

	if !selfInSample && len(d.self.Chain()) > 0 && !d.self.IsDescendentOf(sender.Chain()) {
		sampleToSendMap := getRandSample(
			nrPeersToAdd-1,
			append(knownPeersNotInNeighbors, neighboursWithoutSenderDescendantsAndNotInSample...)...,
		)
		sampleToSend = append(PeerWithIDChainMapToArr(sampleToSendMap), append(sample, d.self)...)
	} else {
		sampleToSendMap := getRandSample(
			nrPeersToAdd,
			append(knownPeersNotInNeighbors, neighboursWithoutSenderDescendantsAndNotInSample...)...,
		)
		sampleToSend = append(PeerWithIDChainMapToArr(sampleToSendMap), sample...)
	}
	if len(sampleToSend) > d.config.NrPeersInWalkMessage {
		return sampleToSend[:d.config.NrPeersInWalkMessage], neighboursWithoutSenderDescendants
	}
	return sampleToSend, neighboursWithoutSenderDescendants
}

func (d *DemmonTree) mergeSiblingsWith(newSiblings []*PeerWithIDChain, parent peer.Peer) {
	// d.logger.Info("Merging siblings...")

	delete(d.mySiblings, d.myParent.String())

	for _, msgSibling := range newSiblings {
		if peer.PeersEqual(d.babel.SelfPeer(), msgSibling) {
			continue
		}

		if peer.PeersEqual(msgSibling, d.myPendingParentInAbsorb) {
			continue
		}

		if _, isChildren := d.myChildren[msgSibling.String()]; isChildren {
			continue
		}

		sibling, ok := d.mySiblings[msgSibling.String()]
		if !ok {
			d.addSibling(msgSibling)
			continue
		}
		if msgSibling.IsHigherVersionThan(sibling) {
			msgSibling.inConnActive = sibling.inConnActive
			msgSibling.outConnActive = sibling.outConnActive
			d.mySiblings[msgSibling.String()] = msgSibling
		}
	}

	for _, mySibling := range d.mySiblings {
		found := false
		for _, msgSibling := range newSiblings {
			if peer.PeersEqual(mySibling, msgSibling) {
				found = true
				break
			}
		}
		if !found {
			if peer.PeersEqual(mySibling, d.myParent) {
				continue
			}

			if _, isChildren := d.myChildren[mySibling.String()]; isChildren {
				delete(d.mySiblings, mySibling.String())
				continue
			}

			d.removeSibling(mySibling, false)
		}
	}
}

func (d *DemmonTree) updateAndMergeSampleEntriesWithEView(sample []*PeerWithIDChain, nrPeersToMerge int) {
	nrPeersMerged := 0

	for i := 0; i < len(sample); i++ {

		currPeer := sample[i]

		if peer.PeersEqual(currPeer, d.babel.SelfPeer()) {
			continue
		}

		if peer.PeersEqual(d.myParent, currPeer) {
			if currPeer.IsHigherVersionThan(d.myParent) {
				currPeer.inConnActive = d.myParent.inConnActive
				currPeer.outConnActive = d.myParent.outConnActive
				d.myParent = currPeer
			}

			continue
		}

		if sibling, ok := d.mySiblings[currPeer.String()]; ok {
			if currPeer.IsHigherVersionThan(sibling) {
				currPeer.inConnActive = sibling.inConnActive
				currPeer.outConnActive = sibling.outConnActive
				d.mySiblings[currPeer.String()] = currPeer
			}

			continue
		}

		if children, isChildren := d.myChildren[currPeer.String()]; isChildren {
			if currPeer.IsHigherVersionThan(children) {
				currPeer.inConnActive = children.inConnActive
				currPeer.outConnActive = children.outConnActive
				d.myChildren[currPeer.String()] = currPeer
			}
			continue
		}

		eViewPeer, ok := d.eView[currPeer.String()]
		if ok {
			if currPeer.IsHigherVersionThan(eViewPeer) {
				if currPeer.IsDescendentOf(d.self.Chain()) {
					d.removeFromMeasuredPeers(currPeer)
					d.removeFromEView(eViewPeer)
					continue
				}
				d.eView[currPeer.String()] = currPeer
			}
			continue
		}

		measuredPeer, ok := d.measuredPeers[currPeer.String()]
		if ok {
			if currPeer.IsHigherVersionThan(measuredPeer.PeerWithIDChain) {
				if currPeer.IsDescendentOf(d.self.Chain()) {
					delete(d.measuredPeers, measuredPeer.String())
					continue
				}
				d.measuredPeers[currPeer.String()] = NewMeasuredPeer(
					currPeer,
					measuredPeer.Latency,
				)
			}
			continue
		}

		if currPeer.IsDescendentOf(d.self.Chain()) {
			continue
		}

		if nrPeersMerged < nrPeersToMerge {
			d.addToEView(currPeer)
			nrPeersMerged++
		}
	}
}
