package protocol

import (
	"time"

	"github.com/nm-morais/demmon/internal/utils"
	"github.com/nm-morais/go-babel/pkg/errors"
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

func (d *DemmonTree) getPeerWithChainAsMeasuredPeer(p *PeerWithIDChain) (*MeasuredPeer, errors.Error) {
	info, err := d.nodeWatcher.GetNodeInfo(p)
	if err != nil {
		return nil, err
	}
	return NewMeasuredPeer(p, info.LatencyCalc().CurrValue()), nil
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
	myNewChain PeerIDChain,
	disconnectFromParent, sendDisconnectMsg bool) {

	haveCause := false
	var existingLatencyMeasurement *time.Duration

	// TODO add to measured peers
	if peer.PeersEqual(newParent, d.myPendingParentInRecovery) {
		d.myPendingParentInRecovery = nil
		haveCause = true
	}

	if peer.PeersEqual(newParent, d.myPendingParentInImprovement) {
		existingLatencyMeasurement = &d.myPendingParentInImprovement.MeasuredLatency
		d.myPendingParentInImprovement = nil
		haveCause = true
	}

	if peer.PeersEqual(newParent, d.myPendingParentInJoin) {
		existingLatencyMeasurement = &d.myPendingParentInJoin.MeasuredLatency
		d.joined = true
		d.joinMap = nil // TODO cleanup join function
		d.bestPeerlastLevel = nil
		d.myPendingParentInJoin = nil
		haveCause = true
	}

	if peer.PeersEqual(newParent, d.myPendingParentInAbsorb) {
		d.myPendingParentInAbsorb = nil
		haveCause = true
	}

	if !haveCause {
		d.logger.Panicf("adding parent but peer is not in possible pending parents")
	}

	if d.myParent != nil && !peer.PeersEqual(d.myParent, newParent) {
		if disconnectFromParent {
			tmp := d.myParent
			d.myParent = nil
			if sendDisconnectMsg {
				toSend := NewDisconnectAsChildMessage()

				d.babel.SendNotification(NewNodeDownNotification(tmp, d.getInView()))
				d.sendMessageAndDisconnect(toSend, tmp)
			} else {
				d.babel.Disconnect(d.ID(), tmp)
			}
			d.nodeWatcher.Unwatch(tmp, d.ID())
		}
	}

	d.logger.Infof(
		"My level changed: (%d -> %d)",
		d.self.Chain().Level(),
		newParent.Chain().Level()+1,
	) // IMPORTANT FOR VISUALIZER
	d.logger.Infof("My chain changed: (%+v -> %+v)", d.self.Chain(), myNewChain)
	d.logger.Infof("My parent changed: (%+v -> %+v)", d.myParent, newParent)

	if !myNewChain.Equal(d.self.chain) {
		d.babel.SendNotification(NewIDChangeNotification(myNewChain))
	}

	d.myGrandParent = newGrandParent
	d.myParent = newParent
	d.self = NewPeerWithIDChain(myNewChain, d.self.Peer, d.self.nChildren, d.self.Version()+1, d.self.Coordinates)

	if existingLatencyMeasurement != nil {
		d.nodeWatcher.WatchWithInitialLatencyValue(newParent, d.ID(), *existingLatencyMeasurement)
	} else {
		d.nodeWatcher.Watch(newParent, d.ID())
	}
	d.babel.Dial(d.ID(), newParent, newParent.ToTCPAddr())
	d.removeFromMeasuredPeers(newParent)
	// d.resetSwitchTimer()

	for childStr, child := range d.myChildren {
		childID := child.Chain()[len(child.Chain())-1]
		newChildrenPtr := NewPeerWithIDChain(
			append(myNewChain, childID),
			child.Peer,
			child.nChildren,
			child.version,
			child.Coordinates,
		)
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

func (d *DemmonTree) addChild(newChild *PeerWithIDChain, childrenLatency time.Duration) PeerID {
	proposedID := d.generateChildID()
	newChildWithID := NewPeerWithIDChain(
		append(d.self.Chain(), proposedID),
		newChild.Peer,
		newChild.nChildren,
		newChild.Version(),
		newChild.Coordinates,
	)

	if childrenLatency != 0 {
		d.nodeWatcher.WatchWithInitialLatencyValue(newChild, d.ID(), childrenLatency)
	} else {
		d.nodeWatcher.Watch(newChild, d.ID())
	}

	d.myChildren[newChild.String()] = newChildWithID
	d.babel.Dial(d.ID(), newChild, newChild.ToTCPAddr())
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
	)
}

func (d *DemmonTree) removeChild(toRemove peer.Peer) {
	d.logger.Infof("removing child: %s", toRemove.String())

	child, ok := d.myChildren[toRemove.String()]
	if ok {
		delete(d.myChildrenLatencies, toRemove.String())
		delete(d.myChildren, toRemove.String())

		d.self = NewPeerWithIDChain(
			d.self.Chain(),
			d.self.Peer,
			d.self.nChildren-1,
			d.self.Version()+1,
			d.self.Coordinates,
		)

		d.babel.SendNotification(NewNodeDownNotification(child, d.getInView()))
		d.babel.Disconnect(d.ID(), toRemove)
		d.nodeWatcher.Unwatch(toRemove, d.ID())

	} else {
		d.logger.Panic("Removing child not in myChildren or myPendingChildren")
	}
}

func (d *DemmonTree) addSibling(newSibling *PeerWithIDChain) {
	d.logger.Infof("Adding sibling: %s", newSibling.String())
	oldSibling, ok := d.mySiblings[newSibling.String()]

	if !ok {
		d.nodeWatcher.Watch(newSibling.Peer, d.ID())
		d.babel.Dial(d.ID(), newSibling.Peer, newSibling.Peer.ToTCPAddr())
		d.removeFromMeasuredPeers(newSibling)
	}

	d.mySiblings[newSibling.String()] = newSibling
	if ok {
		d.mySiblings[newSibling.String()].inConnActive = oldSibling.inConnActive
		d.mySiblings[newSibling.String()].outConnActive = oldSibling.outConnActive
	}
}

func (d *DemmonTree) removeSibling(toRemove peer.Peer) {
	d.logger.Infof("Removing sibling: %s", toRemove.String())
	if sibling, ok := d.mySiblings[toRemove.String()]; ok {
		delete(d.mySiblings, toRemove.String())
		d.nodeWatcher.Unwatch(sibling, d.ID())
		d.babel.SendNotification(NewNodeDownNotification(sibling, d.getInView()))
		d.babel.Disconnect(d.ID(), toRemove)
		return
	}
	d.logger.Panic("Removing sibling not in mySiblings or myPendingSiblings")
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
	childArr := make([]*PeerWithIDChain, 0)
	for _, child := range d.myChildren {
		childArr = append(childArr, child)
	}

	siblingArr := make([]*PeerWithIDChain, 0)
	for _, sibling := range d.mySiblings {
		siblingArr = append(siblingArr, sibling)
	}

	return InView{
		Parent:      d.myParent,
		Grandparent: d.myGrandParent,
		Children:    childArr,
		Siblings:    siblingArr,
	}
}

func (d *DemmonTree) removeFromMeasuredPeers(p peer.Peer) {
	delete(d.measuredPeers, p.String())
}

func (d *DemmonTree) addToMeasuredPeers(p *MeasuredPeer) { // TODO this is shit
	_, alreadyMeasured := d.measuredPeers[p.String()]
	if alreadyMeasured || len(d.measuredPeers) < d.config.MaxMeasuredPeers {
		d.measuredPeers[p.String()] = p
		return
	}
	for measuredPeerID, measuredPeer := range d.measuredPeers {
		if measuredPeer.MeasuredLatency > p.MeasuredLatency {
			delete(d.measuredPeers, measuredPeerID)
			d.measuredPeers[p.String()] = p
			return
		}
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

	for _, peerWithIDChain := range sample {
		if peer.PeersEqual(peerWithIDChain, d.babel.SelfPeer()) {
			selfInSample = true
			break
		}
	}
	d.updateAndMergeSampleEntriesWithEView(sample, nrPeersToMerge)
	neighbors := d.getNeighborsAsPeerWithIDChainArray()

	neighboursWithoutSenderDescendants = getExcludingDescendantsOf(neighbors, sender.Chain())
	sampleAsMap := make(map[string]interface{})
	for _, p := range sample {
		sampleAsMap[p.String()] = nil
	}
	neighboursWithoutSenderDescendantsAndNotInSample := getPeersExcluding(
		neighboursWithoutSenderDescendants,
		sampleAsMap,
	)
	if !selfInSample && d.self != nil && len(d.self.Chain()) > 0 && !d.self.IsDescendentOf(sender.Chain()) {
		sampleToSendMap := getRandSample(
			nrPeersToAdd-1,
			append(neighboursWithoutSenderDescendantsAndNotInSample, peerMapToArr(d.eView)...)...,
		)
		sampleToSend = append(peerMapToArr(sampleToSendMap), append(sample, d.self)...)
	} else {
		sampleToSendMap := getRandSample(
			nrPeersToAdd,
			append(neighboursWithoutSenderDescendantsAndNotInSample, peerMapToArr(d.eView)...)...,
		)
		sampleToSend = append(peerMapToArr(sampleToSendMap), sample...)
	}
	if len(sampleToSend) > d.config.NrPeersInWalkMessage {
		return sampleToSend[:d.config.NrPeersInWalkMessage], neighboursWithoutSenderDescendants
	}
	return sampleToSend, neighboursWithoutSenderDescendants
}

func (d *DemmonTree) mergeSiblingsWith(newSiblings []*PeerWithIDChain) {
	for _, msgSibling := range newSiblings {
		if peer.PeersEqual(d.babel.SelfPeer(), msgSibling) {
			continue
		}
		sibling, ok := d.mySiblings[msgSibling.String()]
		if !ok {
			d.addSibling(msgSibling)
			continue
		}
		if msgSibling.version > sibling.Version() {
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
			if peer.PeersEqual(mySibling, d.myPendingParentInAbsorb) {
				continue
			}

			if peer.PeersEqual(mySibling, d.myParent) {
				continue
			}

			if _, isChildren := d.myChildren[mySibling.String()]; isChildren {
				continue
			}

			d.removeSibling(mySibling)
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

		if d.isNeighbour(currPeer) {
			continue
		}

		if peer.PeersEqual(d.myParent, currPeer) {
			if currPeer.IsHigherVersionThan(d.myParent) {
				d.myParent = currPeer
			}

			continue
		}

		if sibling, ok := d.mySiblings[currPeer.String()]; ok {
			if currPeer.IsHigherVersionThan(sibling) {
				d.mySiblings[currPeer.String()] = currPeer
			}

			continue
		}

		if children, isChildren := d.myChildren[currPeer.String()]; isChildren {
			if currPeer.IsHigherVersionThan(children) {
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
					measuredPeer.PeerWithIDChain,
					measuredPeer.MeasuredLatency,
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