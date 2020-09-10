package membership

import (
	"encoding/binary"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const protoID = 1000
const protoName = "DemonTree"

type DemmonTreeConfig = struct {
	MaxTimeToProgressToNextLevel        time.Duration
	MaxRetriesJoinMsg                   int
	ParentRefreshTickDuration           time.Duration
	ChildrenRefreshTickDuration         time.Duration
	MinLatencyImprovementToBecomeParent time.Duration
	BootstrapRetryTimeout               time.Duration
	Landmarks                           []PeerWithId
	MinGrpSize                          int
	MaxGrpSize                          int
	LimitFirstLevelGroupSize            bool
	RejoinTimerDuration                 time.Duration
	CheckChildenSizeTimerDuration       time.Duration
}

type DemmonTree struct {
	logger *logrus.Logger
	config DemmonTreeConfig

	// node state
	myIDChain     PeerIDChain
	myLevel       uint16
	myGrandParent peer.Peer
	myParent      peer.Peer
	myChildren    map[string]PeerWithId
	mySiblings    map[string]PeerWithId

	// join state
	myPendingParent   peer.Peer
	myPendingChildren map[string]PeerWithId
	joinLevel         uint16

	parentLatencies map[string]time.Duration

	children           map[string]map[string]peer.Peer
	parents            map[string]peer.Peer
	currLevelPeersDone []map[string]peer.Peer
	currLevelPeers     []map[string]peer.Peer

	retries map[string]int
}

func NewDemmonTree(config DemmonTreeConfig) protocol.Protocol {
	return &DemmonTree{
		parents:            make(map[string]peer.Peer),
		children:           make(map[string]map[string]peer.Peer),
		myPendingChildren:  make(map[string]PeerWithId),
		currLevelPeers:     []map[string]peer.Peer{},
		currLevelPeersDone: []map[string]peer.Peer{},
		joinLevel:          1,
		retries:            map[string]int{},
		myChildren:         map[string]PeerWithId{},
		myParent:           nil,
		parentLatencies:    make(map[string]time.Duration),
		logger:             logs.NewLogger(protoName),
		config:             config,
		mySiblings:         map[string]PeerWithId{},
	}
}

func (d *DemmonTree) ID() protocol.ID {
	return protoID
}

func (d *DemmonTree) Name() string {
	return protoName
}

func (d *DemmonTree) Logger() *logrus.Logger {
	return d.logger
}

func (d *DemmonTree) Start() {
	for _, landmark := range d.config.Landmarks {
		if pkg.SelfPeer().Equals(landmark.Peer()) {
			d.myIDChain = PeerIDChain{landmark.ID()}
			d.logger.Infof("I am landmark, my ID is: %+v", d.myIDChain)
			d.myLevel = 0
			d.myParent = nil
			d.myChildren = map[string]PeerWithId{}
			pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
			return
		}
	}

	pkg.RegisterTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
	pkg.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))

	d.joinOverlay()
}

func (d *DemmonTree) Init() {
	pkg.RegisterMessageHandler(d.ID(), joinMessage{}, d.handleJoinMessage)
	pkg.RegisterMessageHandler(d.ID(), joinReplyMessage{}, d.handleJoinReplyMessage)
	pkg.RegisterMessageHandler(d.ID(), joinAsChildMessage{}, d.handleJoinAsChildMessage)
	pkg.RegisterMessageHandler(d.ID(), joinAsChildMessageReply{}, d.handleJoinAsChildMessageReply)
	pkg.RegisterMessageHandler(d.ID(), joinAsParentMessage{}, d.handleJoinAsParentMessage)
	pkg.RegisterMessageHandler(d.ID(), updateParentMessage{}, d.handleUpdateParentMessage)
	pkg.RegisterMessageHandler(d.ID(), updateChildMessage{}, d.handleUpdateChildMessage)
	pkg.RegisterMessageHandler(d.ID(), absorbMessage{}, d.handleAbsorbMessage)
	pkg.RegisterMessageHandler(d.ID(), disconnectAsChildMessage{}, d.handleDisconnectAsChildMsg)

	pkg.RegisterTimerHandler(d.ID(), joinTimerID, d.handleJoinTimer)
	pkg.RegisterTimerHandler(d.ID(), parentRefreshTimerID, d.handleRefreshParentTimer)
	pkg.RegisterTimerHandler(d.ID(), updateChildTimerID, d.handleUpdateChildTimer)
	pkg.RegisterTimerHandler(d.ID(), checkChidrenSizeTimerID, d.handleCheckChildrenSizeTimer)
}

// timer handlers

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {
	if d.joinLevel == 1 && len(d.currLevelPeers[d.joinLevel-1]) == 0 {
		d.logger.Info("-------------Rejoining overlay---------------")
		d.joinOverlay()
	}
	pkg.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
}

func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewJoinTimer(d.config.CheckChildenSizeTimerDuration))
	if len(d.myChildren) == 0 {
		return
	}

	if d.myLevel == 0 && !d.config.LimitFirstLevelGroupSize {
		return
	}

	if len(d.myChildren) > d.config.MaxGrpSize {
		var maxLatPeer peer.Peer
		maxLat := time.Duration(0)
		for _, child := range d.myChildren {
			info, err := pkg.GetNodeWatcher().GetNodeInfo(child.Peer())
			if err != nil {
				panic(err.Reason())
			}
			latTo := info.LatencyCalc.CurrValue()
			if latTo > maxLat && int(child.NrChildren()) < d.config.MaxGrpSize {
				maxLat = latTo
				maxLatPeer = child.Peer()
			}
		}
		d.sendMessage(NewAbsorbMessage(), maxLatPeer)
	}
}

func (d *DemmonTree) handleRefreshParentTimer(timer timer.Timer) {
	d.logger.Info("RefreshParentTimer proc")
	for _, child := range d.myChildren {
		toSend := NewUpdateParentMessage(d.myGrandParent, d.myLevel, append(d.myIDChain, child.ID()))
		d.sendMessage(toSend, child.Peer())
	}
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
}

func (d *DemmonTree) handleUpdateChildTimer(timer timer.Timer) {
	d.logger.Info("UpdateChildTimer proc")
	if d.myParent != nil {
		toSend := NewUpdateChildMessage(len(d.myChildren))
		d.sendMessage(toSend, d.myParent)
	}
	pkg.RegisterTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
}

// message handlers

func (d *DemmonTree) handleAbsorbMessage(sender peer.Peer, m message.Message) {
	d.logger.Infof("Got evictMessage: %+v from %s", m, sender.ToString())

	var minLatPeer peer.Peer
	minLat := time.Duration(math.MaxInt64)
	for _, sibling := range d.mySiblings {
		info, err := pkg.GetNodeWatcher().GetNodeInfo(sibling.Peer())
		if err != nil {
			panic(err.Reason())
		}
		latTo := info.LatencyCalc.CurrValue()
		if latTo > minLat {
			minLat = latTo
			minLatPeer = sibling.Peer()
		}
	}

	if minLatPeer == nil {
		panic("Have no siblings but got evict message")
	}

	d.sendJoinAsParentMsg(minLatPeer)
}

func (d *DemmonTree) handleDisconnectAsChildMsg(sender peer.Peer, m message.Message) {
	delete(d.myChildren, sender.ToString())
	pkg.Disconnect(d.ID(), sender)
	pkg.GetNodeWatcher().Unwatch(sender, d.ID())
}

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {

	aux := make([]peer.Peer, len(d.myChildren))
	i := 0
	for _, c := range d.myChildren {
		aux[i] = c.Peer()
		i++
	}

	parentLatency := time.Duration(math.MaxInt64)
	if d.myParent != nil {
		info, err := pkg.GetNodeWatcher().GetNodeInfo(d.myParent)
		if err != nil {
			panic(err.Reason())
		}
		parentLatency = info.LatencyCalc.CurrValue()
	}

	toSend := NewJoinReplyMessage(aux, d.myLevel, parentLatency, d.myParent)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(joinReplyMessage)

	d.logger.Infof("Got joinReply: %+v from %s", replyMsg, sender.ToString())

	if (d.joinLevel-1 != replyMsg.Level) || (d.joinLevel > 1 && !d.parents[sender.ToString()].Equals(replyMsg.Parent)) {
		if d.joinLevel-1 != replyMsg.Level {
			d.logger.Warnf("Discarding joinReply %+v from %s because joinLevel is not mine: %d", replyMsg, sender.ToString(), d.joinLevel)
		}
		if d.joinLevel > 1 && !d.parents[sender.ToString()].Equals(replyMsg.Parent) {
			d.logger.Warnf("Discarding joinReply %+v from %s because node does not have the same parent... should be: %s", replyMsg, sender.ToString(), d.parents[sender.ToString()].ToString())
		}
		// discard old repeated messages
		delete(d.currLevelPeers[d.joinLevel-1], sender.ToString())
		delete(d.currLevelPeersDone[d.joinLevel-1], sender.ToString())

		if len(d.currLevelPeers[d.joinLevel-1]) == 0 {
			d.logger.Warn("Have no more peers in current level, falling back to parent")
			d.fallbackToParent(sender)
			return
		}

		if d.canProgressToNextStep() {
			d.progressToNextStep()
		}
		return
	}

	d.currLevelPeersDone[d.joinLevel-1][sender.ToString()] = sender
	d.children[sender.ToString()] = make(map[string]peer.Peer, len(replyMsg.Children))
	for _, c := range replyMsg.Children {
		d.children[sender.ToString()][c.ToString()] = c
	}

	d.parentLatencies[sender.ToString()] = time.Duration(replyMsg.ParentLatency)
	for _, children := range replyMsg.Children {
		d.parents[children.ToString()] = sender
	}

	if d.canProgressToNextStep() {
		d.progressToNextStep()
	}
}

func (d *DemmonTree) handleJoinAsParentMessage(sender peer.Peer, m message.Message) {
	japMsg := m.(joinAsParentMessage)
	d.logger.Infof("Peer %s wants to be my parent", sender.ToString())
	if !japMsg.ExpectedParent.Equals(d.myParent) {
		d.logger.Warnf("Discarding parent %s because it was trying to optimize with previous parent", sender.ToString())
		return
	}
	if d.myParent != nil {
		toSend := NewDisconnectAsChildMessage()
		d.sendMessage(toSend, d.myParent)
		d.myParent = nil
	}

	d.myPendingParent = sender
	if d.myLevel != japMsg.Level {
		d.logger.Warnf("My level changed: (%d -> %d)", d.myLevel, japMsg.Level) // IMPORTANT FOR VISUALIZER
	}

	if !ChainsEqual(japMsg.ProposedId, d.myIDChain) {
		d.logger.Warnf("My chain changed: (%+v -> %+v)", d.myIDChain, japMsg.ProposedId)
	}

	d.myIDChain = japMsg.ProposedId
	d.myLevel = japMsg.Level
	pkg.GetNodeWatcher().WatchWithInitialLatencyValue(sender, d.ID(), japMsg.MeasuredLatency)
	pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {
	d.logger.Infof("Peer %s wants to be my children", sender.ToString())
	jacMsg := m.(joinAsChildMessage)
	// TODO Group logic
	for _, child := range jacMsg.Children {
		childStr := child.ToString()
		d.logger.Infof("Removing child: %s as %s is taking it", childStr, sender.ToString())
		if _, ok := d.myChildren[childStr]; ok {
			pkg.Disconnect(d.ID(), child)
			delete(d.myChildren, childStr)
		}
	}

	childrenId := d.generateChildId()
	toSend := NewJoinAsChildMessageReply(true, append(d.myIDChain, childrenId))
	d.sendMessageTmpTCPChan(toSend, sender)
	d.myPendingChildren[sender.ToString()] = NewPeerWithId(childrenId, sender, len(jacMsg.Children))
	pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
	pkg.GetNodeWatcher().WatchWithInitialLatencyValue(sender, d.ID(), jacMsg.MeasuredLatency)
}

func (d *DemmonTree) handleJoinAsChildMessageReply(sender peer.Peer, m message.Message) {
	japrMsg := m.(joinAsChildMessageReply)
	if japrMsg.Accepted {
		d.myPendingParent = sender
		d.logger.Warnf("My chain changed: (%+v -> %+v)", d.myIDChain, japrMsg.ProposedId)
		d.myIDChain = japrMsg.ProposedId
		pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
	} else {
		d.myPendingParent = sender
		d.fallbackToParent(sender)
	}
}

func (d *DemmonTree) handleUpdateParentMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(updateParentMessage)
	d.logger.Infof("got UpdateParentMessage %+v", m)

	if d.myParent == nil || !sender.Equals(d.myParent) {
		if d.myPendingParent == nil || !sender.Equals(d.myPendingParent) {
			d.logger.Panicf("Received UpdateParentMessage from not my parent (parent:%s sender:%s)", d.myParent.ToString(), sender.ToString())
			return
		}
	}

	if d.myLevel != upMsg.ParentLevel+1 {
		d.logger.Warnf("My level changed: (%d -> %d)", d.myLevel, upMsg.ParentLevel+1) // IMPORTANT FOR VISUALIZER
	}

	if upMsg.GrandParent != nil {
		if d.myGrandParent == nil || !d.myGrandParent.Equals(upMsg.GrandParent) {
			d.logger.Warnf("My grandparent changed : (%+v -> %+v)", d.myGrandParent, upMsg.GrandParent)
		}
	}

	if !ChainsEqual(upMsg.ProposedIdChain, d.myIDChain) {
		d.logger.Warnf("My chain changed: (%+v -> %+v)", d.myIDChain, upMsg.ProposedIdChain)
	}

	d.myIDChain = upMsg.ProposedIdChain
	d.myLevel = upMsg.ParentLevel + 1
	d.myGrandParent = upMsg.GrandParent
}

func (d *DemmonTree) handleUpdateChildMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(updateChildMessage)
	d.logger.Infof("got updateChildMessage %+v", m)
	child, ok := d.myChildren[sender.ToString()]
	if !ok {
		child, ok := d.myPendingChildren[sender.ToString()]
		if !ok {
			d.logger.Panicf("got updateChildMessage %+v from not my children, or my pending children: %s", m, sender.ToString())
		}
		child.SetChildrenNr(upMsg.NChildren)
		return
	}
	child.SetChildrenNr(upMsg.NChildren)
}

func (d *DemmonTree) InConnRequested(peer peer.Peer) bool {

	if d.myParent != nil && d.myParent.Equals(peer) || d.myPendingParent != nil && d.myPendingParent.Equals(peer) {
		d.logger.Infof("My parent dialed me")
		return true
	}

	_, isPendingChildren := d.myPendingChildren[peer.ToString()]
	_, isChildren := d.myChildren[peer.ToString()]
	if isChildren || isPendingChildren {
		d.logger.Infof("My pending children (%s) dialed me ", peer.ToString())
		return true
	}

	d.logger.Warnf("Conn requested by unkown peer: %s", peer.ToString())
	return true
}

func (d *DemmonTree) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {

	if sourceProto != d.ID() {
		return false
	}

	d.logger.Infof("Dialed peer with success: %s", peer.ToString())
	if d.myPendingParent != nil && d.myPendingParent.Equals(peer) {
		d.logger.Infof("Dialed parent with success, parent: %s", d.myPendingParent.ToString())
		d.myParent = d.myPendingParent
		d.myPendingParent = nil
		return true
	}

	child, isChildren := d.myPendingChildren[peer.ToString()]
	if isChildren {
		d.logger.Infof("Dialed children with success: %s", peer.ToString())
		delete(d.myPendingChildren, peer.ToString())
		d.myChildren[peer.ToString()] = child
		return true
	}

	d.logger.Infof("d.myParent: %s", d.myParent)
	d.logger.Infof("d.myChildren: %+v", d.myChildren)
	d.logger.Panicf("Dialed unknown peer: %s", peer.ToString())
	return false
}

func (d *DemmonTree) DialFailed(peer peer.Peer) {
	d.logger.Errorf("Failed to dial %s", peer.ToString())
	// TODO
}

func (d *DemmonTree) OutConnDown(peer peer.Peer) {
	// TODO
	d.logger.Warnf("Peer down %s", peer.ToString())
	_, isChildren := d.myChildren[peer.ToString()]
	if isChildren {
		delete(d.myChildren, peer.ToString())
		d.logger.Warnf("Deleted %s from my children", peer.ToString())
	}

}

func (d *DemmonTree) MessageDelivered(message message.Message, peer peer.Peer) {
	d.logger.Infof("Message %+v delivered to: %s", message, peer.ToString())

	switch message.(type) {
	case disconnectAsChildMessage:
		pkg.Disconnect(d.ID(), peer)
	}

}

func (d *DemmonTree) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	d.logger.Infof("Message %+v failed to deliver to: %s because: %s", message, peer.ToString(), error.Reason())
	switch message := message.(type) {
	case joinMessage:
		_, ok := d.retries[peer.ToString()]
		if !ok {
			d.retries[peer.ToString()] = 0
		}

		d.retries[peer.ToString()]++
		if d.retries[peer.ToString()] >= d.config.MaxRetriesJoinMsg {
			d.logger.Warnf("Deleting peer %s from currLevelPeers because it exceeded max retries (%d)", peer.ToString(), d.config.MaxRetriesJoinMsg)
			delete(d.currLevelPeers[d.joinLevel-1], peer.ToString())
			delete(d.retries, peer.ToString())

			if d.canProgressToNextStep() {
				d.progressToNextStep()
			}
			return
		}
		d.sendMessageAndMeasureLatency(message, peer)

	case joinAsChildMessage:
		d.fallbackToParent(peer)

	case disconnectAsChildMessage:
		pkg.Disconnect(d.ID(), peer)
	}
}

func (d *DemmonTree) joinOverlay() {
	nrLandmarks := len(d.config.Landmarks)
	d.currLevelPeersDone = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)} // start level 1
	d.currLevelPeers = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)}     // start level 1
	d.joinLevel = 1
	d.logger.Infof("Landmarks:")
	for i, landmark := range d.config.Landmarks {
		d.logger.Infof("%d :%s", i, landmark.Peer().ToString())
		d.currLevelPeers[0][landmark.Peer().ToString()] = landmark.Peer()
		joinMsg := joinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark.Peer())
	}
}

func (d *DemmonTree) attemptJoinAsChildInLevel(lowestLatencyPeer peer.Peer, lowestLatentyPeerLat time.Duration, lowestLatencyPeerParent peer.Peer, lowestLatencyPeerParentLat time.Duration) bool {
	if len(d.children[lowestLatencyPeer.ToString()]) < d.config.MaxGrpSize { // can join group without more checks
		d.logger.Infof("Latency to (%s:%d) is higher than the latency to the parent (%s:%d nanoSec)",
			lowestLatencyPeer.ToString(), lowestLatentyPeerLat, lowestLatencyPeerParent.ToString(), lowestLatencyPeerParentLat)
		d.logger.Infof("Joining level %d", d.joinLevel-1)
		d.sendJoinAsChildMsg(lowestLatencyPeerParent, lowestLatencyPeerParentLat, nil)
		d.unwatchPeersInLevel(d.joinLevel, []peer.Peer{lowestLatencyPeerParent})
		return true
	} else { // group size is too big
		// can still join if we are joining level 1 and LimitFirstLevelGroupSize == False
		if d.joinLevel == 2 && !d.config.LimitFirstLevelGroupSize { // if level is 3, 4 or more, always care about group size
			d.logger.Infof("Latency to (%s:%d) is higher than the latency to the parent (%s:%d nanoSec)",
				lowestLatencyPeer.ToString(), lowestLatentyPeerLat, lowestLatencyPeerParent.ToString(), lowestLatencyPeerParentLat)
			d.logger.Infof("Joining level %d", d.joinLevel-1)
			d.sendJoinAsChildMsg(lowestLatencyPeerParent, lowestLatencyPeerParentLat, nil)
			d.unwatchPeersInLevel(d.joinLevel, []peer.Peer{lowestLatencyPeerParent})
			return true
		}
	}
	return false
}

func (d *DemmonTree) joinAsParentInLevel(currLevelPeersDone map[string]peer.Peer, lowestLatencyPeer peer.Peer, lowestLatencyPeerLat time.Duration, lowestLatencyPeerParent peer.Peer) {

	possibleChildren := make([]peer.Peer, 0, len(currLevelPeersDone))

	// find which peers to "steal"
	for peerID, p := range currLevelPeersDone {
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(p)
		if err != nil {
			d.logger.Panicf("Node %s parent was not watched", lowestLatencyPeer.ToString())
		}
		currLat := nodeStats.LatencyCalc.CurrValue()
		if d.parentLatencies[peerID]-currLat > d.config.MinLatencyImprovementToBecomeParent {
			d.logger.Infof("Can become parent of: %s (Parent latency %d: , my latency to it: %d)",
				peerID,
				d.parentLatencies[peerID],
				currLat)
			possibleChildren = append(possibleChildren, p)
		}
	}

	// currPeersInGroup  (including myself) - childrenToSteal must be higher than the min number of elements in group
	nrRemainingPeersInGrp := 1 + len(currLevelPeersDone) - len(possibleChildren)
	if nrRemainingPeersInGrp < d.config.MinGrpSize {
		sort.SliceStable(possibleChildren, func(i, j int) bool {
			nodeStats1, err := pkg.GetNodeWatcher().GetNodeInfo(possibleChildren[i])
			if err != nil {
				d.logger.Panicf("Node %s was not watched", possibleChildren[i].ToString())
			}
			nodeStats2, err := pkg.GetNodeWatcher().GetNodeInfo(possibleChildren[j])
			if err != nil {
				d.logger.Panicf("Node %s was not watched", possibleChildren[j].ToString())
			}
			return nodeStats1.LatencyCalc.CurrValue() < nodeStats2.LatencyCalc.CurrValue()
		})
		overflow := d.config.MinGrpSize - nrRemainingPeersInGrp
		if overflow < 0 {
			d.logger.Infof("nrRemainingPeersInGrp: %d", nrRemainingPeersInGrp)
			d.logger.Infof("len(currLevelPeersDone): %d", len(currLevelPeersDone))
			d.logger.Infof("len(possibleChildren): %d", len(possibleChildren))
			panic("overflow is negative...")
		}
		d.logger.Infof("Overflow: %d", overflow)
		possibleChildren = possibleChildren[:len(possibleChildren)-overflow]
	}

	d.sendJoinAsChildMsg(lowestLatencyPeerParent, lowestLatencyPeerLat, possibleChildren)
	d.logger.Infof("Joining level %d with %s as parent and as parent of %+v", d.joinLevel, lowestLatencyPeerParent.ToString(), possibleChildren)
	for _, possibleChild := range possibleChildren {
		d.sendJoinAsParentMsg(possibleChild)
	}
	d.unwatchPeersInLevel(d.joinLevel-1, append(possibleChildren, lowestLatencyPeerParent))
}

func (d *DemmonTree) sendJoinAsParentMsg(newChildren peer.Peer) {
	childStats, err := pkg.GetNodeWatcher().GetNodeInfo(newChildren)
	if err != nil {
		panic(err.Reason())
	}
	proposedId := d.generateChildId()
	toSend := NewJoinAsParentMessage(d.parents[newChildren.ToString()], append(d.myIDChain, proposedId), d.joinLevel, childStats.LatencyCalc.CurrValue())
	d.myPendingChildren[newChildren.ToString()] = NewPeerWithId(proposedId, newChildren, len(d.children[newChildren.ToString()]))
	d.sendMessageTmpTCPChan(toSend, newChildren)
	pkg.Dial(newChildren, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) sendJoinAsChildMsg(newParent peer.Peer, newParentLat time.Duration, children []peer.Peer) {
	d.logger.Infof("Pending parent: %s", newParent.ToString())
	d.logger.Infof("Joining level %d", d.joinLevel)
	toSend := NewJoinAsChildMessage(children, newParentLat)
	d.sendMessageTmpTCPChan(toSend, newParent)
}

func (d *DemmonTree) unwatchPeersInLevel(level uint16, exclusions []peer.Peer) {
	for _, currPeer := range d.currLevelPeers[level-1] {
		found := false
		for _, exclusion := range exclusions {
			if exclusion.Equals(currPeer) {
				found = true
				break
			}
		}
		if !found {
			pkg.GetNodeWatcher().Unwatch(currPeer, d.ID())
		}
	}
}

func (d *DemmonTree) canProgressToNextStep() (res bool) {

	if len(d.currLevelPeers) == 0 {
		d.logger.Info("Cannot progress because len(d.currLevelPeers) == 0")
		res = false
		return
	}

	if len(d.currLevelPeers[d.joinLevel-1]) == 0 {
		d.logger.Infof("Cannot progress because len(d.currLevelPeers[d.joinLevel-1]) == 0 ")
		res = false
		return
	}

	for _, p := range d.currLevelPeers[d.joinLevel-1] {
		_, ok := d.currLevelPeersDone[d.joinLevel-1][p.ToString()]
		if !ok {
			d.logger.Infof("Cannot progress because not all peers are done, missing: %s", p.ToString())
			res = false
			return
		}
	}
	d.logger.Infof("Can progress!")
	res = true
	return
}

func (d *DemmonTree) progressToNextStep() {

	d.logger.Infof("Getting lowest latency peer...")
	lowestLatencyPeer, lowestLatencyPeerLat, err := d.getLowestLatencyPeerInLvlWithDeadline(d.joinLevel-1, time.Now().Add(d.config.MaxTimeToProgressToNextLevel))
	if err != nil {
		d.logger.Panicf(err.Reason())
	}
	d.logger.Infof("Lowest Latency Peer: %s , Latency: %d", lowestLatencyPeer.ToString(), lowestLatencyPeerLat)

	currLevelPeers := d.currLevelPeers[d.joinLevel-1]
	currLevelPeersDone := d.currLevelPeers[d.joinLevel-1]

	toPrint := ""
	for _, peer := range currLevelPeers {
		toPrint = toPrint + "; " + peer.ToString()
	}
	d.logger.Infof("d.currLevelPeers (level %d): %s", d.joinLevel-1, toPrint)

	toPrint = ""
	for _, peerDone := range currLevelPeersDone {
		toPrint = toPrint + "; " + peerDone.ToString()
	}
	d.logger.Infof("d.currLevelPeersDone (level %d): %s:", d.joinLevel-1, toPrint)

	if d.joinLevel > 1 {
		lowestLatencyPeerParent := d.parents[lowestLatencyPeer.ToString()]
		lowestLatencyPeerParentStats, err := pkg.GetNodeWatcher().GetNodeInfo(lowestLatencyPeerParent)
		lowestLatencyPeerParentLat := lowestLatencyPeerParentStats.LatencyCalc.CurrValue()

		if err != nil {
			d.logger.Panicf("Node %s parent (%s) was not watched", lowestLatencyPeer.ToString(), lowestLatencyPeerParent.ToString())
		}

		d.logger.Infof("Latency to %s : %d", lowestLatencyPeerParent.ToString(), lowestLatencyPeerLat)
		// Checking to see if i can become parent of currLevelNodes
		if len(currLevelPeersDone) > d.config.MinGrpSize {
			if d.parentLatencies[lowestLatencyPeer.ToString()]-lowestLatencyPeerLat > d.config.MinLatencyImprovementToBecomeParent {
				d.joinAsParentInLevel(currLevelPeers, lowestLatencyPeer, lowestLatencyPeerLat, lowestLatencyPeerParent)
				return
			}
		}

		if lowestLatencyPeerParentLat < lowestLatencyPeerLat { // Checking to see if i either progress to lower levels or join curr level
			if d.attemptJoinAsChildInLevel(lowestLatencyPeer, lowestLatencyPeerLat, lowestLatencyPeerParent, lowestLatencyPeerParentLat) {
				return
			}
		}
	}

	d.unwatchPeersInLevel(d.joinLevel, []peer.Peer{lowestLatencyPeer})       // unwatch all nodes but the best node
	if len(d.children[lowestLatencyPeer.ToString()]) < d.config.MinGrpSize { // if the group has enough members, evaluate if can go down
		d.logger.Infof("Joining level %d because nodes in this level have not enough members", d.joinLevel)
		d.sendJoinAsChildMsg(lowestLatencyPeer, lowestLatencyPeerLat, nil)
		return
	}
	d.progressToNextLevel(lowestLatencyPeer)
}

func (d *DemmonTree) progressToNextLevel(lowestLatencyPeer peer.Peer) {
	d.logger.Infof("Progressing to next level (currLevel=%d), (nextLevel=%d) ", d.joinLevel, d.joinLevel+1)
	d.joinLevel++
	if int(d.joinLevel-1) < len(d.currLevelPeers) {
		d.currLevelPeers[d.joinLevel-1] = d.children[lowestLatencyPeer.ToString()]
	} else {
		d.currLevelPeers = append(d.currLevelPeers, d.children[lowestLatencyPeer.ToString()])
	}

	if int(d.joinLevel-1) < len(d.currLevelPeersDone) {
		d.currLevelPeersDone[d.joinLevel-1] = make(map[string]peer.Peer)
	} else {
		d.currLevelPeersDone = append(d.currLevelPeersDone, make(map[string]peer.Peer))
	}

	for _, p := range d.currLevelPeers[d.joinLevel-1] {
		d.sendMessageAndMeasureLatency(NewJoinMessage(), p)
	}
}

// aux functions
func (d *DemmonTree) sendMessageTmpTCPChan(toSend message.Message, destPeer peer.Peer) {
	pkg.SendMessageSideStream(toSend, destPeer, d.ID(), []protocol.ID{d.ID()}, stream.NewTCPDialer())
}

func (d *DemmonTree) sendMessageAndMeasureLatency(toSend message.Message, destPeer peer.Peer) {
	d.sendMessageTmpTCPChan(toSend, destPeer)
	pkg.GetNodeWatcher().Watch(destPeer, d.ID())
}

func (d *DemmonTree) sendMessage(toSend message.Message, destPeer peer.Peer) {
	d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.ToString())
	pkg.SendMessage(toSend, destPeer, d.ID(), []protocol.ID{d.ID()})
}

func (d *DemmonTree) getLowestLatencyPeerInLvlWithDeadline(level uint16, deadline time.Time) (peer.Peer, time.Duration, errors.Error) {
	var lowestLat = time.Duration(math.MaxInt64)
	var lowestLatPeer peer.Peer

	if len(d.currLevelPeersDone[level]) == 0 {
		return nil, time.Duration(math.MaxInt64), errors.NonFatalError(404, "peer collection is empty", protoName)
	}

	for _, currPeer := range d.currLevelPeersDone[level] {
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfoWithDeadline(currPeer, deadline)
		if err != nil {
			d.logger.Warnf("Do not have latency measurement for %s", currPeer.ToString())
			continue
		}
		currLat := nodeStats.LatencyCalc.CurrValue()
		if currLat < lowestLat {
			lowestLatPeer = currPeer
			lowestLat = currLat
		}
	}
	return lowestLatPeer, lowestLat, nil
}

func (d *DemmonTree) generateChildId() PeerID {
	var peerId PeerID
	occupiedIds := make(map[PeerID]bool, len(d.myChildren)+len(d.myPendingChildren))
	for _, child := range d.myChildren {
		occupiedIds[child.ID()] = true
	}

	for _, child := range d.myPendingChildren {
		occupiedIds[child.ID()] = true
	}

	maxId := int(math.Exp2(IdSegmentLen))
	for i := 0; i < maxId; i++ {
		binary.BigEndian.PutUint64(peerId[:], uint64(i))
		_, ok := occupiedIds[peerId]
		if !ok {
			d.logger.Infof("Generated peerID: %+v", peerId)
			return peerId
		}
	}
	panic("Could not generate children ID")
}

func (d *DemmonTree) fallbackToParent(node peer.Peer) {
	peerParent, ok := d.parents[node.ToString()]
	if !ok {
		d.logger.Panicf("Peer %s has no parent", node.ToString())
	}
	info, err := pkg.GetNodeWatcher().GetNodeInfo(peerParent)
	if err != nil {
		panic(err.Reason())
	}
	peerLat := info.LatencyCalc.CurrValue()
	d.sendJoinAsChildMsg(peerParent, peerLat, []peer.Peer{})
}
