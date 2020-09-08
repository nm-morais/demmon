package membership

import (
	"encoding/binary"
	"math"
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
	MaxTimeToReplyToJoinMsg         time.Duration
	MaxTimeToProgressToNextLevel    time.Duration
	MaxRetriesJoinMsg               int
	ParentRefreshTickDuration       time.Duration
	GParentLatencyIncreaseThreshold time.Duration
	NrSamplesForLatency             int
	BootstrapRetryTimeout           time.Duration
	Landmarks                       []PeerWithId
	MinMembersPerLevel              int
	MaxMembersPerLevel              int
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

	// join state
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
			return
		}
	}
	pkg.RegisterTimer(d.ID(), NewJoinTimer(10*time.Second))
	d.joinOverlay()
}

func (d *DemmonTree) Init() {
	pkg.RegisterMessageHandler(d.ID(), joinMessage{}, d.handleJoinMessage)
	pkg.RegisterMessageHandler(d.ID(), joinReplyMessage{}, d.handleJoinReplyMessage)
	pkg.RegisterMessageHandler(d.ID(), joinAsChildMessage{}, d.handleJoinAsChildMessage)
	pkg.RegisterMessageHandler(d.ID(), joinAsChildMessageReply{}, d.handleJoinAsChildMessageReply)
	pkg.RegisterMessageHandler(d.ID(), joinAsParentMessage{}, d.handleJoinAsParentMessage)
	pkg.RegisterMessageHandler(d.ID(), updateParentMessage{}, d.handleUpdateParentMessage)

	pkg.RegisterTimerHandler(d.ID(), joinTimerID, d.handleJoinTimer)
	pkg.RegisterTimerHandler(d.ID(), parentRefreshTimerID, d.handleRefreshParentTimer)
}

// timer handlers

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {
	if d.joinLevel == 1 && len(d.currLevelPeers[d.joinLevel-1]) == 0 {
		d.logger.Info("-------------Rejoining overlay---------------")
		d.joinOverlay()
	}
	pkg.RegisterTimer(d.ID(), NewJoinTimer(10*time.Second))
}

func (d *DemmonTree) handleRefreshParentTimer(timer timer.Timer) {
	refreshTimer := timer.(*parentRefreshTimer)
	child, ok := d.myChildren[refreshTimer.Child.ToString()]
	if !ok {
		d.logger.Warnf("Stopped sending refreshParent messages to: %s", refreshTimer.Child.ToString())
		return
	}
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(1*time.Second, refreshTimer.Child))
	toSend := NewUpdateParentMessage(d.myGrandParent, d.myLevel, append(d.myIDChain, child.ID()))
	d.sendMessage(toSend, refreshTimer.Child)
}

// message handlers

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
	toSend := NewJoinReplyMessage(aux, d.myLevel, parentLatency)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(joinReplyMessage)

	d.logger.Infof("Got joinReply: %+v from %s", replyMsg, sender.ToString())

	if d.joinLevel-1 != replyMsg.Level {
		d.logger.Warnf("Discarding message %+v from because joinLevel is not mine: %d", replyMsg, d.joinLevel)
		// discard old repeated messages
		delete(d.currLevelPeers[d.joinLevel-1], sender.ToString())
		delete(d.currLevelPeersDone[d.joinLevel-1], sender.ToString())

		if len(d.currLevelPeers[d.joinLevel-1]) == 0 {
			d.logger.Warn("Have no more peers in current level, falling back to parent")
			d.fallbackToParent(sender)
			return
		}

		if d.canProgressToNextLevel() {
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

	if d.canProgressToNextLevel() {
		d.progressToNextStep()
	}
}

func (d *DemmonTree) fallbackToParent(node peer.Peer) {
	peerParent, ok := d.parents[node.ToString()]
	if !ok {
		d.logger.Panicf("Peer %s has no parent", node.ToString())
	}
	d.sendJoinAsChildMsg(peerParent, []peer.Peer{})
}

func (d *DemmonTree) handleJoinAsParentMessage(sender peer.Peer, m message.Message) {
	japMsg := m.(joinAsParentMessage)
	d.logger.Infof("Peer %s wants to be my parent", sender.ToString())
	if !d.myParent.Equals(japMsg.ExpectedParent) {
		d.logger.Warnf("Discarding parent %s because it was trying to optimize with previous parent", sender.ToString())
		return
	}
	if d.myParent != nil {
		pkg.Disconnect(d.ID(), d.myParent)
	}

	d.myParent = sender
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
	d.myPendingChildren[sender.ToString()] = NewPeerWithId(childrenId, sender)
	pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) handleJoinAsChildMessageReply(sender peer.Peer, m message.Message) {
	japrMsg := m.(joinAsChildMessageReply)
	if japrMsg.Accepted {
		d.myParent = sender
		d.logger.Warnf("My chain changed: (%+v -> %+v)", d.myIDChain, japrMsg.ProposedId)
		d.myIDChain = japrMsg.ProposedId
		pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
	} else {
		d.myParent = nil
		d.fallbackToParent(sender)
	}
}

func (d *DemmonTree) handleUpdateParentMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(updateParentMessage)
	d.logger.Infof("got UpdateParentMessage%+v", m)
	if d.myLevel != upMsg.ParentLevel+1 {
		d.logger.Warnf("My level changed: (%d -> %d)", d.myLevel, upMsg.ParentLevel+1) // IMPORTANT FOR VISUALIZER
	}
	if upMsg.GrandParent != nil {
		if d.myGrandParent == nil || !d.myGrandParent.Equals(upMsg.GrandParent) {
			d.logger.Warnf("My grandparent changed : (%+v -> %+v)", d.myGrandParent, upMsg.GrandParent)
		}
	}

	if d.myParent == nil || !d.myParent.Equals(sender) {
		d.logger.Warnf("Received UpdateParentMessage from not my parent (parent:%s sender:%s)", d.myParent.ToString(), sender.ToString())
		return
	}

	if !ChainsEqual(upMsg.ProposedIdChain, d.myIDChain) {
		d.logger.Warnf("My chain changed: (%+v -> %+v)", d.myIDChain, upMsg.ProposedIdChain)
	}

	d.myIDChain = upMsg.ProposedIdChain
	d.myLevel = upMsg.ParentLevel + 1
	d.myParent = sender
	d.myGrandParent = upMsg.GrandParent
}

func (d *DemmonTree) InConnRequested(peer peer.Peer) bool {

	if d.myParent != nil && d.myParent.Equals(peer) {
		d.logger.Infof("My parent dialed me")
		return true
	}

	_, isChildren := d.myPendingChildren[peer.ToString()]
	if isChildren {
		d.logger.Infof("My pending children dialed me, dialing back %s", peer.ToString())
		pkg.GetNodeWatcher().Watch(peer, d.ID())
		pkg.Dial(peer, d.ID(), stream.NewTCPDialer())
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
	if d.myParent != nil && d.myParent.Equals(peer) {
		d.logger.Infof("Dialed parent with success, parent: %s", d.myParent.ToString())
		return true
	}

	child, isChildren := d.myPendingChildren[peer.ToString()]
	if isChildren {
		d.logger.Infof("Dialed children with success: %s", peer.ToString())
		delete(d.myPendingChildren, peer.ToString())
		d.myChildren[peer.ToString()] = child
		pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(1*time.Second, peer))
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

			if d.canProgressToNextLevel() {
				d.progressToNextStep()
			}
			return
		}
		d.sendMessageAndMeasureLatency(message, peer)
	case joinAsChildMessage:
		d.fallbackToParent(peer)
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

func (d *DemmonTree) progressToNextStep() {

	d.logger.Infof("Getting lowest latency peer...")
	lowestLatencyPeer, peerLat, err := d.getLowestLatencyPeerWithDeadline(d.joinLevel-1, time.Now().Add(d.config.MaxTimeToProgressToNextLevel))
	if err != nil {
		d.logger.Panicf(err.Reason())
	}
	d.logger.Infof("Lowest Latency Peer: %s , Latency: %d", lowestLatencyPeer.ToString(), peerLat)

	currLevelPeers := d.currLevelPeers[d.joinLevel-1]
	currLevelPeersDone := d.currLevelPeers[d.joinLevel-1]

	d.logger.Infof("d.currLevelPeers (level %d):", d.joinLevel-1)
	for _, peerDone := range currLevelPeers {
		d.logger.Infof("%s", peerDone.ToString())
	}

	d.logger.Infof("d.currLevelPeersDone (level %d):", d.joinLevel-1)
	for _, peerDone := range currLevelPeersDone {
		d.logger.Infof("%s", peerDone.ToString())
	}

	if d.joinLevel > 1 {
		lowestLatencyPeerParent := d.parents[lowestLatencyPeer.ToString()]
		lowestLatencyPeerParentStats, err := pkg.GetNodeWatcher().GetNodeInfo(lowestLatencyPeerParent)
		if err != nil {
			d.logger.Panicf("Node %s parent (%s) was not watched", lowestLatencyPeer.ToString(), lowestLatencyPeerParent.ToString())
		}

		d.logger.Infof("Latency to %s : %d", lowestLatencyPeerParent.ToString(), lowestLatencyPeerParentStats.LatencyCalc.CurrValue())

		// Checking to see if i can become parent of currLevelNodes
		if d.parentLatencies[lowestLatencyPeer.ToString()]+d.config.GParentLatencyIncreaseThreshold >
			lowestLatencyPeerParentStats.LatencyCalc.CurrValue() {
			possibleChildren := make([]peer.Peer, 0, len(currLevelPeersDone))
			for peerID, p := range currLevelPeersDone {
				nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(d.parents[peerID])
				if err != nil {
					d.logger.Panicf("Node %s parent was not watched", lowestLatencyPeer.ToString())
				}
				if d.parentLatencies[peerID]+d.config.GParentLatencyIncreaseThreshold > nodeStats.LatencyCalc.CurrValue() {
					d.logger.Infof("Can become grandparent of: %s (%s Parent latency %d: , my latency to its parent: %d)",
						lowestLatencyPeer.ToString(),
						lowestLatencyPeer.ToString(),
						d.parentLatencies[lowestLatencyPeer.ToString()],
						lowestLatencyPeerParentStats.LatencyCalc.CurrValue())
					possibleChildren = append(possibleChildren, p)
				}
			}

			d.sendJoinAsChildMsg(lowestLatencyPeerParent, possibleChildren)
			d.logger.Infof("Joining level %d with %s as parent and as parent of %+v", d.joinLevel, lowestLatencyPeerParent.ToString(), possibleChildren)
			for _, possibleChild := range possibleChildren {
				d.sendJoinAsParentMsg(possibleChild)
			}
			d.unwatchPeersInLevel(d.joinLevel-1, append(possibleChildren, lowestLatencyPeerParent))
			return
		}

		// Checking to see if i either progress to lower levels or join curr level
		if lowestLatencyPeerParentStats.LatencyCalc.CurrValue() < peerLat {
			d.logger.Infof("Latency to (%s:%d) is higher than the latency to the parent (%s:%d nanoSec)",
				lowestLatencyPeer.ToString(), peerLat, lowestLatencyPeerParent.ToString(), lowestLatencyPeerParentStats.LatencyCalc.CurrValue())
			d.logger.Infof("Joining level %d", d.joinLevel-1)
			d.sendJoinAsChildMsg(lowestLatencyPeerParent, nil)
			d.unwatchPeersInLevel(d.joinLevel-1, []peer.Peer{lowestLatencyPeerParent})
			return
		}
	}
	d.unwatchPeersInLevel(d.joinLevel, []peer.Peer{lowestLatencyPeer}) // unwatch all nodes but the best node
	// base case
	if len(d.children[lowestLatencyPeer.ToString()]) == 0 {
		d.logger.Infof("Joining level %d because nodes in this level have no children", d.joinLevel)
		d.logger.Infof("Pending parent: %s", lowestLatencyPeer.ToString())
		d.logger.Infof("Joining level %d", d.joinLevel)
		d.sendJoinAsChildMsg(lowestLatencyPeer, nil)
		return
	}

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

func (d *DemmonTree) sendJoinAsParentMsg(newChildren peer.Peer) {
	childStats, err := pkg.GetNodeWatcher().GetNodeInfo(newChildren)
	if err != nil {
		panic(err.Reason())
	}
	proposedId := d.generateChildId()
	toSend := NewJoinAsParentMessage(d.parents[newChildren.ToString()], append(d.myIDChain, proposedId), d.joinLevel, childStats.LatencyCalc.CurrValue())
	d.myPendingChildren[newChildren.ToString()] = NewPeerWithId(proposedId, newChildren)
	d.sendMessageTmpTCPChan(toSend, newChildren)
}

func (d *DemmonTree) sendJoinAsChildMsg(newParent peer.Peer, children []peer.Peer) {
	toSend := NewJoinAsChildMessage(children)
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

func (d *DemmonTree) canProgressToNextLevel() (res bool) {
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

// aux functions
func (d *DemmonTree) sendMessageTmpTCPChan(toSend message.Message, destPeer peer.Peer) {
	pkg.SendMessageSideStream(toSend, destPeer, d.ID(), []protocol.ID{d.ID()}, stream.NewTCPDialer())
}

func (d *DemmonTree) sendMessageAndMeasureLatency(toSend message.Message, destPeer peer.Peer) {
	d.sendMessageTmpTCPChan(toSend, destPeer)
	pkg.GetNodeWatcher().Watch(destPeer, d.ID())
}

func (d *DemmonTree) sendMessage(toSend message.Message, destPeer peer.Peer) {
	pkg.SendMessage(toSend, destPeer, d.ID(), []protocol.ID{d.ID()})
}

func (d *DemmonTree) getLowestLatencyPeerWithDeadline(level uint16, deadline time.Time) (peer.Peer, time.Duration, errors.Error) {
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

	for i := 0; i < 2^IdSegmentLen; i++ {
		binary.BigEndian.PutUint64(peerId[:], uint64(i))
		_, ok := occupiedIds[peerId]
		if !ok {
			d.logger.Infof("Generated peerID: %+v", peerId)
			return peerId
		}
	}
	panic("Could not generate children ID")
}
