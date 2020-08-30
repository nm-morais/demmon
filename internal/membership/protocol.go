package membership

import (
	"math"
	"reflect"
	"runtime"
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
	MaxTimeToProgressToNextLevel    time.Duration
	MaxRetriesJoinMsg               int
	ParentRefreshTickDuration       time.Duration
	GParentLatencyIncreaseThreshold time.Duration
	NrSamplesForLatency             int
	MaxRetriesForLatency            int
	BootstrapRetryTimeout           time.Duration
	Landmarks                       []peer.Peer
}

type DemmonTree struct {
	logger *logrus.Logger
	config DemmonTreeConfig

	// node state
	myLevel       uint16
	myGrandParent peer.Peer
	myParent      peer.Peer
	myChildren    map[string]peer.Peer

	// join state

	children        map[string]map[string]peer.Peer
	parents         map[string]peer.Peer
	parentLatencies map[string]time.Duration

	myPendingChildren map[string]peer.Peer
	myPendingParent   peer.Peer

	joinLevel          uint16
	currLevelPeersDone []map[string]peer.Peer
	currLevelPeers     []map[string]peer.Peer
	retries            map[string]int
}

func NewDemmonTree(config DemmonTreeConfig) protocol.Protocol {
	return &DemmonTree{
		parents:            make(map[string]peer.Peer),
		children:           make(map[string]map[string]peer.Peer),
		currLevelPeers:     []map[string]peer.Peer{},
		currLevelPeersDone: []map[string]peer.Peer{},
		myPendingChildren:  map[string]peer.Peer{},
		myPendingParent:    nil,
		joinLevel:          1,
		retries:            map[string]int{},
		myChildren:         map[string]peer.Peer{},
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
		if pkg.SelfPeer().Equals(landmark) {
			d.logger.Infof("I am landmark")
			d.myLevel = 0
			d.myParent = nil
			d.myChildren = map[string]peer.Peer{}
			return
		}
	}
	pkg.RegisterTimer(d.ID(), NewJoinTimer(10*time.Second))
	d.joinOverlay()
}

func (d *DemmonTree) Init() {
	pkg.RegisterMessageHandler(d.ID(), JoinMessage{}, d.handleJoinMessage)
	pkg.RegisterMessageHandler(d.ID(), JoinReplyMessage{}, d.handleJoinReplyMessage)
	pkg.RegisterMessageHandler(d.ID(), JoinAsChildMessage{}, d.handleJoinAsChildMessage)
	pkg.RegisterMessageHandler(d.ID(), JoinAsParentMessage{}, d.handleJoinAsParentMessage)
	pkg.RegisterMessageHandler(d.ID(), UpdateParentMessage{}, d.handleUpdateParentMessage)

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
	_, ok := d.myChildren[refreshTimer.Child.ToString()]
	if !ok {
		d.logger.Warnf("Stopped sending refreshParent messages to: %s", refreshTimer.Child.ToString())
		return
	}
	d.logger.Infof("Nr goroutines: %d", runtime.NumGoroutine())
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(1*time.Second, refreshTimer.Child))
	toSend := UpdateParentMessage{GrandParent: d.myParent, ParentLevel: d.myLevel}
	d.sendMessage(toSend, refreshTimer.Child)
}

// message handlers

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {

	aux := make([]peer.Peer, len(d.myChildren))
	i := 0
	for _, c := range d.myChildren {
		aux[i] = c
		i++
	}

	parentLatency := time.Duration(math.MaxInt64)
	if d.myParent != nil {
		if info, err := pkg.GetNodeWatcher().GetNodeInfo(d.myParent); err == nil {
			parentLatency = info.LatencyCalc.CurrValue()
		}
	}

	d.sendMessageTmpTCPChan(JoinReplyMessage{
		Children:      aux,
		Level:         d.myLevel,
		ParentLatency: parentLatency,
	}, sender)
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(JoinReplyMessage)

	d.logger.Infof("Got joinReply: %+v", replyMsg)

	if d.joinLevel-1 != replyMsg.Level {
		d.logger.Warnf("Discarding old message %+v because joinLevel is too high: %d", replyMsg, d.joinLevel)
		// discard old repeated messages
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

func (d *DemmonTree) handleJoinAsParentMessage(sender peer.Peer, m message.Message) {
	d.logger.Infof("Peer %s wants to be my parent", sender.ToString())
	d.myPendingParent = sender
	pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {
	d.logger.Infof("Peer %s wants to be my children", sender.ToString())
	d.myPendingChildren[sender.ToString()] = sender
	pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) handleUpdateParentMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(UpdateParentMessage)

	d.myLevel = upMsg.ParentLevel + 1
	d.myParent = sender
	d.myGrandParent = upMsg.GrandParent

	d.logger.Infof("My level : %d", d.myLevel)

	if d.myParent == nil {
		d.logger.Infof("My parent : %+v", d.myParent)
	} else {
		d.logger.Infof("My parent : %s", d.myParent.ToString())
	}
	if d.myGrandParent == nil {
		d.logger.Infof("My grandparent : %+v", d.myGrandParent)
	} else {
		d.logger.Infof("My grandparent : %s", d.myGrandParent.ToString())
	}

	d.logger.Infof("Nr goroutines: %d", runtime.NumGoroutine())
}

func (d *DemmonTree) InConnRequested(peer peer.Peer) bool {

	if d.myPendingParent != nil && d.myPendingParent.Equals(peer) {
		d.logger.Infof("My pending parent dialed me")
		pkg.Dial(peer, d.ID(), stream.NewTCPDialer())
		return true
	}

	if d.myParent != nil && d.myParent.Equals(peer) {
		d.logger.Infof("My parent dialed me back")
		pkg.Dial(peer, d.ID(), stream.NewTCPDialer())
		return true
	}

	_, isChildren := d.myChildren[peer.ToString()]
	if isChildren {
		d.logger.Infof("My children dialed me back")
		return true
	}

	if d.myPendingParent == nil {
		d.logger.Infof("d.myPendingParent: %+v", d.myPendingParent)
	} else {
		d.logger.Infof("d.myPendingParent: %s", d.myPendingParent.ToString())
	}
	d.logger.Infof("d.myChildren: %+v", d.myChildren)

	d.logger.Panicf("Was dialed by unknown peer: %s", peer.ToString())
	return false
}

func (d *DemmonTree) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {

	if sourceProto != d.ID() {
		return false
	}

	d.logger.Infof("Dialed peer with success: %s", peer.ToString())
	if d.myPendingParent != nil && d.myPendingParent.Equals(peer) {
		d.myPendingParent = nil
		d.myParent = peer
		d.logger.Infof("Dialed parent with success, parent: %s", d.myParent.ToString())
		return true
	}

	_, isPendingChildren := d.myPendingChildren[peer.ToString()]
	if isPendingChildren {
		delete(d.myPendingChildren, peer.ToString())
		pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(1*time.Second, peer))
		d.myChildren[peer.ToString()] = peer
		d.logger.Infof("I have new children %s", peer.ToString())
		return true
	}

	d.logger.Infof("d.myPendingParent: %s", d.myPendingParent)
	d.logger.Infof("d.myChildren: %+v", d.myChildren)
	d.logger.Panicf("Dialed unknown peer: %s", peer.ToString())
	return false
}

func (d *DemmonTree) DialFailed(peer peer.Peer) {
	d.logger.Errorf("Failed to dial %s", peer.ToString())
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
	d.logger.Infof("Message %s delivered to: %s", reflect.TypeOf(message), peer.ToString())
}

func (d *DemmonTree) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	d.logger.Infof("Message %s failed to deliver to: %s because: %s", reflect.TypeOf(message), peer.ToString(), error.Reason())
	switch message.(type) {
	case JoinMessage:
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
	}
}

func (d *DemmonTree) joinOverlay() {
	nrLandmarks := len(d.config.Landmarks)
	d.currLevelPeersDone = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)} // start level 1
	d.currLevelPeers = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)}     // start level 1
	d.joinLevel = 1
	d.logger.Infof("Landmarks:")
	for i, landmark := range d.config.Landmarks {
		d.logger.Infof("%d :%s", i, landmark.ToString())
		d.currLevelPeers[0][landmark.ToString()] = landmark
		d.parents[landmark.ToString()] = nil
		joinMsg := JoinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark)
	}
}

func (d *DemmonTree) progressToNextStep() {

	lowestLatencyPeer, peerLat, err := d.getLowestLatencyPeerWithDeadline(d.joinLevel-1, time.Now().Add(d.config.MaxTimeToProgressToNextLevel))
	if err != nil {
		d.logger.Panicf(err.Reason())
	}

	d.logger.Infof("Lowest Latency Peer: %s , Latency: %d", lowestLatencyPeer.ToString(), peerLat)

	if d.joinLevel > 1 {
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(d.parents[lowestLatencyPeer.ToString()])
		d.logger.Infof("Latency to %s : %d", lowestLatencyPeer.ToString(), nodeStats.LatencyCalc.CurrValue())
		if err != nil && d.parentLatencies[lowestLatencyPeer.ToString()]+d.config.GParentLatencyIncreaseThreshold > nodeStats.LatencyCalc.CurrValue() {
			aux := make([]peer.Peer, 0, len(d.currLevelPeersDone[d.joinLevel]))
			for peerID, p := range d.currLevelPeersDone[d.joinLevel] {
				nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(d.parents[peerID])
				if err != nil && d.parentLatencies[peerID]+d.config.GParentLatencyIncreaseThreshold >
					nodeStats.LatencyCalc.CurrValue() {
					aux = append(aux, p)
				}
			}

			toSendToChildren := JoinAsParentMessage{
				Children: aux,
				Level:    d.joinLevel,
			}

			d.myLevel = d.joinLevel

			for _, possibleChild := range aux {
				d.myPendingChildren[possibleChild.ToString()] = possibleChild
				d.sendMessageTmpTCPChan(toSendToChildren, possibleChild)
			}

			d.myPendingParent = d.parents[lowestLatencyPeer.ToString()]
			d.sendMessageTmpTCPChan(JoinAsChildMessage{}, d.parents[lowestLatencyPeer.ToString()])
			d.logger.Infof("Joining level %d with %s as parent and as parent of %s", d.joinLevel, d.myPendingParent.ToString(), aux)
			return
		}

		lowestLatencyPeerParent, ok := d.parents[lowestLatencyPeer.ToString()]
		if ok {
			lowestLatencyPeerParentStats, err := pkg.GetNodeWatcher().GetNodeInfo(lowestLatencyPeerParent)
			if err != nil && lowestLatencyPeerParentStats.LatencyCalc.CurrValue() < peerLat {
				toSend := JoinAsChildMessage{}
				d.logger.Infof("Latency to (%s:%d) is higher than the latency to the parent (%s:%d nanoSec)",
					lowestLatencyPeer.ToString(), peerLat, lowestLatencyPeerParent.ToString(), lowestLatencyPeerParentStats.LatencyCalc.CurrValue())
				d.logger.Infof("Joining level %d", d.joinLevel-1)
				d.myLevel = d.joinLevel
				d.myPendingParent = d.parents[lowestLatencyPeer.ToString()]
				d.sendMessageTmpTCPChan(toSend, d.parents[lowestLatencyPeer.ToString()])
				return
			}
		}
	}

	// base case
	if len(d.children[lowestLatencyPeer.ToString()]) == 0 {
		d.logger.Infof("Joining level %d because nodes in this level have no children", d.joinLevel)
		d.logger.Infof("Pending parent: %s", lowestLatencyPeer.ToString())
		d.logger.Infof("Joining level %d", d.joinLevel)
		d.myLevel = d.joinLevel
		d.myPendingParent = lowestLatencyPeer
		d.sendMessageTmpTCPChan(JoinAsChildMessage{}, lowestLatencyPeer)
		return
	}

	d.joinLevel++
	d.logger.Infof("Progressing to next level (currLevel=%d), (nextLevel=%d) ", d.joinLevel-1, d.joinLevel)

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
		d.sendMessageAndMeasureLatency(JoinMessage{}, p)
	}
}

func (d *DemmonTree) canProgressToNextLevel() (res bool) {
	d.logger.Info("d.currLevelPeersDone:")
	for i, peersDone := range d.currLevelPeersDone {
		d.logger.Infof("Level %d", i)
		for _, peerDone := range peersDone {
			d.logger.Infof("%s", peerDone.ToString())
		}
	}

	d.logger.Info("d.currLevelPeers:")
	for i, peersDone := range d.currLevelPeers {
		d.logger.Infof("Level %d", i)
		for _, peerDone := range peersDone {
			d.logger.Infof("%s", peerDone.ToString())
		}
	}

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

func (d *DemmonTree) sendMessageTmpUDPChan(toSend message.Message, destPeer peer.Peer) {
	pkg.SendMessageSideStream(toSend, destPeer, d.ID(), []protocol.ID{d.ID()}, stream.NewUDPDialer())
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
