package membership

import (
	"encoding/binary"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"time"

	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const protoID = 1000
const protoName = "DemonTree"

type DemmonTreeConfig = struct {
	// join protocol configs
	MaxTimeToProgressToNextLevel        time.Duration
	MaxRetriesJoinMsg                   int
	ParentRefreshTickDuration           time.Duration
	ChildrenRefreshTickDuration         time.Duration
	MinLatencyImprovementToBecomeParent time.Duration
	Landmarks                           []PeerWithId
	MinGrpSize                          int
	MaxGrpSize                          int
	LimitFirstLevelGroupSize            bool
	RejoinTimerDuration                 time.Duration

	// maintenance configs
	CheckChildenSizeTimerDuration time.Duration

	// Peer sampling service
	MaxPeersInEView            int
	EmitWalkTimeout            time.Duration // how much time  to wait before sending new Walk
	EmitWalkProbability        float32
	BiasedWalkProbability      float32
	NrHopsToIgnoreWalk         int
	RandomWalkTTL              int
	BiasedWalkTTL              int
	NrPeersInWalkMessage       int
	NrPeersToMergeInWalkSample int

	// self-improvement service
	NrPeersToMeasure                       int
	MeasureNewPeersRefreshTickDuration     time.Duration
	MeasuredPeersSize                      int
	EvalMeasuredPeersRefreshTickDuration   time.Duration
	AttemptImprovePositionProbability      float32
	MinLatencyImprovementToImprovePosition time.Duration
}

type measuredPeer = struct {
	peer            PeerWithIdChain
	measuredLatency time.Duration
}

type DemmonTree struct {
	logger *logrus.Logger
	config DemmonTreeConfig

	// node state
	landmark      bool
	myIDChain     PeerIDChain
	myLevel       uint16
	myGrandParent peer.Peer
	myParent      peer.Peer
	myChildren    map[string]PeerWithId
	mySiblings    map[string]PeerWithId

	// join state
	myPendingParentInJoin   peer.Peer
	myPendingChildrenInJoin map[string]PeerWithId
	joinLevel               uint16
	children                map[string]map[string]PeerWithIdChain
	parents                 map[string]PeerWithIdChain
	parentLatencies         map[string]time.Duration
	currLevelPeers          []map[string]peer.Peer
	currLevelPeersDone      []map[string]PeerWithIdChain
	retries                 map[string]int

	// improvement service state
	measuringPeers               map[string]bool
	measuredPeers                []measuredPeer
	eView                        []PeerWithIdChain
	myPendingParentInImprovement *measuredPeer
}

func NewDemmonTree(config DemmonTreeConfig) protocol.Protocol {
	return &DemmonTree{

		logger: logs.NewLogger(protoName),

		// join state
		parents:               map[string]PeerWithIdChain{},
		children:              map[string]map[string]PeerWithIdChain{},
		currLevelPeers:        []map[string]peer.Peer{},
		currLevelPeersDone:    []map[string]PeerWithIdChain{},
		joinLevel:             1,
		retries:               map[string]int{},
		parentLatencies:       make(map[string]time.Duration),
		config:                config,
		myPendingParentInJoin: nil,

		// node state
		myIDChain:               nil,
		myGrandParent:           nil,
		myParent:                nil,
		myLevel:                 math.MaxInt16,
		myPendingChildrenInJoin: make(map[string]PeerWithId),
		mySiblings:              map[string]PeerWithId{},
		myChildren:              map[string]PeerWithId{},

		// improvement state
		eView:                        []PeerWithIdChain{},
		measuringPeers:               make(map[string]bool),
		measuredPeers:                []measuredPeer{},
		myPendingParentInImprovement: nil,
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
			d.landmark = true
			for _, landmark := range d.config.Landmarks {
				if !pkg.SelfPeer().Equals(landmark.Peer()) {
					d.mySiblings[landmark.Peer().ToString()] = landmark
				}
			}
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
	pkg.RegisterTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration))
	pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
	pkg.RegisterTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout))

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

	// pkg.RegisterMessageHandler(d.ID(), biasedWalkMessage{}, d.handleBiasedWalkMessage)

	pkg.RegisterMessageHandler(d.ID(), randomWalkMessage{}, d.handleRandomWalkMessage)
	pkg.RegisterMessageHandler(d.ID(), walkReplyMessage{}, d.handleWalkReplyMessage)

	pkg.RegisterNotificationHandler(d.ID(), peerMeasuredNotification{}, d.handlePeerMeasuredNotification)

	pkg.RegisterTimerHandler(d.ID(), joinTimerID, d.handleJoinTimer)
	pkg.RegisterTimerHandler(d.ID(), parentRefreshTimerID, d.handleRefreshParentTimer)
	pkg.RegisterTimerHandler(d.ID(), updateChildTimerID, d.handleUpdateChildTimer)
	pkg.RegisterTimerHandler(d.ID(), checkChidrenSizeTimerID, d.handleCheckChildrenSizeTimer)
	pkg.RegisterTimerHandler(d.ID(), externalNeighboringTimerID, d.handleExternalNeighboringTimer)
	pkg.RegisterTimerHandler(d.ID(), measureNewPeersTimerID, d.handleMeasureNewPeersTimer)
	pkg.RegisterTimerHandler(d.ID(), evalMeasuredPeersTimerID, d.handleEvalMeasuredPeersTimer)
}

// notification handlers

func (d *DemmonTree) handlePeerMeasuredNotification(notification notification.Notification) {
	peerMeasuredNotification := notification.(peerMeasuredNotification)
	delete(d.measuringPeers, peerMeasuredNotification.peerMeasured.Peer().ToString())
	currNodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(peerMeasuredNotification.peerMeasured.Peer())
	if err != nil {
		d.logger.Error(err.Reason())
		return
	} else {
		peerMeasured := peerMeasuredNotification.peerMeasured
		found := false
		for _, curr := range d.measuredPeers {
			if curr.peer.Peer().Equals(peerMeasured.Peer()) {
				found = true
				curr.measuredLatency = currNodeStats.LatencyCalc.CurrValue()
				break
			}
		}
		if !found {
			d.measuredPeers = append(d.measuredPeers, measuredPeer{
				peer:            peerMeasuredNotification.peerMeasured,
				measuredLatency: currNodeStats.LatencyCalc.CurrValue(),
			})
		}
		sort.SliceStable(d.measuredPeers, func(i, j int) bool { return d.measuredPeers[i].measuredLatency < d.measuredPeers[j].measuredLatency })
		if len(d.measuredPeers) > d.config.MeasuredPeersSize {
			d.measuredPeers = d.measuredPeers[:d.config.MeasuredPeersSize]
		}
	}
	if !d.isNeighbour(peerMeasuredNotification.peerMeasured.Peer()) {
		pkg.GetNodeWatcher().Unwatch(peerMeasuredNotification.peerMeasured.Peer(), d.ID())
	}
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
	d.logger.Info("RefreshParentTimer trigger")
	for _, child := range d.myChildren {
		toSend := NewUpdateParentMessage(d.myGrandParent, d.myLevel, append(d.myIDChain, child.ID()))
		d.sendMessage(toSend, child.Peer())
	}
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
}

func (d *DemmonTree) handleUpdateChildTimer(timer timer.Timer) {
	d.logger.Info("UpdateChildTimer trigger")
	if d.myParent != nil {
		toSend := NewUpdateChildMessage(len(d.myChildren))
		d.sendMessage(toSend, d.myParent)
	}
	pkg.RegisterTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
}

func (d *DemmonTree) handleExternalNeighboringTimer(joinTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout))

	d.logger.Info("ExternalNeighboringTimer trigger")

	if d.myParent == nil || len(d.myIDChain) == 0 { // TODO ask about this
		return
	}

	r := rand.Float32()
	if r > d.config.EmitWalkProbability {
		return
	}

	d.logger.Info("sending walk...")
	selfPeerWithChain := NewPeerWithIdChain(d.myIDChain, pkg.SelfPeer(), uint16(len(d.myChildren)))
	possibilitiesToSend := d.getNeighborsAsPeerWithIdChainArray()
	toPrint := ""

	for _, possibility := range possibilitiesToSend {
		toPrint = toPrint + "; " + possibility.Peer().ToString()
	}
	d.logger.Infof("d.possibilitiesToSend: %s:", toPrint)

	sample := getRandSample(d.config.NrPeersInWalkMessage-1, possibilitiesToSend...)
	sample = append(sample, selfPeerWithChain)

	r = rand.Float32()

	var msgToSend message.Message
	var peerToSendTo PeerWithIdChain
	// if r < d.config.BiasedWalkProbability {
	// 	msgToSend = NewBiasedWalkMessage(uint16(d.config.RandomWalkTTL), selfPeerWithChain, sample)
	// 	peerToSendTo = getBiasedPeerExcluding(possibilitiesToSend, selfPeerWithChain)
	// } else {
	msgToSend = NewRandomWalkMessage(uint16(d.config.RandomWalkTTL), selfPeerWithChain, sample)

	peerToSendTo = getRandomExcluding(getExcludingDescendantsOf(possibilitiesToSend, d.myIDChain), selfPeerWithChain)
	// }

	if peerToSendTo == nil {
		d.logger.Error("peerToSendTo is nil")
		return
	}

	_, isChildren := d.myChildren[peerToSendTo.Peer().ToString()]
	if isChildren {
		d.sendMessage(msgToSend, peerToSendTo.Peer())
		return
	}

	isParent := d.myParent.Equals(peerToSendTo.Peer())
	if isParent {
		d.sendMessage(msgToSend, peerToSendTo.Peer())
		return
	}
	d.sendMessageTmpUDPChan(msgToSend, peerToSendTo.Peer())
}

func (d *DemmonTree) handleEvalMeasuredPeersTimer(evalMeasuredPeersTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
	d.logger.Info("EvalMeasuredPeersTimer trigger...")

	if d.myIDChain != nil || len(d.myIDChain) == 0 {
		return
	}

	r := rand.Float32()
	if r > d.config.AttemptImprovePositionProbability {
		return
	}

	d.logger.Info("Evaluating new peers...")
	if len(d.measuredPeers) == 0 {
		return
	}

	d.logger.Info("measured peers:")
	var bestPeer measuredPeer
	checked := 0
	for _, measuredPeer := range d.measuredPeers {
		d.logger.Infof("%s : %s", measuredPeer.peer.Peer().ToString(), measuredPeer.measuredLatency)
		if !d.isNeighbour(measuredPeer.peer.Peer()) && !IsDescendant(d.myIDChain, bestPeer.peer.Chain()) {
			bestPeer = measuredPeer
			break
		}
	}
	d.measuredPeers = d.measuredPeers[checked:]
	d.logger.Infof("Least latency peer: %s", bestPeer.peer.Peer().ToString())

	currParLatency, err := pkg.GetNodeWatcher().GetNodeInfo(d.myParent)
	if err != nil {
		d.logger.Error(err.Reason())
		return
	}
	parentLatency := currParLatency.LatencyCalc.CurrValue()
	if bestPeer.measuredLatency > parentLatency {
		return
	}

	// parent latency is higher than latency to peer
	latencyImprovement := parentLatency - bestPeer.measuredLatency
	if latencyImprovement > d.config.MinLatencyImprovementToImprovePosition {
		pkg.GetNodeWatcher().WatchWithInitialLatencyValue(bestPeer.peer.Peer(), d.ID(), bestPeer.measuredLatency)
		d.myPendingParentInImprovement = &bestPeer
		childrenAsArr := make([]peer.Peer, 0, len(d.myChildren))
		for _, child := range d.myChildren {
			childrenAsArr = append(childrenAsArr, child.Peer())
		}
		d.sendJoinAsChildMsg(bestPeer.peer.Peer(), bestPeer.measuredLatency, bestPeer.peer.Chain(), childrenAsArr)
	}
}

func (d *DemmonTree) handleMeasureNewPeersTimer(measureNewPeersTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration))
	d.logger.Infof("handleMeasureNewPeersTimer trigger")
	if len(d.eView) == 0 {
		return
	}
	toMeasure := d.config.NrPeersToMeasure
	sample := getRandSample(d.config.NrPeersToMeasure, d.eView...)
	for _, peer := range sample {
		if toMeasure == 0 {
			return
		}

		if _, isMeasuring := d.measuringPeers[peer.Peer().ToString()]; isMeasuring {
			continue
		}

		found := false
		for _, curr := range d.measuredPeers {
			if curr.peer.Peer().Equals(peer.Peer()) {
				found = true
				break
			}
		}

		if !found {
			d.logger.Infof("measuring peer: %s", peer.Peer().ToString())
			pkg.GetNodeWatcher().Watch(peer.Peer(), d.ID())
			c := pkg.Condition{
				Repeatable:                false,
				CondFunc:                  func(pkg.NodeInfo) bool { return true },
				EvalConditionTickDuration: 500 * time.Millisecond,
				Notification:              peerMeasuredNotification{peerMeasured: peer},
				Peer:                      peer.Peer(),
				EnableGracePeriod:         false,
				ProtoId:                   d.ID(),
			}
			d.logger.Infof("Doing NotifyOnCondition for node %s...", peer.Peer().ToString())
			pkg.GetNodeWatcher().NotifyOnCondition(c)
			d.measuringPeers[peer.Peer().ToString()] = true
			toMeasure--
		}
	}
}

// message handlers

func (d *DemmonTree) handleRandomWalkMessage(sender peer.Peer, m message.Message) {
	randWalkMsg := m.(randomWalkMessage)
	d.logger.Infof("Got randomWalkMessage: %+v from %s", randWalkMsg, sender.ToString())
	toPrint := ""
	for _, peer := range randWalkMsg.Sample {
		toPrint = toPrint + "; " + peer.Peer().ToString()
	}
	d.logger.Infof("randomWalkMessage peers: %s", toPrint)
	d.logger.Infof("randomWalkMessage sender: %s : %+v", randWalkMsg.Sender.Peer().ToString(), randWalkMsg.Sender.Chain())

	hopsTaken := d.config.RandomWalkTTL - int(randWalkMsg.TTL)
	self := &peerWithIdChain{
		nChildren: uint16(len(d.myChildren)),
		chain:     d.myIDChain,
		self:      pkg.SelfPeer(),
	}

	if hopsTaken < d.config.NrHopsToIgnoreWalk {
		randWalkMsg.TTL--
		neighbours := d.getNeighborsAsPeerWithIdChainArray()
		neighboursWithoutSenderDescendants := getExcludingDescendantsOf(neighbours, randWalkMsg.Sender.Chain())
		toPrint := ""
		for _, peer := range neighboursWithoutSenderDescendants {
			d.logger.Infof("%+v", peer)
			d.logger.Infof("neighboursWithoutSenderDescendants peer: %s", peer.Peer().ToString())
		}
		d.logger.Infof("neighboursWithoutSenderDescendants: %s", toPrint)
		p := getRandomExcluding(neighboursWithoutSenderDescendants, randWalkMsg.Sender)
		if p == nil {
			sampleToSend := d.mergeEViewWith(randWalkMsg.Sample, neighboursWithoutSenderDescendants, self, d.config.NrPeersToMergeInWalkSample, randWalkMsg.Sender.Chain())
			walkReply := NewWalkReplyMessage(sampleToSend)
			d.sendMessageTmpTCPChan(walkReply, randWalkMsg.Sender.Peer())
			return
		}
		d.sendMessage(randWalkMsg, p.Peer())
		return
	}

	// removed, remaining <- removeNNHopsAway(excludeDescendants(sample, self) ,NrPeersToMerge, MinHopsToMerge)
	// toInsert <- selectRand(excludeInSample({siblings, parent, sender ,eView}, sample), len(removed))
	if randWalkMsg.TTL > 0 {
		neighbours := d.getNeighborsAsPeerWithIdChainArray()
		neighboursWithoutSenderDescendants := getExcludingDescendantsOf(neighbours, randWalkMsg.Sender.Chain())
		toPrint := ""
		for _, peer := range neighboursWithoutSenderDescendants {
			d.logger.Infof("%+v", peer)
			d.logger.Infof("neighboursWithoutSenderDescendants peer: %s", peer.Peer().ToString())
		}
		d.logger.Infof("neighboursWithoutSenderDescendants: %s", toPrint)
		sampleToSend := d.mergeEViewWith(randWalkMsg.Sample, neighboursWithoutSenderDescendants, self, d.config.NrPeersToMergeInWalkSample, randWalkMsg.Sender.Chain())
		randWalkMsg.TTL--
		p := getRandomExcluding(neighboursWithoutSenderDescendants, randWalkMsg.Sender)
		if p == nil {
			walkReply := NewWalkReplyMessage(sampleToSend)
			d.sendMessageTmpTCPChan(walkReply, randWalkMsg.Sender.Peer())
			return
		}
		randWalkMsg.Sample = sampleToSend
		d.sendMessage(randWalkMsg, p.Peer())
		return
	}

	// TTL == 0
	if randWalkMsg.TTL == 0 {
		possibilitiesToSend := d.getNeighborsAsPeerWithIdChainArray()
		neighboursWithoutSenderDescendants := getExcludingDescendantsOf(possibilitiesToSend, randWalkMsg.Sender.Chain())
		toPrint := ""
		for _, peer := range neighboursWithoutSenderDescendants {
			d.logger.Infof("%+v", peer)
			d.logger.Infof("neighboursWithoutSenderDescendants peer: %s", peer.Peer().ToString())
		}
		d.logger.Infof("neighboursWithoutSenderDescendants: %s", toPrint)
		sampleToSend := d.mergeEViewWith(randWalkMsg.Sample, neighboursWithoutSenderDescendants, self, d.config.NrPeersToMergeInWalkSample, randWalkMsg.Sender.Chain())
		randWalkMsg.Sample = sampleToSend
		d.sendMessage(NewWalkReplyMessage(randWalkMsg.Sample), randWalkMsg.Sender.Peer())
		return
	}
}

func (d *DemmonTree) handleWalkReplyMessage(sender peer.Peer, m message.Message) {
	walkReply := m.(walkReplyMessage)
	d.logger.Infof("Got walkReplyMessage: %+v from %s", walkReply, sender.ToString())
	sample := walkReply.Sample
	if len(d.myIDChain) > 0 {
	outer:
		for i := 0; i < len(sample); i++ {
			currPeer := sample[i]

			if currPeer.Peer().Equals(pkg.SelfPeer()) {
				continue
			}

			if d.isNeighbour(currPeer.Peer()) {
				continue
			}

			for _, p := range d.eView {
				if p.Peer().Equals(currPeer.Peer()) {
					continue outer
				}
			}

			if IsDescendant(d.myIDChain, currPeer.Chain()) {
				continue outer
			}

			if len(d.eView) == d.config.MaxPeersInEView { // eView is full
				toRemoveIdx := rand.Intn(len(d.eView))
				d.eView[toRemoveIdx] = sample[i]
			} else {
				d.eView = append(d.eView, sample[i])
			}
		}
	}

}

func (d *DemmonTree) handleAbsorbMessage(sender peer.Peer, m message.Message) {
	d.logger.Infof("Got evictMessage: %+v from %s", m, sender.ToString())

	var minLatSibling PeerWithId
	minLat := time.Duration(math.MaxInt64)
	for _, sibling := range d.mySiblings {
		info, err := pkg.GetNodeWatcher().GetNodeInfo(sibling.Peer())
		if err != nil {
			panic(err.Reason())
		}
		latTo := info.LatencyCalc.CurrValue()
		if latTo > minLat {
			minLat = latTo
			minLatSibling = sibling
		}
	}

	if minLatSibling == nil {
		panic("Have no siblings but got evict message")
	}

	d.sendJoinAsParentMsg(&peerWithIdChain{nChildren: 0, chain: append(d.myIDChain[:len(d.myIDChain)-1], minLatSibling.ID()), self: minLatSibling.Peer()})
}

func (d *DemmonTree) handleDisconnectAsChildMsg(sender peer.Peer, m message.Message) {
	dacMsg := m.(disconnectAsChildMessage)
	d.logger.Infof("got DisconnectAsChildMsg %+v from %s", dacMsg, sender.ToString())
	delete(d.myChildren, sender.ToString())
	pkg.Disconnect(d.ID(), sender)
	pkg.GetNodeWatcher().Unwatch(sender, d.ID())
}

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {

	aux := make([]PeerWithId, len(d.myChildren))
	i := 0
	for _, c := range d.myChildren {
		aux[i] = c
		i++
	}

	if d.landmark {
		toSend := NewJoinReplyMessage(aux, d.myLevel, math.MaxInt64, d.myIDChain)
		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	if d.myParent != nil {
		info, err := pkg.GetNodeWatcher().GetNodeInfo(d.myParent)
		if err != nil {
			panic(err.Reason())
		}
		parentLatency := info.LatencyCalc.CurrValue()
		toSend := NewJoinReplyMessage(aux, d.myLevel, parentLatency, d.myIDChain)
		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	if d.myPendingParentInJoin != nil {
		info, err := pkg.GetNodeWatcher().GetNodeInfo(d.myPendingParentInJoin)
		if err != nil {
			panic(err.Reason())
		}
		parentLatency := info.LatencyCalc.CurrValue()
		toSend := NewJoinReplyMessage(aux, d.myLevel, parentLatency, d.myIDChain)
		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	panic("do not have parentLatency")
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(joinReplyMessage)

	d.logger.Infof("Got joinReply: %+v from %s", replyMsg, sender.ToString())

	if (d.joinLevel-1 != replyMsg.Level) || (d.joinLevel > 1 && !IsDescendant(d.parents[sender.ToString()].Chain(), replyMsg.IdChain)) {

		if d.joinLevel-1 != replyMsg.Level {
			d.logger.Warnf("Discarding joinReply %+v from %s because joinLevel is not mine: %d", replyMsg, sender.ToString(), d.joinLevel)
		}
		if d.joinLevel > 1 && !IsDescendant(d.parents[sender.ToString()].Chain(), replyMsg.IdChain) {
			d.logger.Warnf("Discarding joinReply %+v from %s because node does not have the same parent... should be: %s", replyMsg, sender.ToString(), d.parents[sender.ToString()].Peer().ToString())
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

	peerWithIdChain := NewPeerWithIdChain(replyMsg.IdChain, sender, uint16(len(replyMsg.Children)))
	d.currLevelPeersDone[d.joinLevel-1][sender.ToString()] = peerWithIdChain
	d.children[sender.ToString()] = make(map[string]PeerWithIdChain, len(replyMsg.Children))
	for _, c := range replyMsg.Children {
		d.children[sender.ToString()][c.Peer().ToString()] = NewPeerWithIdChain(append(replyMsg.IdChain, c.ID()), c.Peer(), 0)
	}

	d.parentLatencies[sender.ToString()] = time.Duration(replyMsg.ParentLatency)
	for _, children := range replyMsg.Children {
		d.parents[children.Peer().ToString()] = peerWithIdChain
	}

	if d.canProgressToNextStep() {
		d.progressToNextStep()
	}
}

func (d *DemmonTree) handleJoinAsParentMessage(sender peer.Peer, m message.Message) {
	japMsg := m.(joinAsParentMessage)
	d.logger.Infof("got JoinAsParentMessage %+v from %s", japMsg, sender.ToString())

	if !ChainsEqual(d.myIDChain, japMsg.ExpectedId) {
		d.logger.Warnf("Discarding parent %s because it was trying to optimize with previous position", sender.ToString())
		return
	}
	d.addParent(sender, japMsg.ProposedId, japMsg.Level, japMsg.MeasuredLatency)
}

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {
	jacMsg := m.(joinAsChildMessage)
	d.logger.Infof("got JoinAsChildMessage %+v from %s", jacMsg, sender.ToString())

	if !ChainsEqual(jacMsg.ExpectedId, d.myIDChain) {
		toSend := NewJoinAsChildMessageReply(false, nil, 0, nil)
		d.logger.Infof("denying joinAsChildReply from %s", sender.ToString())
		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	for _, child := range jacMsg.Children {
		childStr := child.ToString()
		d.logger.Infof("Removing child: %s as %s is taking it", childStr, sender.ToString())
		if _, ok := d.myChildren[childStr]; ok {
			pkg.Disconnect(d.ID(), child)
			delete(d.myChildren, childStr)
		}
	}
	childrenId := d.generateChildId()
	childrenToSend := make([]PeerWithId, 0, len(d.myChildren))
	for _, child := range d.myChildren {
		childrenToSend = append(childrenToSend, child)
	}
	toSend := NewJoinAsChildMessageReply(true, append(d.myIDChain, childrenId), d.myLevel, childrenToSend)
	d.sendMessageTmpTCPChan(toSend, sender)
	d.myPendingChildrenInJoin[sender.ToString()] = NewPeerWithId(childrenId, sender, len(jacMsg.Children))
	pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
	pkg.GetNodeWatcher().WatchWithInitialLatencyValue(sender, d.ID(), jacMsg.MeasuredLatency)
}

func (d *DemmonTree) handleJoinAsChildMessageReply(sender peer.Peer, m message.Message) {
	japrMsg := m.(joinAsChildMessageReply)
	d.logger.Infof("got JoinAsChildMessageReply %+v from %s", japrMsg, sender.ToString())

	if sender.Equals(d.myPendingParentInJoin) {
		if japrMsg.Accepted {
			d.addParent(sender, japrMsg.ProposedId, japrMsg.ParentLevel, 0)
		} else {
			d.fallbackToParent(sender)
		}
	}

	if d.myPendingParentInImprovement != nil && sender.Equals(d.myPendingParentInImprovement.peer.Peer()) {
		if japrMsg.Accepted {
			d.addParent(sender, japrMsg.ProposedId, japrMsg.ParentLevel, d.myPendingParentInImprovement.measuredLatency)
		}
		d.myPendingParentInImprovement = nil
	}
}

func (d *DemmonTree) handleUpdateParentMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(updateParentMessage)
	d.logger.Infof("got UpdateParentMessage %+v from %s", upMsg, sender.ToString())
	if d.myParent == nil || !sender.Equals(d.myParent) {
		if d.myParent != nil {
			d.logger.Errorf("Received UpdateParentMessage from not my parent (parent:%s sender:%s)", d.myParent.ToString(), sender.ToString())
			return
		} else {
			d.logger.Errorf("Received UpdateParentMessage from not my parent (parent:%+v sender:%s)", d.myParent, sender.ToString())
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
	d.logger.Infof("got updateChildMessage %+v from %s", m, sender.ToString())
	child, ok := d.myChildren[sender.ToString()]
	if !ok {
		child, ok := d.myPendingChildrenInJoin[sender.ToString()]
		if !ok {
			d.logger.Errorf("got updateChildMessage %+v from not my children, or my pending children: %s", m, sender.ToString())
			return
		}
		child.SetChildrenNr(upMsg.NChildren)
		return
	}
	child.SetChildrenNr(upMsg.NChildren)
}

func (d *DemmonTree) InConnRequested(peer peer.Peer) bool {

	if (d.myParent != nil && d.myParent.Equals(peer)) || (d.myPendingParentInJoin != nil && d.myPendingParentInJoin.Equals(peer)) {
		d.logger.Infof("My parent dialed me")
		return true
	}

	_, isPendingChildren := d.myPendingChildrenInJoin[peer.ToString()]
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
	if d.myPendingParentInJoin != nil && d.myPendingParentInJoin.Equals(peer) {
		d.logger.Infof("Dialed parent with success, parent: %s", d.myPendingParentInJoin.ToString())
		d.myParent = d.myPendingParentInJoin
		d.myPendingParentInJoin = nil
		return true
	}

	child, isPendingChildren := d.myPendingChildrenInJoin[peer.ToString()]
	if isPendingChildren {
		d.logger.Infof("Dialed children with success: %s", peer.ToString())
		delete(d.myPendingChildrenInJoin, peer.ToString())
		d.myChildren[peer.ToString()] = child
		return true
	}

	d.logger.Infof("d.myParent: %s", d.myParent)
	d.logger.Infof("d.myChildren: %+v", d.myChildren)
	d.logger.Panicf("Dialed unknown peer: %s", peer.ToString())
	return false
}

func (d *DemmonTree) DialFailed(peer peer.Peer) {
	d.logger.Panicf("Failed to dial %s", peer.ToString())
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
	d.currLevelPeersDone = []map[string]PeerWithIdChain{make(map[string]PeerWithIdChain, nrLandmarks)} // start level 1
	d.currLevelPeers = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)}                 // start level 1
	d.joinLevel = 1
	d.logger.Infof("Landmarks:")
	for i, landmark := range d.config.Landmarks {
		d.logger.Infof("%d :%s", i, landmark.Peer().ToString())
		d.currLevelPeers[0][landmark.Peer().ToString()] = landmark.Peer()
		joinMsg := joinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark.Peer())
	}
}

func (d *DemmonTree) attemptJoinAsChildInLevel(lowestLatencyPeer PeerWithIdChain, lowestLatentyPeerLat time.Duration, lowestLatencyPeerParent PeerWithIdChain, lowestLatencyPeerParentLat time.Duration) bool {
	if len(d.children[lowestLatencyPeer.Peer().ToString()]) < d.config.MaxGrpSize { // can join group without more checks
		d.logger.Infof("Latency to (%s:%d) is higher than the latency to the parent (%s:%d nanoSec)",
			lowestLatencyPeer.Peer().ToString(), lowestLatentyPeerLat, lowestLatencyPeerParent.Peer().ToString(), lowestLatencyPeerParentLat)
		d.logger.Infof("Joining level %d", d.joinLevel-1)
		d.myPendingParentInJoin = lowestLatencyPeerParent.Peer()
		d.sendJoinAsChildMsg(lowestLatencyPeerParent.Peer(), lowestLatencyPeerParentLat, lowestLatencyPeerParent.Chain(), nil)
		return true
	} else { // group size is too big
		// can still join if we are joining level 1 and LimitFirstLevelGroupSize == False
		if d.joinLevel == 2 && !d.config.LimitFirstLevelGroupSize { // if level is 3, 4 or more, always care about group size
			d.logger.Infof("Latency to (%s:%d) is higher than the latency to the parent (%s:%d nanoSec)",
				lowestLatencyPeer.Peer().ToString(), lowestLatentyPeerLat, lowestLatencyPeerParent.Peer().ToString(), lowestLatencyPeerParentLat)
			d.logger.Infof("Joining level %d", d.joinLevel-1)
			d.myPendingParentInJoin = lowestLatencyPeerParent.Peer()
			d.sendJoinAsChildMsg(lowestLatencyPeerParent.Peer(), lowestLatencyPeerParentLat, lowestLatencyPeerParent.Chain(), nil)
			return true
		}
	}
	return false
}

func (d *DemmonTree) joinAsParentInLevel(currLevelPeersDone map[string]PeerWithIdChain, lowestLatencyPeer PeerWithIdChain, lowestLatencyPeerLat time.Duration, lowestLatencyPeerParent PeerWithIdChain, lowestLatencyPeerParentLat time.Duration) {

	possibleChildren := make([]PeerWithIdChain, 0)

	// find which peers to "steal"
	for peerID, p := range currLevelPeersDone {
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(p.Peer())
		if err != nil {
			d.logger.Panicf("Node %s parent was not watched", lowestLatencyPeer.Peer().ToString())
		}
		currLat := nodeStats.LatencyCalc.CurrValue()

		if d.parentLatencies[peerID] > currLat {
			latDifference := d.parentLatencies[peerID] - currLat
			if latDifference > d.config.MinLatencyImprovementToBecomeParent {
				d.logger.Infof("Can become parent of: %s (Parent latency %d: , my latency to it: %d)",
					peerID,
					d.parentLatencies[peerID],
					currLat)
				possibleChildren = append(possibleChildren, p)
			}
		}
	}

	// currPeersInGroup  (including myself) - childrenToSteal must be higher than the min number of elements in group
	nrRemainingPeersInGrp := 1 + len(currLevelPeersDone) - len(possibleChildren)
	if nrRemainingPeersInGrp < d.config.MinGrpSize {
		sort.SliceStable(possibleChildren, func(i, j int) bool {
			child1 := possibleChildren[i].Peer()
			nodeStats1, err := pkg.GetNodeWatcher().GetNodeInfo(child1)
			if err != nil {
				d.logger.Panicf("Node %s was not watched", child1)
			}
			child2 := possibleChildren[j].Peer()
			nodeStats2, err := pkg.GetNodeWatcher().GetNodeInfo(child2)
			if err != nil {
				d.logger.Panicf("Node %s was not watched", child2)
			}
			return nodeStats1.LatencyCalc.CurrValue() < nodeStats2.LatencyCalc.CurrValue()
		})
		overflow := d.config.MinGrpSize - nrRemainingPeersInGrp
		d.logger.Infof("nrRemainingPeersInGrp: %d", nrRemainingPeersInGrp)
		d.logger.Infof("len(currLevelPeersDone): %d", len(currLevelPeersDone))
		d.logger.Infof("len(possibleChildren): %d", len(possibleChildren))
		if overflow < 0 {
			panic("overflow is negative...")
		}
		d.logger.Infof("Overflow: %d", overflow)
		possibleChildren = possibleChildren[:len(possibleChildren)-overflow]
	}

	d.myPendingParentInJoin = lowestLatencyPeerParent.Peer()
	childrenAsArr := make([]peer.Peer, 0, len(possibleChildren))
	for _, newPossibleChild := range possibleChildren {
		childrenAsArr = append(childrenAsArr, newPossibleChild.Peer())
	}
	d.sendJoinAsChildMsg(lowestLatencyPeerParent.Peer(), lowestLatencyPeerParentLat, lowestLatencyPeerParent.Chain(), childrenAsArr)
	d.logger.Infof("Joining level %d with %s as parent and as parent of %s", d.joinLevel, lowestLatencyPeerParent.Peer().ToString(), possibleChildren)
	var exclusions []peer.Peer
	for _, possibleChild := range possibleChildren {
		d.sendJoinAsParentMsg(possibleChild)
		exclusions = append(exclusions, possibleChild.Peer())
	}
	exclusions = append(exclusions, lowestLatencyPeerParent.Peer())
	d.unwatchPeersInLevel(d.joinLevel-1, exclusions)
}

func (d *DemmonTree) sendJoinAsParentMsg(newChild PeerWithIdChain) {
	childStats, err := pkg.GetNodeWatcher().GetNodeInfo(newChild.Peer())
	if err != nil {
		panic(err.Reason())
	}
	proposedId := d.generateChildId()
	toSend := NewJoinAsParentMessage(newChild.Chain(), append(d.myIDChain, proposedId), d.joinLevel, childStats.LatencyCalc.CurrValue())
	d.myPendingChildrenInJoin[newChild.Peer().ToString()] = NewPeerWithId(proposedId, newChild.Peer(), len(d.children[newChild.Peer().ToString()]))
	d.sendMessageTmpTCPChan(toSend, newChild.Peer())
	pkg.Dial(newChild.Peer(), d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) sendJoinAsChildMsg(newParent peer.Peer, newParentLat time.Duration, newParentId PeerIDChain, childrenAsArr []peer.Peer) {
	d.logger.Infof("Pending parent: %s", newParent.ToString())
	d.logger.Infof("Joining level %d", d.joinLevel)
	toSend := NewJoinAsChildMessage(childrenAsArr, newParentLat, newParentId)
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
	d.logger.Infof("Lowest Latency Peer: %s , Latency: %d", lowestLatencyPeer.Peer().ToString(), lowestLatencyPeerLat)

	currLevelPeers := d.currLevelPeers[d.joinLevel-1]
	currLevelPeersDone := d.currLevelPeersDone[d.joinLevel-1]

	toPrint := ""
	for _, peer := range currLevelPeers {
		toPrint = toPrint + "; " + peer.ToString()
	}
	d.logger.Infof("d.currLevelPeers (level %d): %s", d.joinLevel-1, toPrint)

	toPrint = ""
	for _, peerDone := range currLevelPeersDone {
		toPrint = toPrint + "; " + peerDone.Peer().ToString()
	}
	d.logger.Infof("d.currLevelPeersDone (level %d): %s:", d.joinLevel-1, toPrint)

	if d.joinLevel > 1 {
		lowestLatencyPeerParent := d.parents[lowestLatencyPeer.Peer().ToString()]
		lowestLatencyPeerParentStats, err := pkg.GetNodeWatcher().GetNodeInfo(lowestLatencyPeerParent.Peer())
		lowestLatencyPeerParentLat := lowestLatencyPeerParentStats.LatencyCalc.CurrValue()

		if err != nil {
			d.logger.Panicf("Node %s parent (%s) was not watched", lowestLatencyPeer.Peer().ToString(), lowestLatencyPeerParent.Peer().ToString())
		}

		d.logger.Infof("Latency to %s : %d", lowestLatencyPeerParent.Peer().ToString(), lowestLatencyPeerLat)

		// Checking to see if i can become parent of currLevelNodes
		if d.parentLatencies[lowestLatencyPeer.Peer().ToString()] > lowestLatencyPeerLat {
			latDifference := lowestLatencyPeerParentLat - lowestLatencyPeerLat
			if latDifference > d.config.MinLatencyImprovementToBecomeParent {
				if len(d.children[lowestLatencyPeerParent.Peer().ToString()]) > d.config.MinGrpSize {
					d.joinAsParentInLevel(currLevelPeersDone, lowestLatencyPeer, lowestLatencyPeerLat, lowestLatencyPeerParent, lowestLatencyPeerParentLat)
					return
				}
			}
		}

		if lowestLatencyPeerParentLat < lowestLatencyPeerLat { // Checking to see if i either progress to lower levels or join curr level
			if d.attemptJoinAsChildInLevel(lowestLatencyPeer, lowestLatencyPeerLat, lowestLatencyPeerParent, lowestLatencyPeerParentLat) {
				return
			}
		}
	}

	d.unwatchPeersInLevel(d.joinLevel, []peer.Peer{lowestLatencyPeer.Peer()})       // unwatch all nodes but the best node
	if len(d.children[lowestLatencyPeer.Peer().ToString()]) < d.config.MinGrpSize { // if the group has enough members, evaluate if can go down
		d.logger.Infof("Joining level %d because nodes in this level have not enough members", d.joinLevel)
		d.myPendingParentInJoin = lowestLatencyPeer.Peer()
		d.sendJoinAsChildMsg(lowestLatencyPeer.Peer(), lowestLatencyPeerLat, lowestLatencyPeer.Chain(), nil)
		return
	}
	d.progressToNextLevel(lowestLatencyPeer.Peer())
}

func (d *DemmonTree) progressToNextLevel(lowestLatencyPeer peer.Peer) {
	d.logger.Infof("Progressing to next level (currLevel=%d), (nextLevel=%d) ", d.joinLevel, d.joinLevel+1)
	d.joinLevel++
	if int(d.joinLevel-1) < len(d.currLevelPeers) {
		for _, c := range d.children[lowestLatencyPeer.ToString()] {
			d.currLevelPeers[d.joinLevel-1][c.Peer().ToString()] = c.Peer()
		}
	} else {
		toAppend := make(map[string]peer.Peer, len(d.children[lowestLatencyPeer.ToString()]))
		for _, c := range d.children[lowestLatencyPeer.ToString()] {
			toAppend[c.Peer().ToString()] = c.Peer()
		}
		d.currLevelPeers = append(d.currLevelPeers, toAppend)
	}

	if int(d.joinLevel-1) < len(d.currLevelPeersDone) {
		d.currLevelPeersDone[d.joinLevel-1] = make(map[string]PeerWithIdChain)
	} else {
		d.currLevelPeersDone = append(d.currLevelPeersDone, make(map[string]PeerWithIdChain))
	}

	for _, p := range d.currLevelPeers[d.joinLevel-1] {
		d.sendMessageAndMeasureLatency(NewJoinMessage(), p)
	}
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
	d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.ToString())
	pkg.SendMessage(toSend, destPeer, d.ID(), []protocol.ID{d.ID()})
}

func (d *DemmonTree) getLowestLatencyPeerInLvlWithDeadline(level uint16, deadline time.Time) (PeerWithIdChain, time.Duration, errors.Error) {
	var lowestLat = time.Duration(math.MaxInt64)
	var lowestLatPeer PeerWithIdChain

	if len(d.currLevelPeersDone[level]) == 0 {
		return nil, time.Duration(math.MaxInt64), errors.NonFatalError(404, "peer collection is empty", protoName)
	}

	for _, currPeerWithChain := range d.currLevelPeersDone[level] {
		currPeer := currPeerWithChain.Peer()
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfoWithDeadline(currPeer, deadline)
		if err != nil {
			d.logger.Warnf("Do not have latency measurement for %s", currPeer.ToString())
			continue
		}
		currLat := nodeStats.LatencyCalc.CurrValue()
		if currLat < lowestLat {
			lowestLatPeer = currPeerWithChain
			lowestLat = currLat
		}
	}
	return lowestLatPeer, lowestLat, nil
}

func (d *DemmonTree) generateChildId() PeerID {
	var peerId PeerID
	occupiedIds := make(map[PeerID]bool, len(d.myChildren)+len(d.myPendingChildrenInJoin))
	for _, child := range d.myChildren {
		occupiedIds[child.ID()] = true
	}

	for _, child := range d.myPendingChildrenInJoin {
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
	info, err := pkg.GetNodeWatcher().GetNodeInfo(peerParent.Peer())
	if err != nil {
		panic(err.Reason())
	}
	peerLat := info.LatencyCalc.CurrValue()
	d.myPendingParentInJoin = peerParent.Peer()
	d.sendJoinAsChildMsg(peerParent.Peer(), peerLat, peerParent.Chain(), []peer.Peer{})
}

func (d *DemmonTree) mergeEViewWith(sample []PeerWithIdChain, neighboursWithoutSenderDescendants []PeerWithIdChain, self PeerWithIdChain, nrPeersToMerge int, senderChain PeerIDChain) []PeerWithIdChain {
	neighboursWithoutSenderDescendantsAndNotInSample := getPeersExcluding(neighboursWithoutSenderDescendants, sample...)
	sampleToSend := getRandSample(nrPeersToMerge-1, neighboursWithoutSenderDescendantsAndNotInSample...)

	if len(d.myIDChain) > 0 {
	outer:
		for i := len(sample) - 1; i >= 0; i-- {
			if len(sample)-i == nrPeersToMerge {
				break
			}
			currPeer := sample[i]

			if currPeer.Peer().Equals(pkg.SelfPeer()) {
				continue
			}

			if d.isNeighbour(currPeer.Peer()) {
				continue
			}

			for _, p := range d.eView {
				if p.Peer().Equals(currPeer.Peer()) {
					continue outer
				}
			}

			if IsDescendant(d.myIDChain, currPeer.Chain()) {
				continue outer
			}

			if len(d.eView) == d.config.MaxPeersInEView { // eView is full
				toRemoveIdx := rand.Intn(len(d.eView))
				d.eView[toRemoveIdx] = sample[i]
			} else {
				d.eView = append(d.eView, sample[i])
			}
		}
	}

	if len(d.myIDChain) > 0 {
		sampleToSend = append(sampleToSend, self)
	}

	sample = append(sampleToSend, sample...)
	if len(sample) > d.config.NrPeersInWalkMessage {
		sample = sample[:d.config.NrPeersInWalkMessage]
	}

	return sample
}

func (d *DemmonTree) getNeighborsAsPeerWithIdChainArray() []PeerWithIdChain {
	possibilitiesToSend := make([]PeerWithIdChain, 0, len(d.myChildren)+len(d.eView)+len(d.mySiblings)+1) // parent and me
	if len(d.myIDChain) > 0 {
		for _, child := range d.myChildren {
			peerWithIdChain := NewPeerWithIdChain(append(d.myIDChain, child.ID()), child.Peer(), child.NrChildren())
			possibilitiesToSend = append(possibilitiesToSend, peerWithIdChain)
		}
		for _, sibling := range d.mySiblings {
			peerWithIdChain := NewPeerWithIdChain(append(d.myIDChain[len(d.myIDChain)-1:], sibling.ID()), sibling.Peer(), sibling.NrChildren())
			possibilitiesToSend = append(possibilitiesToSend, peerWithIdChain)
		}
		if !d.landmark && d.myParent != nil {
			parentAsPeerWithChain := NewPeerWithIdChain(d.myIDChain[(len(d.myIDChain)-1):], d.myParent, uint16(len(d.mySiblings)))
			possibilitiesToSend = append(possibilitiesToSend, parentAsPeerWithChain)
		}
	}
	possibilitiesToSend = append(possibilitiesToSend, d.eView...)
	return possibilitiesToSend
}

func (d *DemmonTree) addParent(newParent peer.Peer, proposedId PeerIDChain, parentLevel uint16, parentLatency time.Duration) {
	d.logger.Warnf("My level changed: (%d -> %d)", d.myLevel, parentLevel+1) // IMPORTANT FOR VISUALIZER
	myNewId := PeerIDChain{}
	d.logger.Warnf("My chain changed: (%+v -> %+v)", d.myIDChain, myNewId)
	d.myPendingParentInJoin = newParent
	if d.myParent != nil {
		toSend := NewDisconnectAsChildMessage()
		d.sendMessage(toSend, d.myParent)
		pkg.GetNodeWatcher().Unwatch(d.myParent, d.ID())
		d.myParent = nil
	}
	d.myIDChain = myNewId
	d.myLevel = parentLevel + 1
	if parentLatency != 0 {
		pkg.GetNodeWatcher().WatchWithInitialLatencyValue(newParent, d.ID(), parentLatency)
	}
	pkg.Dial(newParent, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) isNeighbour(toTest peer.Peer) bool {

	if toTest.Equals(d.myPendingParentInJoin) {
		return true
	}

	if toTest.Equals(d.myParent) {
		return true
	}

	if toTest.Equals(pkg.SelfPeer()) {
		return true
	}

	if _, ok := d.mySiblings[toTest.ToString()]; ok {
		return true
	}

	if _, isPendingChildren := d.myPendingChildrenInJoin[toTest.ToString()]; isPendingChildren {
		return true
	}

	return false
}
