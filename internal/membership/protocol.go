package membership

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
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
	LandmarkRedialTimer               time.Duration
	JoinMessageTimeout                time.Duration
	MaxTimeToProgressToNextLevel      time.Duration
	MaxRetriesJoinMsg                 int
	ParentRefreshTickDuration         time.Duration
	ChildrenRefreshTickDuration       time.Duration
	Landmarks                         []PeerWithId
	MinGrpSize                        uint16
	MaxGrpSize                        uint16
	LimitFirstLevelGroupSize          bool
	RejoinTimerDuration               time.Duration
	NrPeersToConsiderAsParentToAbsorb int
	NrPeersToAbsorb                   uint16

	// maintenance configs

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
	CheckChildenSizeTimerDuration          time.Duration
}

type DemmonTree struct {
	logger *logrus.Logger
	config DemmonTreeConfig

	// node state
	self                PeerWithIdChain
	landmark            bool
	myLevel             uint16
	myGrandParent       peer.Peer
	myParent            peer.Peer
	myChildren          map[string]PeerWithId
	myChildrenLatencies map[string]MeasuredPeersByLat
	mySiblings          map[string]PeerWithId

	// join state
	joinLevel                uint16
	children                 map[string]map[string]PeerWithIdChain
	parents                  map[string]PeerWithIdChain
	currLevelResponseTimeuts map[string]int
	currLevelPeers           []map[string]peer.Peer
	currLevelPeersDone       []map[string]PeerWithIdChain
	retries                  map[string]int

	// improvement service state
	measuringPeers               map[string]bool
	measuredPeers                MeasuredPeersByLat
	eView                        []PeerWithIdChain
	myPendingParentInImprovement MeasuredPeer
}

func NewDemmonTree(config DemmonTreeConfig) protocol.Protocol {
	return &DemmonTree{

		logger: logs.NewLogger(protoName),

		// join state
		parents:                  map[string]PeerWithIdChain{},
		children:                 map[string]map[string]PeerWithIdChain{},
		currLevelResponseTimeuts: make(map[string]int),
		currLevelPeers:           []map[string]peer.Peer{},
		currLevelPeersDone:       []map[string]PeerWithIdChain{},
		joinLevel:                1,
		retries:                  map[string]int{},
		config:                   config,

		// node state
		self:                NewPeerWithIdChain(nil, pkg.SelfPeer(), 0),
		myGrandParent:       nil,
		myParent:            nil,
		myLevel:             math.MaxInt16,
		mySiblings:          map[string]PeerWithId{},
		myChildren:          map[string]PeerWithId{},
		myChildrenLatencies: make(map[string]MeasuredPeersByLat),

		// improvement state
		eView:                        []PeerWithIdChain{},
		measuringPeers:               make(map[string]bool),
		measuredPeers:                MeasuredPeersByLat{},
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
		if pkg.SelfPeer().Equals(landmark) {
			d.landmark = true
			for _, landmark := range d.config.Landmarks {
				if !pkg.SelfPeer().Equals(landmark) {
					d.mySiblings[landmark.ToString()] = landmark
					pkg.Dial(landmark, d.ID(), stream.NewTCPDialer())
				}
			}
			d.self.SetChain(PeerIDChain{landmark.ID()})
			d.logger.Infof("I am landmark, my ID is: %+v", d.self.Chain())
			d.myLevel = 0
			d.myParent = nil
			d.myChildren = map[string]PeerWithId{}
			if d.config.LimitFirstLevelGroupSize {
				pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
			}
			pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
			return
		}
	}

	pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
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

	pkg.RegisterMessageHandler(d.ID(), updateParentMessage{}, d.handleUpdateParentMessage)
	pkg.RegisterMessageHandler(d.ID(), updateChildMessage{}, d.handleUpdateChildMessage)
	pkg.RegisterMessageHandler(d.ID(), absorbMessage{}, d.handleAbsorbMessage)
	pkg.RegisterMessageHandler(d.ID(), disconnectAsChildMessage{}, d.handleDisconnectAsChildMsg)

	// pkg.RegisterMessageHandler(d.ID(), joinAsParentMessage{}, d.handleJoinAsParentMessage)
	// pkg.RegisterMessageHandler(d.ID(), biasedWalkMessage{}, d.handleBiasedWalkMessage)

	pkg.RegisterMessageHandler(d.ID(), randomWalkMessage{}, d.handleRandomWalkMessage)
	pkg.RegisterMessageHandler(d.ID(), walkReplyMessage{}, d.handleWalkReplyMessage)

	pkg.RegisterNotificationHandler(d.ID(), peerMeasuredNotification{}, d.handlePeerMeasuredNotification)

	pkg.RegisterTimerHandler(d.ID(), joinTimerID, d.handleJoinTimer)
	pkg.RegisterTimerHandler(d.ID(), landmarkRedialTimerID, d.handleLandmarkRedialTimer)
	pkg.RegisterTimerHandler(d.ID(), parentRefreshTimerID, d.handleRefreshParentTimer)
	pkg.RegisterTimerHandler(d.ID(), updateChildTimerID, d.handleUpdateChildTimer)
	pkg.RegisterTimerHandler(d.ID(), checkChidrenSizeTimerID, d.handleCheckChildrenSizeTimer)
	pkg.RegisterTimerHandler(d.ID(), externalNeighboringTimerID, d.handleExternalNeighboringTimer)
	pkg.RegisterTimerHandler(d.ID(), measureNewPeersTimerID, d.handleMeasureNewPeersTimer)
	pkg.RegisterTimerHandler(d.ID(), evalMeasuredPeersTimerID, d.handleEvalMeasuredPeersTimer)
	pkg.RegisterTimerHandler(d.ID(), peerJoinMessageResponseTimeoutID, d.handleJoinMessageTimeout)

}

// notification handlers

func (d *DemmonTree) handlePeerMeasuredNotification(notification notification.Notification) {
	peerMeasuredNotification := notification.(peerMeasuredNotification)
	delete(d.measuringPeers, peerMeasuredNotification.peerMeasured.ToString())
	currNodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(peerMeasuredNotification.peerMeasured)
	if err != nil {
		d.logger.Error(err.Reason())
		return
	} else {
		d.measuredPeers = append(d.measuredPeers, NewMeasuredPeer(peerMeasuredNotification.peerMeasured, currNodeStats.LatencyCalc.CurrValue()))
		sort.Sort(d.measuredPeers)

		if len(d.measuredPeers) > d.config.MeasuredPeersSize {
			for i := d.config.MeasuredPeersSize; i < len(d.measuredPeers); i++ {
				d.measuredPeers[i] = nil
			}
			d.measuredPeers = d.measuredPeers[:d.config.MeasuredPeersSize]
		}
	}
	if !d.isNeighbour(peerMeasuredNotification.peerMeasured) {
		pkg.GetNodeWatcher().Unwatch(peerMeasuredNotification.peerMeasured, d.ID())
	}
}

// timer handlers

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {
	// if d.joinLevel == 0 && len(d.currLevelPeers[d.joinLevel]) == 0 {
	// 	d.logger.Info("-------------Rejoining overlay---------------")
	// 	d.joinOverlay()
	// }
	pkg.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
}

func (d *DemmonTree) handleLandmarkRedialTimer(t timer.Timer) {
	redialTimer := t.(*landmarkRedialTimer)
	pkg.Dial(redialTimer.LandmarkToRedial, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) handleRefreshParentTimer(timer timer.Timer) {
	d.logger.Info("RefreshParentTimer trigger")
	for _, child := range d.myChildren {
		toSend := NewUpdateParentMessage(d.myParent, d.myLevel, append(d.self.Chain(), child.ID()), d.getChildrenAsPeerWithIdChainArray(child))
		d.sendMessage(toSend, child)
	}
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
}

func (d *DemmonTree) handleUpdateChildTimer(timer timer.Timer) {
	d.logger.Info("UpdateChildTimer trigger")
	if d.myParent != nil {
		d.sendUpdateChildMessage(d.myParent)
	}
	pkg.RegisterTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
}

func (d *DemmonTree) sendUpdateChildMessage(dest peer.Peer) {
	if len(d.self.Chain()) > 0 {
		measuredSiblings := make(MeasuredPeersByLat, 0, len(d.mySiblings))
		for _, sibling := range d.mySiblings {
			nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(sibling)
			var currLat time.Duration
			if err != nil {
				d.logger.Warnf("Do not have latency measurement for %s", sibling.ToString())
				currLat = math.MaxInt64
			} else {
				currLat = nodeStats.LatencyCalc.CurrValue()
			}

			peerIdChain := append(getParentChainFrom(d.self.Chain()), sibling.ID())
			measuredSiblings = append(measuredSiblings, NewMeasuredPeer(NewPeerWithIdChain(peerIdChain, sibling, sibling.NrChildren()), currLat))
		}
		toSend := NewUpdateChildMessage(d.self.NrChildren(), measuredSiblings)
		d.sendMessage(toSend, dest)
	}
}

func (d *DemmonTree) handleExternalNeighboringTimer(joinTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout))

	d.logger.Info("ExternalNeighboringTimer trigger")

	if d.myParent == nil || len(d.self.Chain()) == 0 { // TODO ask about this
		return
	}

	r := rand.Float32()
	if r > d.config.EmitWalkProbability {
		return
	}

	d.logger.Info("sending walk...")
	possibilitiesToSend := d.getNeighborsAsPeerWithIdChainArray()
	toPrint := ""

	for _, possibility := range possibilitiesToSend {
		toPrint = toPrint + "; " + possibility.ToString()
	}
	d.logger.Infof("d.possibilitiesToSend: %s:", toPrint)

	sample := getRandSample(d.config.NrPeersInWalkMessage-1, possibilitiesToSend...)
	sample = append(sample, d.self)

	r = rand.Float32()

	var msgToSend message.Message
	var peerToSendTo PeerWithIdChain
	// if r < d.config.BiasedWalkProbability {
	// 	msgToSend = NewBiasedWalkMessage(uint16(d.config.RandomWalkTTL), selfPeerWithChain, sample)
	// 	peerToSendTo = getBiasedPeerExcluding(possibilitiesToSend, selfPeerWithChain)
	// } else {
	msgToSend = NewRandomWalkMessage(uint16(d.config.RandomWalkTTL), d.self, sample)
	peerToSendTo = getRandomExcluding(getExcludingDescendantsOf(possibilitiesToSend, d.self.Chain()), d.self)
	// }

	if peerToSendTo == nil {
		d.logger.Error("peerToSendTo is nil")
		return
	}

	_, isChildren := d.myChildren[peerToSendTo.ToString()]
	if isChildren {
		d.sendMessage(msgToSend, peerToSendTo)
		return
	}

	isParent := d.myParent.Equals(peerToSendTo)
	if isParent {
		d.sendMessage(msgToSend, peerToSendTo)
		return
	}

	_, isSibling := d.mySiblings[peerToSendTo.ToString()]
	if isSibling {
		d.sendMessage(msgToSend, peerToSendTo)
		return
	}

	d.sendMessageTmpUDPChan(msgToSend, peerToSendTo)
}

func (d *DemmonTree) handleEvalMeasuredPeersTimer(evalMeasuredPeersTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
	d.logger.Info("EvalMeasuredPeersTimer trigger...")

	if d.self.Chain() != nil || len(d.self.Chain()) == 0 || d.myParent == nil || d.myPendingParentInImprovement != nil {
		return
	}

	r := rand.Float32()
	if r > d.config.AttemptImprovePositionProbability {
		return
	}

	i := 0
	for _, measuredPeer := range d.measuredPeers {
		if !d.isNeighbour(measuredPeer) && !measuredPeer.IsDescendentOf(d.self.Chain()) {
			d.logger.Infof("%s : %s", measuredPeer.ToString(), measuredPeer.MeasuredLatency())
			i++
			d.measuredPeers[i] = measuredPeer
		}
	}
	for j := i; j < len(d.measuredPeers); j++ {
		d.measuredPeers[j] = nil
	}
	d.measuredPeers = d.measuredPeers[:i]

	d.logger.Info("Evaluating measuredPeers peers...")
	if len(d.measuredPeers) == 0 {
		d.logger.Info("returning due to len(d.measuredPeers) == 0")
		return
	}

	for _, measuredPeer := range d.measuredPeers {
		bestPeer := measuredPeer

		if bestPeer.NrChildren() == 0 {
			continue
		}

		d.logger.Infof("Least latency peer with children: %s", bestPeer.ToString())
		parentStats, err := pkg.GetNodeWatcher().GetNodeInfo(d.myParent)
		if err != nil {
			d.logger.Panicf(err.Reason())
			return
		}
		parentLatency := parentStats.LatencyCalc.CurrValue()
		// parent latency is higher than latency to peer
		latencyImprovement := parentLatency - bestPeer.MeasuredLatency()
		if latencyImprovement > d.config.MinLatencyImprovementToImprovePosition {
			d.logger.Infof("Improving position towards: %s", bestPeer.ToString())
			d.logger.Infof("latencyImprovement: %s", latencyImprovement)
			d.logger.Infof("parentLatency: %s", parentLatency)
			d.logger.Infof("bestPeer.MeasuredLatency(): %s", bestPeer.MeasuredLatency())

			pkg.GetNodeWatcher().WatchWithInitialLatencyValue(bestPeer, d.ID(), bestPeer.MeasuredLatency())
			d.myPendingParentInImprovement = bestPeer
			d.sendJoinAsChildMsg(bestPeer, uint16(len(bestPeer.Chain())-1), bestPeer.MeasuredLatency(), bestPeer.Chain(), uint16(d.self.NrChildren()), false)
		} else {
			d.logger.Infof("Not improving position towards best peer: %s", bestPeer.ToString())
			d.logger.Infof("latencyImprovement: %s", latencyImprovement)
			d.logger.Infof("parentLatency: %s", parentLatency)
			d.logger.Infof("bestPeer.MeasuredLatency(): %s", bestPeer.MeasuredLatency())
		}
		return
	}
}

func (d *DemmonTree) handleMeasureNewPeersTimer(measureNewPeersTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration))
	d.logger.Infof("handleMeasureNewPeersTimer trigger")
	if len(d.eView) == 0 {
		return
	}
	nrMeasured := 0
	sample := getRandSample(d.config.NrPeersToMeasure, d.eView...)
	for _, peer := range sample {
		if nrMeasured == d.config.NrPeersToMeasure {
			return
		}

		if _, isMeasuring := d.measuringPeers[peer.ToString()]; isMeasuring {
			continue
		}

		for _, curr := range d.measuredPeers {
			if curr.Equals(peer) {
				continue
			}
		}
		d.logger.Infof("measuring peer: %s", peer.ToString())
		pkg.GetNodeWatcher().Watch(peer, d.ID())
		c := pkg.Condition{
			Repeatable:                false,
			CondFunc:                  func(pkg.NodeInfo) bool { return true },
			EvalConditionTickDuration: 1000 * time.Millisecond,
			Notification:              peerMeasuredNotification{peerMeasured: peer},
			Peer:                      peer,
			EnableGracePeriod:         false,
			ProtoId:                   d.ID(),
		}
		d.logger.Infof("Doing NotifyOnCondition for node %s...", peer.ToString())
		pkg.GetNodeWatcher().NotifyOnCondition(c)
		d.measuringPeers[peer.ToString()] = true
		nrMeasured++
	}
}

func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
	d.logger.Info("handleCheckChildrenSize timer trigger")

	if d.self.NrChildren() == 0 {
		d.logger.Info("d.self.NrChildren() == 0, returning...")
		return
	}

	if d.self.NrChildren() <= d.config.MaxGrpSize {
		d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
		return
	}

	toPrint := ""
	for _, child := range d.myChildren {
		toPrint = toPrint + "; " + child.ToString()
	}
	d.logger.Infof("myChildren: %s:", toPrint)

	childrenAsMeasuredPeers := make(MeasuredPeersByLat, 0, d.self.NrChildren())
	for _, children := range d.myChildren {
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(children)
		if err != nil {
			d.logger.Warnf("Do not have latency measurement for %s", children.ToString())
			continue
		}
		currLat := nodeStats.LatencyCalc.CurrValue()
		childrenAsPeerWithChain := NewPeerWithIdChain(append(d.self.Chain(), children.ID()), children, children.NrChildren())
		childrenAsMeasuredPeers = append(childrenAsMeasuredPeers, NewMeasuredPeer(childrenAsPeerWithChain, currLat))
	}

	toPrint = ""
	for _, possibility := range childrenAsMeasuredPeers {
		toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.ToString(), possibility.MeasuredLatency())
	}

	d.logger.Infof("childrenAsMeasuredPeers: %s", toPrint)
	candidatesToAbsorb := make([]MeasuredPeer, 0, d.config.NrPeersToConsiderAsParentToAbsorb)
	i := 0
	for _, measuredChild := range childrenAsMeasuredPeers {
		if len(candidatesToAbsorb) == d.config.NrPeersToConsiderAsParentToAbsorb {
			break
		}
		if measuredChild.NrChildren()+d.config.NrPeersToAbsorb < d.config.MaxGrpSize {
			candidatesToAbsorb = append(candidatesToAbsorb, measuredChild)
		}
		i++
	}
	sort.Sort(childrenAsMeasuredPeers)
	if len(candidatesToAbsorb) == 0 {
		d.logger.Warn("Have no candidates to send absorb message to")
	}

	toPrint = ""
	for _, possibility := range candidatesToAbsorb {
		toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.ToString(), possibility.MeasuredLatency())
	}
	d.logger.Infof("candidatesToAbsorb: %s:", toPrint)

	var bestCandidate MeasuredPeer
	var bestCandidateChildrenToAbsorb MeasuredPeersByLat
	var bestCandidateLatTotal *big.Int

	for _, candidateToAbsorb := range candidatesToAbsorb {
		currCandidateChildren := MeasuredPeersByLat{}
		candidateToAbsorbSiblingLatencies := d.myChildrenLatencies[candidateToAbsorb.ToString()]

		if len(candidateToAbsorbSiblingLatencies) < int(d.config.NrPeersToAbsorb) {
			continue
		}

		latTotal := &big.Int{}
		sort.Sort(candidateToAbsorbSiblingLatencies)

		toPrint = ""
		for _, possibility := range candidateToAbsorbSiblingLatencies {
			toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.ToString(), possibility.MeasuredLatency())
		}
		d.logger.Infof("candidateToAbsorbSiblingLatencies: %s:", toPrint)

		for i := uint16(0); i < d.config.NrPeersToAbsorb; i++ {
			currCandidateAbsorbtion := candidateToAbsorbSiblingLatencies[i]
			latTotal.Add(latTotal, big.NewInt(int64(currCandidateAbsorbtion.MeasuredLatency())))
			currCandidateChildren = append(currCandidateChildren, currCandidateAbsorbtion)
		}

		if bestCandidateLatTotal == nil {
			bestCandidate = candidateToAbsorb
			bestCandidateChildrenToAbsorb = currCandidateChildren
			bestCandidateLatTotal = latTotal
			continue
		}

		if latTotal.Cmp(bestCandidateLatTotal) == -1 && len(bestCandidateChildrenToAbsorb) == int(d.config.NrPeersToAbsorb) {
			bestCandidate = candidateToAbsorb
			bestCandidateChildrenToAbsorb = currCandidateChildren
			bestCandidateLatTotal = latTotal
			continue
		}
	}

	if bestCandidateChildrenToAbsorb == nil {
		d.logger.Infof("No candidates to absorb")
		return
	}

	toPrint = ""
	for _, possibility := range bestCandidateChildrenToAbsorb {
		toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.ToString(), possibility.MeasuredLatency())
	}
	d.logger.Infof("Sending absorb message with bestCandidateChildrenToAbsorb: %s to: %s", toPrint, bestCandidate.ToString())

	toSend := NewAbsorbMessage(bestCandidateChildrenToAbsorb, bestCandidate)
	d.logger.Infof("Sending absorb message %+v to %s", toSend, bestCandidate.ToString())

	for _, child := range d.myChildren {
		d.sendMessage(toSend, child)
	}

	for _, newGrandChildren := range bestCandidateChildrenToAbsorb {
		d.removeChild(newGrandChildren)
	}

}

// message handlers

func (d *DemmonTree) handleRandomWalkMessage(sender peer.Peer, m message.Message) {
	randWalkMsg := m.(randomWalkMessage)
	d.logger.Infof("Got randomWalkMessage: %+v from %s", randWalkMsg, sender.ToString())
	toPrint := ""
	for _, peer := range randWalkMsg.Sample {
		toPrint = toPrint + "; " + peer.ToString()
	}
	d.logger.Infof("randomWalkMessage peers: %s", toPrint)

	hopsTaken := d.config.RandomWalkTTL - int(randWalkMsg.TTL)
	if hopsTaken < d.config.NrHopsToIgnoreWalk {
		randWalkMsg.TTL--
		neighbours := d.getNeighborsAsPeerWithIdChainArray()
		neighboursWithoutSenderDescendants := getExcludingDescendantsOf(neighbours, randWalkMsg.Sender.Chain())
		p := getRandomExcluding(neighboursWithoutSenderDescendants, randWalkMsg.Sender)
		if p == nil {
			sampleToSend := d.mergeEViewWith(randWalkMsg.Sample, neighboursWithoutSenderDescendants, d.self, d.config.NrPeersToMergeInWalkSample)
			walkReply := NewWalkReplyMessage(sampleToSend)
			d.sendMessageTmpTCPChan(walkReply, randWalkMsg.Sender)
			return
		}
		if d.isNeighbour(p) {
			d.sendMessage(randWalkMsg, p)
		} else {
			d.sendMessageTmpUDPChan(randWalkMsg, p)
		}
		return
	}

	if randWalkMsg.TTL > 0 {
		neighbours := d.getNeighborsAsPeerWithIdChainArray()
		neighboursWithoutSenderDescendants := getExcludingDescendantsOf(neighbours, randWalkMsg.Sender.Chain())
		// toPrint := ""
		// for _, peer := range neighboursWithoutSenderDescendants {
		// 	d.logger.Infof("%+v", peer)
		// 	// d.logger.Infof("neighboursWithoutSenderDescendants peer: %s", peer.ToString())
		// }
		// d.logger.Infof("neighboursWithoutSenderDescendants: %s", toPrint)
		sampleToSend := d.mergeEViewWith(randWalkMsg.Sample, neighboursWithoutSenderDescendants, d.self, d.config.NrPeersToMergeInWalkSample)
		randWalkMsg.TTL--
		p := getRandomExcluding(neighboursWithoutSenderDescendants, randWalkMsg.Sender)
		if p == nil {
			walkReply := NewWalkReplyMessage(sampleToSend)
			d.sendMessageTmpTCPChan(walkReply, randWalkMsg.Sender)
			return
		}
		randWalkMsg.Sample = sampleToSend
		if d.isNeighbour(p) {
			d.sendMessage(randWalkMsg, p)
		} else {
			d.sendMessageTmpUDPChan(randWalkMsg, p)
		}
		return
	}

	// TTL == 0
	if randWalkMsg.TTL == 0 {
		possibilitiesToSend := d.getNeighborsAsPeerWithIdChainArray()
		neighboursWithoutSenderDescendants := getExcludingDescendantsOf(possibilitiesToSend, randWalkMsg.Sender.Chain())
		// toPrint := ""
		// for _, peer := range neighboursWithoutSenderDescendants {
		// 	d.logger.Infof("%+v", peer)
		// 	d.logger.Infof("neighboursWithoutSenderDescendants peer: %s", peer.ToString())
		// }
		// d.logger.Infof("neighboursWithoutSenderDescendants: %s", toPrint)
		sampleToSend := d.mergeEViewWith(randWalkMsg.Sample, neighboursWithoutSenderDescendants, d.self, d.config.NrPeersToMergeInWalkSample)
		d.sendMessage(NewWalkReplyMessage(sampleToSend), randWalkMsg.Sender)
		return
	}
}

func (d *DemmonTree) handleWalkReplyMessage(sender peer.Peer, m message.Message) {
	walkReply := m.(walkReplyMessage)
	d.logger.Infof("Got walkReplyMessage: %+v from %s", walkReply, sender.ToString())
	sample := walkReply.Sample
	if len(d.self.Chain()) > 0 {
	outer:
		for i := 0; i < len(sample); i++ {
			currPeer := sample[i]

			if currPeer.IsDescendentOf(d.self.Chain()) {
				continue outer
			}

			if currPeer.Equals(pkg.SelfPeer()) {
				continue outer
			}

			if d.isNeighbour(currPeer) {
				continue outer
			}

			for _, p := range d.eView {
				if p.Equals(currPeer) {
					currPeer.SetChain(p.Chain())
					currPeer.SetChildrenNr(p.NrChildren())
					continue outer
				}
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
	absorbMessage := m.(absorbMessage)

	d.logger.Infof("Got absorbMessage: %+v from %s", m, sender.ToString())

	toPrint := ""
	for _, peerToAbsorb := range absorbMessage.PeersToAbsorb {
		toPrint = toPrint + "; " + peerToAbsorb.ToString()
	}
	d.logger.Infof("peerAbsorber in absorbMessage: %s:", absorbMessage.PeerAbsorber.ToString())
	d.logger.Infof("peersToAbsorb in absorbMessage: %s:", toPrint)

	if pkg.SelfPeer().Equals(absorbMessage.PeerAbsorber) {
		for _, aux := range absorbMessage.PeersToAbsorb {
			if newChildren, ok := d.mySiblings[aux.ToString()]; ok {
				delete(d.mySiblings, newChildren.ToString())
				d.myChildren[newChildren.ToString()] = newChildren
				d.self.SetChildrenNr(uint16(len(d.myChildren)))
			} else {
				d.addChild(aux, true, true, 0, 0)
			}
		}
		return
	}

	for _, peerToAbsorb := range absorbMessage.PeersToAbsorb {
		if pkg.SelfPeer().Equals(peerToAbsorb) {
			newParent, ok := d.mySiblings[absorbMessage.PeerAbsorber.ToString()]
			parentChain := getParentChainFrom(d.self.Chain())
			if !ok {
				d.addParent(absorbMessage.PeerAbsorber, d.myParent, parentChain, d.myLevel, true, 0, true, false, true)
			} else {
				d.addParent(newParent, sender, parentChain, d.myLevel, true, 0, true, true, false)
			}
			delete(d.mySiblings, absorbMessage.PeerAbsorber.ToString())
			for _, sibling := range d.mySiblings {
				for _, p := range absorbMessage.PeersToAbsorb {
					if p.Equals(sibling) {
						d.removeSibling(sibling)
						continue
					}
				}
			}
			return
		}
	}

	for _, p := range absorbMessage.PeersToAbsorb {
		d.removeSibling(p)
	}
}

func (d *DemmonTree) handleDisconnectAsChildMsg(sender peer.Peer, m message.Message) {
	dacMsg := m.(disconnectAsChildMessage)
	d.logger.Infof("got DisconnectAsChildMsg %+v from %s", dacMsg, sender.ToString())
	d.removeChild(sender)
}

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {

	aux := make([]PeerWithId, d.self.NrChildren())
	i := 0
	for _, c := range d.myChildren {
		aux[i] = c
		i++
	}

	if d.landmark {
		toSend := NewJoinReplyMessage(aux, d.myLevel, d.self.Chain())
		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	toSend := NewJoinReplyMessage(aux, d.myLevel, d.self.Chain())
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinMessageTimeout(timer timer.Timer) {
	peer := timer.(*peerJoinMessageResponseTimeout).Peer
	d.logger.Warnf("Join message timeout from %s", peer.ToString())

	delete(d.currLevelPeers[d.joinLevel], peer.ToString())
	delete(d.currLevelPeersDone[d.joinLevel], peer.ToString())

	if len(d.currLevelPeers[d.joinLevel]) == 0 {
		d.logger.Warn("Have no more peers in current level, falling back to parent")
		d.fallbackToParentInJoin(peer)
		return
	}

	if d.canProgressToNextStep() {
		d.progressToNextStep()
	}
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(joinReplyMessage)

	d.logger.Infof("Got joinReply: %+v from %s", replyMsg, sender.ToString())

	if d.joinLevel == math.MaxInt16 {
		d.logger.Errorf("Got joinReply: %+v but already joined... %s", replyMsg, sender.ToString())
		return
	}
	if _, ok := d.currLevelPeers[d.joinLevel][sender.ToString()]; ok {
		pkg.CancelTimer(d.currLevelResponseTimeuts[sender.ToString()])
	} else {
		d.logger.Errorf("Got joinReply: %+v from timed out peer... %s", replyMsg, sender.ToString())
		return
	}

	if (d.joinLevel != replyMsg.Level) || (d.joinLevel > 1 && !d.parents[sender.ToString()].IsParentOf(replyMsg.IdChain)) {

		if d.joinLevel != replyMsg.Level {
			d.logger.Warnf("Discarding joinReply %+v from %s because joinLevel is not mine: %d", replyMsg, sender.ToString(), d.joinLevel)
		}

		if d.joinLevel > 1 && !d.parents[sender.ToString()].IsParentOf(replyMsg.IdChain) {
			d.logger.Warnf("Discarding joinReply %+v from %s because node does not have the same parent... should be: %s", replyMsg, sender.ToString(), d.parents[sender.ToString()].ToString())
		}

		// discard old repeated messages
		delete(d.currLevelPeers[d.joinLevel], sender.ToString())
		delete(d.currLevelPeersDone[d.joinLevel], sender.ToString())

		if len(d.currLevelPeers[d.joinLevel]) == 0 {
			d.logger.Warn("Have no more peers in current level, falling back to parent")
			d.fallbackToParentInJoin(sender)
			return
		}

		if d.canProgressToNextStep() {
			d.progressToNextStep()
		}
		return
	}

	peerWithIdChain := NewPeerWithIdChain(replyMsg.IdChain, sender, uint16(len(replyMsg.Children)))
	d.currLevelPeersDone[d.joinLevel][sender.ToString()] = peerWithIdChain
	d.children[sender.ToString()] = make(map[string]PeerWithIdChain, len(replyMsg.Children))
	for _, c := range replyMsg.Children {
		d.children[sender.ToString()][c.ToString()] = NewPeerWithIdChain(append(replyMsg.IdChain, c.ID()), c, 0)
	}

	for _, children := range replyMsg.Children {
		d.parents[children.ToString()] = peerWithIdChain
	}

	if d.canProgressToNextStep() {
		d.progressToNextStep()
	}
}

// func (d *DemmonTree) handleJoinAsParentMessage(sender peer.Peer, m message.Message) {
// 	japMsg := m.(joinAsParentMessage)
// 	d.logger.Infof("got JoinAsParentMessage %+v from %s", japMsg, sender.ToString())
// 	if !ChainsEqual(d.myIDChain, japMsg.ExpectedId) {
// 		d.logger.Warnf("Discarding parent %s because it was trying to optimize with previous position", sender.ToString())
// 		return
// 	}
// 	d.addParent(sender, japMsg.ProposedId, japMsg.Level, 0, false, false)
// 	d.mergeSiblingsWith(japMsg.Siblings)
// }

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {
	jacMsg := m.(joinAsChildMessage)
	d.logger.Infof("got JoinAsChildMessage %+v from %s", jacMsg, sender.ToString())

	if !jacMsg.Urgent && !d.landmark {
		if len(d.self.Chain()) == 0 && !ChainsEqual(jacMsg.ExpectedId, d.self.Chain()) || d.self.NrChildren() == 0 {
			toSend := NewJoinAsChildMessageReply(false, nil, 0, nil, nil)
			d.logger.Infof("denying joinAsChildReply from %s", sender.ToString())
			d.sendMessageTmpTCPChan(toSend, sender)
			return
		}
	}

	newChildId := d.addChild(sender, true, true, jacMsg.MeasuredLatency, jacMsg.NrChildren)
	childrenToSend := make([]PeerWithId, 0, d.self.NrChildren())
	for _, child := range d.myChildren {
		childrenToSend = append(childrenToSend, child)
	}
	toSend := NewJoinAsChildMessageReply(true, append(d.self.Chain(), newChildId), d.myLevel, childrenToSend, d.myGrandParent)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinAsChildMessageReply(sender peer.Peer, m message.Message) {
	japrMsg := m.(joinAsChildMessageReply)
	d.logger.Infof("got JoinAsChildMessageReply %+v from %s", japrMsg, sender.ToString())

	// special case for parent in improvement
	if d.myPendingParentInImprovement != nil && sender.Equals(d.myPendingParentInImprovement) {
		if japrMsg.Accepted {
			d.addParent(sender, japrMsg.GrandParent, japrMsg.ProposedId, japrMsg.ParentLevel, true, d.myPendingParentInImprovement.MeasuredLatency(), true, true, true)
		}
		d.myPendingParentInImprovement = nil
		return
	}

	if japrMsg.Accepted {
		d.addParent(sender, japrMsg.GrandParent, japrMsg.ProposedId, japrMsg.ParentLevel, false, 0, false, true, true)
	} else {
		d.fallbackToParentInJoin(sender)
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

	if !ChainsEqual(upMsg.ProposedIdChain, d.self.Chain()) {
		d.logger.Warnf("My chain changed: (%+v -> %+v)", d.self.Chain(), upMsg.ProposedIdChain)
	}

	d.self.SetChain(upMsg.ProposedIdChain)
	d.myLevel = upMsg.ParentLevel + 1
	d.myGrandParent = upMsg.GrandParent
	d.mergeSiblingsWith(upMsg.Siblings)
}

func (d *DemmonTree) handleUpdateChildMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(updateChildMessage)
	d.logger.Infof("got updateChildMessage %+v from %s", m, sender.ToString())
	child, ok := d.myChildren[sender.ToString()]
	if !ok {
		d.logger.Errorf("got updateChildMessage %+v from not my children, or my pending children: %s", m, sender.ToString())
		return
	}

	toPrint := ""
	for _, measuredPeer := range upMsg.SiblingLatencies {
		toPrint = toPrint + "; " + measuredPeer.Sibling.ToString()
	}

	d.logger.Infof("SiblingLatencies in updateChildMessage: %s:", toPrint)

	child.SetChildrenNr(upMsg.NChildren)
	measuredPeersByLat := MeasuredPeersByLat{}
	for _, measuredChild := range upMsg.SiblingLatencies {
		if child, ok := d.myChildren[measuredChild.Sibling.ToString()]; ok {
			measuredChildWithIdChain := NewPeerWithIdChain(append(d.self.Chain(), child.ID()), child, child.NrChildren())
			measuredPeersByLat = append(measuredPeersByLat, NewMeasuredPeer(measuredChildWithIdChain, measuredChild.Lat))
		}
	}
	d.myChildrenLatencies[child.ToString()] = measuredPeersByLat
}

func (d *DemmonTree) InConnRequested(peer peer.Peer) bool {

	if d.myParent != nil && d.myParent.Equals(peer) {
		d.logger.Infof("My parent dialed me")
		return true
	}

	_, isChildren := d.myChildren[peer.ToString()]
	if isChildren {
		d.logger.Infof("My children (%s) dialed me ", peer.ToString())
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
		d.sendUpdateChildMessage(d.myParent)
		return true
	}

	child, isChildren := d.myChildren[peer.ToString()]
	if isChildren {
		d.logger.Infof("Dialed children with success: %s", peer.ToString())
		toSend := NewUpdateParentMessage(d.myParent, d.myLevel, append(d.self.Chain(), child.ID()), d.getChildrenAsPeerWithIdChainArray(child))
		d.sendMessage(toSend, child)
		return true
	}

	_, isSibling := d.mySiblings[peer.ToString()]
	if isSibling {
		d.logger.Infof("Dialed sibling with success: %s", peer.ToString())
		return true
	}

	d.logger.Infof("d.myParent: %s", d.myParent)
	d.logger.Infof("d.myChildren: %+v", d.myChildren)
	d.logger.Errorf("Dialed unknown peer: %s", peer.ToString())
	return false
}

func (d *DemmonTree) DialFailed(peer peer.Peer) {
	d.logger.Errorf("Failed to dial %s", peer.ToString())

	if d.landmark {
		for _, landmark := range d.config.Landmarks {
			if landmark.Equals(peer) {
				pkg.RegisterTimer(d.ID(), NewLandmarkRedialTimer(d.config.LandmarkRedialTimer, landmark))
			}
		}
	}

}

func (d *DemmonTree) OutConnDown(peer peer.Peer) {
	// TODO
	d.logger.Errorf("peer down %s", peer.ToString())

	if peer.Equals(d.myParent) {
		d.logger.Warnf("Parent down %s", peer.ToString())
		d.myParent = nil
		pkg.GetNodeWatcher().Unwatch(peer, d.ID())

		if d.myGrandParent != nil {
			d.logger.Warnf("Falling back to grandparent %s", d.myGrandParent.ToString())
			d.sendJoinAsChildMsg(d.myGrandParent, d.myLevel-2, 0, nil, d.self.NrChildren(), true)
			return
		}
		d.logger.Warnf("Grandparent is nil... falling back to measured peers")
		d.fallbackToMeasuredPeers()
		return
	}

	if child, isChildren := d.myChildren[peer.ToString()]; isChildren {
		d.logger.Warnf("Child down %s", peer.ToString())
		d.removeChild(child)
		return
	}

	if _, isSibling := d.mySiblings[peer.ToString()]; isSibling {
		d.logger.Warnf("Sibling down %s", peer.ToString())
		delete(d.mySiblings, peer.ToString())
		pkg.GetNodeWatcher().Unwatch(peer, d.ID())
		return
	}

	pkg.GetNodeWatcher().Unwatch(peer, d.ID())
	d.logger.Panicf("Unknown peer down %s", peer.ToString())
}

func (d *DemmonTree) MessageDelivered(message message.Message, peer peer.Peer) {
	d.logger.Infof("Message %+v delivered to: %s", message, peer.ToString())
	switch message.(type) {
	case disconnectAsChildMessage:
		pkg.Disconnect(d.ID(), peer)
	}
}

func (d *DemmonTree) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	d.logger.Errorf("Message %+v failed to deliver to: %s because: %s", message, peer.ToString(), error.Reason())
	switch message := message.(type) {
	case joinMessage:
		_, ok := d.retries[peer.ToString()]
		if !ok {
			d.retries[peer.ToString()] = 0
		}
		if d.joinLevel != math.MaxInt16 {
			d.retries[peer.ToString()]++
			if d.retries[peer.ToString()] >= d.config.MaxRetriesJoinMsg {
				d.logger.Warnf("Deleting peer %s from currLevelPeers because it exceeded max retries (%d)", peer.ToString(), d.config.MaxRetriesJoinMsg)
				delete(d.currLevelPeers[d.joinLevel], peer.ToString())
				delete(d.retries, peer.ToString())
				if d.canProgressToNextStep() {
					d.progressToNextStep()
				}
				return
			}
			d.sendMessageAndMeasureLatency(message, peer)
		}

	case joinAsChildMessage:

		if peer.Equals(d.myGrandParent) { // message was a fault-tolerance message to grandparent
			d.myGrandParent = nil
			d.fallbackToMeasuredPeers()
			return
		}

		if d.joinLevel != math.MaxInt16 { // message was lost in join
			d.fallbackToParentInJoin(peer)
			return
		}

	case disconnectAsChildMessage:
		pkg.Disconnect(d.ID(), peer)
	}
}

func (d *DemmonTree) joinOverlay() {
	nrLandmarks := len(d.config.Landmarks)
	d.currLevelPeersDone = []map[string]PeerWithIdChain{make(map[string]PeerWithIdChain, nrLandmarks)} // start level 1
	d.currLevelPeers = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)}                 // start level 1
	d.joinLevel = 0
	d.logger.Infof("Landmarks:")
	for i, landmark := range d.config.Landmarks {
		d.logger.Infof("%d :%s", i, landmark.ToString())
		d.currLevelPeers[d.joinLevel][landmark.ToString()] = landmark
		joinMsg := joinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark)
	}
}

func (d *DemmonTree) addParent(newParent peer.Peer, newGrandParent peer.Peer, myNewId PeerIDChain, parentLevel uint16, watchNewParent bool, parentLatency time.Duration, disconnectFromParent, sendDisconnectMsg, dialParent bool) {
	d.logger.Warnf("My level changed: (%d -> %d)", d.myLevel, parentLevel+1) // IMPORTANT FOR VISUALIZER
	d.logger.Warnf("My chain changed: (%+v -> %+v)", d.self.Chain(), myNewId)

	if d.myParent != nil && !d.myParent.Equals(newParent) {
		if disconnectFromParent {
			if sendDisconnectMsg {
				toSend := NewDisconnectAsChildMessage()
				d.sendMessage(toSend, d.myParent)
			} else {
				pkg.Disconnect(d.ID(), d.myParent)
			}
		}
	}

	d.myGrandParent = newGrandParent
	d.myParent = newParent
	d.self.SetChain(myNewId)
	d.myLevel = parentLevel + 1
	d.joinLevel = math.MaxUint16

	if watchNewParent {
		if parentLatency != 0 {
			pkg.GetNodeWatcher().WatchWithInitialLatencyValue(newParent, d.ID(), parentLatency)
		} else {
			pkg.GetNodeWatcher().Watch(newParent, d.ID())
		}
	}

	if dialParent {
		pkg.Dial(newParent, d.ID(), stream.NewTCPDialer())
	} else {
		d.logger.Infof("Dialed parent with success, parent: %s", d.myParent.ToString()) // just here to visualize
	}

}

func (d *DemmonTree) addChild(newChild peer.Peer, dialChild bool, watchChild bool, childrenLatency time.Duration, nrChildren uint16) PeerID {
	proposedId := d.generateChildId()
	if watchChild {
		if childrenLatency != 0 {
			pkg.GetNodeWatcher().WatchWithInitialLatencyValue(newChild, d.ID(), childrenLatency)
		} else {
			pkg.GetNodeWatcher().Watch(newChild, d.ID())
		}
	}
	d.myChildren[newChild.ToString()] = NewPeerWithId(proposedId, newChild, nrChildren)
	if dialChild {
		pkg.Dial(newChild, d.ID(), stream.NewTCPDialer())
	}
	d.self.SetChildrenNr(uint16(len(d.myChildren)))
	return proposedId
}

func (d *DemmonTree) removeChild(toRemove peer.Peer) {
	pkg.Disconnect(d.ID(), toRemove)
	delete(d.myChildrenLatencies, toRemove.ToString())
	delete(d.myChildren, toRemove.ToString())
	pkg.GetNodeWatcher().Unwatch(toRemove, d.ID())
	d.self.SetChildrenNr(uint16(len(d.myChildren)))
}

func (d *DemmonTree) sendJoinAsChildMsg(newParent peer.Peer, parentLevel uint16, newParentLat time.Duration, newParentId PeerIDChain, nrChildren uint16, urgent bool) {
	d.logger.Infof("Pending parent: %s", newParent.ToString())
	d.logger.Infof("Joining level %d", parentLevel+1)
	toSend := NewJoinAsChildMessage(newParentLat, newParentId, nrChildren, urgent)
	d.sendMessageTmpTCPChan(toSend, newParent)
}

func (d *DemmonTree) unwatchPeersInLevel(level uint16, exclusions ...peer.Peer) {
	for _, currPeer := range d.currLevelPeers[level] {
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

	if len(d.currLevelPeers[d.joinLevel]) == 0 {
		d.logger.Infof("Cannot progress because len(d.currLevelPeers[d.joinLevel]) == 0 ")
		res = false
		return
	}

	for _, p := range d.currLevelPeers[d.joinLevel] {
		_, ok := d.currLevelPeersDone[d.joinLevel][p.ToString()]
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
	currLevelPeersDone := d.GetPeersInLevelByLat(d.joinLevel, time.Now().Add(d.config.MaxTimeToProgressToNextLevel))
	lowestLatencyPeer := currLevelPeersDone[0]

	d.logger.Infof("Lowest Latency Peer: %s , Latency: %d", lowestLatencyPeer.ToString(), lowestLatencyPeer.MeasuredLatency())
	toPrint := ""
	for _, peerDone := range currLevelPeersDone {
		toPrint = toPrint + "; " + peerDone.ToString()
	}
	d.logger.Infof("d.currLevelPeersDone (level %d): %s:", d.joinLevel, toPrint)

	if d.joinLevel == 0 {
		if lowestLatencyPeer.NrChildren() < d.config.MinGrpSize {
			d.sendJoinAsChildMsg(lowestLatencyPeer, d.joinLevel, lowestLatencyPeer.MeasuredLatency(), lowestLatencyPeer.Chain(), 0, false)
			return
		} else {
			d.unwatchPeersInLevel(d.joinLevel, lowestLatencyPeer)
			d.progressToNextLevel(lowestLatencyPeer)
			return
		}
	}

	if lowestLatencyPeer.NrChildren() >= d.config.MinGrpSize {
		d.unwatchPeersInLevel(d.joinLevel - 1)
		d.unwatchPeersInLevel(d.joinLevel, lowestLatencyPeer)
		d.progressToNextLevel(lowestLatencyPeer)
		return
	} else {
		lowestLatencyPeerParent := d.parents[lowestLatencyPeer.ToString()]
		d.logger.Infof("Joining level %d because nodes in this level have not enough members", d.joinLevel)
		info, err := pkg.GetNodeWatcher().GetNodeInfo(lowestLatencyPeerParent)
		if err != nil {
			d.logger.Panic(err.Reason())
		}
		parentLatency := info.LatencyCalc.CurrValue()
		if parentLatency < lowestLatencyPeer.MeasuredLatency() {
			d.unwatchPeersInLevel(d.joinLevel-1, lowestLatencyPeerParent)
			d.sendJoinAsChildMsg(lowestLatencyPeerParent, d.joinLevel-1, info.LatencyCalc.CurrValue(), lowestLatencyPeerParent.Chain(), 0, false)
			return
		} else {
			d.sendJoinAsChildMsg(lowestLatencyPeer, d.joinLevel, lowestLatencyPeer.MeasuredLatency(), lowestLatencyPeer.Chain(), 0, false)
			return
		}
	}
}

func (d *DemmonTree) progressToNextLevel(lowestLatencyPeer peer.Peer) {
	d.logger.Infof("Progressing to next level (currLevel=%d), (nextLevel=%d) ", d.joinLevel, d.joinLevel+1)
	d.joinLevel++
	if int(d.joinLevel) < len(d.currLevelPeers) {
		for _, c := range d.children[lowestLatencyPeer.ToString()] {
			d.currLevelPeers[d.joinLevel][c.ToString()] = c
		}
	} else {
		toAppend := make(map[string]peer.Peer, len(d.children[lowestLatencyPeer.ToString()]))
		for _, c := range d.children[lowestLatencyPeer.ToString()] {
			toAppend[c.ToString()] = c
		}
		d.currLevelPeers = append(d.currLevelPeers, toAppend)
	}

	if int(d.joinLevel) < len(d.currLevelPeersDone) {
		d.currLevelPeersDone[d.joinLevel] = make(map[string]PeerWithIdChain)
	} else {
		d.currLevelPeersDone = append(d.currLevelPeersDone, make(map[string]PeerWithIdChain))
	}

	d.currLevelResponseTimeuts = make(map[string]int)
	for _, p := range d.currLevelPeers[d.joinLevel] {
		d.currLevelResponseTimeuts[p.ToString()] = pkg.RegisterTimer(d.ID(), NewJoinMessageResponseTimeout(d.config.JoinMessageTimeout, p))
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

func (d *DemmonTree) GetPeersInLevelByLat(level uint16, deadline time.Time) MeasuredPeersByLat {
	if len(d.currLevelPeersDone[level]) == 0 {
		return MeasuredPeersByLat{}
	}
	measuredPeersInLvl := make(MeasuredPeersByLat, 0, len(d.currLevelPeersDone[level]))
	for _, currPeerWithChain := range d.currLevelPeersDone[level] {
		currPeer := currPeerWithChain
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfoWithDeadline(currPeer, deadline)
		if err != nil {
			d.logger.Warnf("Do not have latency measurement for %s", currPeer.ToString())
			continue
		}
		currLat := nodeStats.LatencyCalc.CurrValue()
		measuredPeersInLvl = append(measuredPeersInLvl, NewMeasuredPeer(currPeerWithChain, currLat))
	}
	sort.Sort(measuredPeersInLvl)
	return measuredPeersInLvl
}

func (d *DemmonTree) generateChildId() PeerID {
	var peerId PeerID
	occupiedIds := make(map[PeerID]bool, d.self.NrChildren())
	for _, child := range d.myChildren {
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

func (d *DemmonTree) fallbackToParentInJoin(node peer.Peer) {
	peerParent, ok := d.parents[node.ToString()]
	if !ok {
		d.logger.Panicf("Peer %s has no parent", node.ToString())
	}
	info, err := pkg.GetNodeWatcher().GetNodeInfo(peerParent)
	if err != nil {
		panic(err.Reason())
	}
	peerLat := info.LatencyCalc.CurrValue()
	d.unwatchPeersInLevel(d.joinLevel)
	d.joinLevel--
	d.sendJoinAsChildMsg(peerParent, d.joinLevel, peerLat, peerParent.Chain(), 0, true)
}

func (d *DemmonTree) fallbackToMeasuredPeers() {

	panic("Should have fallen back to grandparent")

}

func (d *DemmonTree) mergeEViewWith(sample []PeerWithIdChain, neighboursWithoutSenderDescendants []PeerWithIdChain, self PeerWithIdChain, nrPeersToMerge int) []PeerWithIdChain {
	if len(d.self.Chain()) > 0 {
		neighboursWithoutSenderDescendants = append(neighboursWithoutSenderDescendants, self)
	}
	neighboursWithoutSenderDescendantsAndNotInSample := getPeersExcluding(neighboursWithoutSenderDescendants, sample...)
	sampleToSend := getRandSample(nrPeersToMerge-1, neighboursWithoutSenderDescendantsAndNotInSample...)

	if len(d.self.Chain()) > 0 {
	outer:
		for i := len(sample) - 1; i >= 0; i-- {

			if len(sample)-i == nrPeersToMerge {
				break
			}

			currPeer := sample[i]

			if currPeer.Equals(pkg.SelfPeer()) {
				currPeer.SetChain(d.self.Chain())
				currPeer.SetChildrenNr(uint16(d.self.NrChildren()))
				continue
			}

			if d.isNeighbour(currPeer) {
				continue
			}

			for _, p := range d.eView {
				if p.Equals(currPeer) {
					continue outer
				}
			}

			for _, p := range d.eView {
				if p.Equals(currPeer) {
					p.SetChildrenNr(currPeer.NrChildren())
					p.SetChain(currPeer.Chain())
					continue outer
				}
			}

			if currPeer.IsDescendentOf(d.self.Chain()) {
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

	sample = append(sampleToSend, sample...)
	if len(sample) > d.config.NrPeersInWalkMessage {
		for i := d.config.NrPeersInWalkMessage; i < len(sample); i++ {
			sample[i] = nil
		}
		sample = sample[:d.config.NrPeersInWalkMessage]
	}

	return sample
}

func (d *DemmonTree) mergeSiblingsWith(newSiblings []PeerWithId) {
	for _, newSibling := range newSiblings {
		if pkg.SelfPeer().Equals(newSibling) {
			continue
		}
		sibling, ok := d.mySiblings[newSibling.ToString()]
		if !ok {
			d.addSibling(newSibling)
			continue
		}
		sibling.SetChildrenNr(newSibling.NrChildren())
	}
}

func (d *DemmonTree) addSibling(newSibling PeerWithId) {
	d.mySiblings[newSibling.ToString()] = newSibling
	pkg.GetNodeWatcher().Watch(newSibling, d.ID())
	pkg.Dial(newSibling, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) removeSibling(toRemove peer.Peer) {
	delete(d.mySiblings, toRemove.ToString())
	pkg.GetNodeWatcher().Unwatch(toRemove, d.ID())
	pkg.Disconnect(d.ID(), toRemove)
}

func (d *DemmonTree) getChildrenAsPeerWithIdChainArray(exclusion PeerWithId) []PeerWithId {
	toReturn := make([]PeerWithId, 0, d.self.NrChildren()-1)
	for _, child := range d.myChildren {
		if !exclusion.Equals(child) {
			toReturn = append(toReturn, child)
		}
	}
	return toReturn
}

func (d *DemmonTree) getNeighborsAsPeerWithIdChainArray() []PeerWithIdChain {
	possibilitiesToSend := make([]PeerWithIdChain, 0, int(d.self.NrChildren())+len(d.eView)+len(d.mySiblings)+1) // parent and me
	if len(d.self.Chain()) > 0 {
		parentChain := d.self.Chain()[(len(d.self.Chain()) - 1):]
		for _, child := range d.myChildren {
			peerWithIdChain := NewPeerWithIdChain(append(d.self.Chain(), child.ID()), child, child.NrChildren())
			possibilitiesToSend = append(possibilitiesToSend, peerWithIdChain)
		}
		for _, sibling := range d.mySiblings {
			peerWithIdChain := NewPeerWithIdChain(append(parentChain, sibling.ID()), sibling, sibling.NrChildren())
			possibilitiesToSend = append(possibilitiesToSend, peerWithIdChain)
		}
		if !d.landmark && d.myParent != nil {
			parentAsPeerWithChain := NewPeerWithIdChain(parentChain, d.myParent, uint16(len(d.mySiblings)))
			possibilitiesToSend = append(possibilitiesToSend, parentAsPeerWithChain)
		}
	}
	possibilitiesToSend = append(possibilitiesToSend, d.eView...)
	return possibilitiesToSend
}

func (d *DemmonTree) isNeighbour(toTest peer.Peer) bool {
	if toTest.Equals(pkg.SelfPeer()) {
		panic("is self")
	}

	if toTest.Equals(d.myParent) {
		return true
	}

	if _, ok := d.mySiblings[toTest.ToString()]; ok {
		return true
	}

	if _, isChildren := d.myChildren[toTest.ToString()]; isChildren {
		return true
	}
	return false
}

// VERSION where nodes n nodes  with lowest latency are picked, and the one with least overall latency to its siblings get picked
// func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
// 	pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
// 	d.logger.Info("handleCheckChildrenSize timer trigger")

// 	if d.self.NrChildren() == 0 {
// 		d.logger.Info("d.self.NrChildren() == 0, returning...")
// 		return
// 	}

// 	if d.self.NrChildren() < int(d.config.MaxGrpSize) {
// 		d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
// 		return
// 	}

// 	toPrint := ""
// 	for _, child := range d.myChildren {
// 		toPrint = toPrint + "; " + child.ToString()
// 	}
// 	d.logger.Infof("myChildren: %s:", toPrint)

// 	childrenAsMeasuredPeers := make(MeasuredPeersByLat, 0, d.self.NrChildren())
// 	for _, children := range d.myChildren {
// 		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(children)
// 		if err != nil {
// 			d.logger.Warnf("Do not have latency measurement for %s", children.ToString())
// 			continue
// 		}
// 		currLat := nodeStats.LatencyCalc.CurrValue()
// 		childrenAsPeerWithChain := PeerWithIdChainFromPeerWithId(append(d.myIDChain, children.ID()), children)
// 		childrenAsMeasuredPeers = append(childrenAsMeasuredPeers, NewMeasuredPeer(childrenAsPeerWithChain, currLat))
// 	}

// 	toPrint = ""
// 	for _, possibility := range childrenAsMeasuredPeers {
// 		toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.ToString(), possibility.MeasuredLatency())
// 	}
// 	d.logger.Infof("childrenAsMeasuredPeers: %s", toPrint)

// 	if len(childrenAsMeasuredPeers) == 0 {
// 		d.logger.Warn("Have no candidates to send absorb message to")
// 		return
// 	}

// 	sort.Sort(childrenAsMeasuredPeers)
// 	for i := 0; i < len(childrenAsMeasuredPeers); i++ {
// 		candidateToAbsorb := childrenAsMeasuredPeers[i]
// 		candidateToAbsorbSiblingLatencies := d.myChildrenLatencies[candidateToAbsorb.ToString()]
// 		if len(candidateToAbsorbSiblingLatencies) < int(d.config.NrPeersToAbsorb) {
// 			continue
// 		}
// 		sort.Sort(candidateToAbsorbSiblingLatencies)
// 		toPrint = ""
// 		for _, possibility := range candidateToAbsorbSiblingLatencies {
// 			toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.ToString(), possibility.MeasuredLatency())
// 		}
// 		d.logger.Infof("candidateToAbsorbSiblingLatencies: %s:", toPrint)

// 		toAbsorb := MeasuredPeersByLat{}
// 		for i := uint16(0); i < d.config.NrPeersToAbsorb; i++ {
// 			toAbsorb = append(toAbsorb, candidateToAbsorbSiblingLatencies[i])
// 		}

// 		toPrint = ""
// 		for _, possibility := range toAbsorb {
// 			toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.ToString(), possibility.MeasuredLatency())
// 		}

// 		d.logger.Infof("Sending absorb message with bestCandidateChildrenToAbsorb: %s to: %s", toPrint, candidateToAbsorb.ToString())

// 		toSend := NewAbsorbMessage(toAbsorb, candidateToAbsorb)
// 		d.logger.Infof("Sending absorb message %+v to %s", toSend, candidateToAbsorb.ToString())

// 		for _, child := range d.myChildren {
// 			d.sendMessage(toSend, child)
// 		}

// 		for _, newGrandChildren := range toAbsorb {
// 			d.removeChild(newGrandChildren)
// 		}
// 		return
// 	}
// }
