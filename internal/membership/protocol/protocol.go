package protocol

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"time"

	"github.com/nm-morais/demmon/internal/utils"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/nodeWatcher"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const ProtoID = 2000
const ProtoName = "DemonTree"

type DemmonTreeConfig = struct {
	Landmarks                    []*PeerWithIDChain
	LandmarkRedialTimer          time.Duration
	JoinMessageTimeout           time.Duration
	MaxTimeToProgressToNextLevel time.Duration
	MaxRetriesJoinMsg            int
	ParentRefreshTickDuration    time.Duration
	ChildrenRefreshTickDuration  time.Duration
	RejoinTimerDuration          time.Duration

	NrPeersToBecomeChildrenPerParentInAbsorb int
	NrPeersToBecomeParentInAbsorb            int

	PhiLevelForNodeDown                    float64
	MaxPeersInEView                        int
	EmitWalkTimeout                        time.Duration
	NrHopsToIgnoreWalk                     int
	RandomWalkTTL                          int
	BiasedWalkTTL                          int
	NrPeersInWalkMessage                   int
	NrPeersToMergeInWalkSample             int
	NrPeersToMeasureBiased                 int
	NrPeersToMeasureRandom                 int
	MeasureNewPeersRefreshTickDuration     time.Duration
	MaxMeasuredPeers                       int
	EvalMeasuredPeersRefreshTickDuration   time.Duration
	MinLatencyImprovementToImprovePosition time.Duration
	CheckChildenSizeTimerDuration          time.Duration
	MinLatencyImprovementPerPeerForSwitch  time.Duration
	EmitWalkProbability                    float64
	BiasedWalkProbability                  float64
	AttemptImprovePositionProbability      float64
	MinGrpSize                             uint16
	MaxGrpSize                             uint16

	LimitFirstLevelGroupSize bool
}

type PeerWithParentAndChildren struct {
	parent   *PeerWithIDChain
	peer     *MeasuredPeer
	children []*PeerWithIDChain
}

type DemmonTree struct {

	// utils
	nodeWatcher nodeWatcher.NodeWatcher
	babel       protocolManager.ProtocolManager
	logger      *logrus.Logger
	config      *DemmonTreeConfig

	// join state
	lastLevelProgress     time.Time
	joined                bool
	joinMap               map[string]*PeerWithParentAndChildren
	bestPeerlastLevel     *PeerWithParentAndChildren
	myPendingParentInJoin *MeasuredPeer

	// state TODO refactor to class
	self                         *PeerWithIDChain
	myGrandParent                *PeerWithIDChain
	myParent                     *PeerWithIDChain
	myPendingParentInRecovery    *PeerWithIDChain
	myPendingParentInAbsorb      *PeerWithIDChain
	myPendingParentInImprovement *MeasuredPeer
	myChildren                   map[string]*PeerWithIDChain
	mySiblings                   map[string]*PeerWithIDChain
	myChildrenLatencies          map[string]MeasuredPeersByLat

	measuringPeers map[string]bool
	measuredPeers  map[string]*MeasuredPeer
	eView          map[string]*PeerWithIDChain

	joinTimeoutTimerIds map[string]int
	landmark            bool
}

const (
	DebugTimerDuration = 1 * time.Second
)

func New(config *DemmonTreeConfig, babel protocolManager.ProtocolManager, nw nodeWatcher.NodeWatcher) protocol.Protocol {
	logger := logs.NewLogger(ProtoName)
	logger.Infof("Starting demmonTree with config: %+v", config)
	return &DemmonTree{
		nodeWatcher: nw,
		babel:       babel,
		config:      config,
		logger:      logger,
		// join state
		joinMap: make(map[string]*PeerWithParentAndChildren),
		// node state
		self:                nil,
		myGrandParent:       nil,
		myParent:            nil,
		mySiblings:          make(map[string]*PeerWithIDChain),
		myChildren:          make(map[string]*PeerWithIDChain),
		myChildrenLatencies: make(map[string]MeasuredPeersByLat),
		joinTimeoutTimerIds: make(map[string]int),

		// improvement state
		eView:                        make(map[string]*PeerWithIDChain),
		measuringPeers:               make(map[string]bool),
		measuredPeers:                make(map[string]*MeasuredPeer),
		myPendingParentInImprovement: nil,
	}
}

func (d *DemmonTree) ID() protocol.ID {
	return ProtoID
}

func (d *DemmonTree) Name() string {
	return ProtoName
}

func (d *DemmonTree) Logger() *logrus.Logger {
	return d.logger
}

func (d *DemmonTree) Start() {

	d.self = NewPeerWithIDChain(nil, d.babel.SelfPeer(), 0, 0, make(Coordinates, len(d.config.Landmarks)))
	for _, landmark := range d.config.Landmarks {

		if !peer.PeersEqual(d.babel.SelfPeer(), landmark) {
			continue
		}

		d.self = NewPeerWithIDChain(
			landmark.Chain(),
			landmark.Peer,
			0,
			0,
			make(Coordinates, len(d.config.Landmarks)),
		)
		d.landmark = true
		for _, landmark := range d.config.Landmarks {

			if peer.PeersEqual(d.babel.SelfPeer(), landmark) {
				continue
			}

			d.mySiblings[landmark.String()] = landmark
			d.babel.Dial(d.ID(), landmark, landmark.ToTCPAddr())
			d.nodeWatcher.Watch(landmark, d.ID())

			c := nodeWatcher.Condition{
				Repeatable:                false,
				CondFunc:                  func(nodeWatcher.NodeInfo) bool { return true },
				EvalConditionTickDuration: 1000 * time.Millisecond,
				Notification:              LandmarkMeasuredNotification{landmarkMeasured: landmark},
				Peer:                      landmark,
				EnableGracePeriod:         false,
				ProtoId:                   d.ID(),
			}
			d.nodeWatcher.NotifyOnCondition(c)
		}

		d.logger.Infof("I am landmark, my ID is: %+v", d.self.Chain())
		d.myParent = nil
		d.myChildren = make(map[string]*PeerWithIDChain)

		d.babel.RegisterPeriodicTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
		d.babel.RegisterPeriodicTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
		d.babel.RegisterPeriodicTimer(d.ID(), NewDebugTimer(DebugTimerDuration))
		d.babel.RegisterPeriodicTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
		return
	}

	d.babel.RegisterPeriodicTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
	d.babel.RegisterPeriodicTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
	d.babel.RegisterPeriodicTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration))
	d.babel.RegisterPeriodicTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout))
	d.babel.RegisterPeriodicTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
	d.babel.RegisterPeriodicTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
	d.babel.RegisterPeriodicTimer(d.ID(), NewDebugTimer(DebugTimerDuration))
	d.joinOverlay()
	d.logger.Error("HERE")
}

func (d *DemmonTree) Init() {
	d.babel.RegisterRequestHandler(d.ID(), GetNeighboursReqID, d.handleGetInView)
	d.babel.RegisterRequestHandler(d.ID(), BroadcastMessageReqID, d.handleBroadcastMessageReq)

	// join messages
	d.babel.RegisterMessageHandler(d.ID(), JoinMessage{}, d.handleJoinMessage)
	d.babel.RegisterMessageHandler(d.ID(), JoinReplyMessage{}, d.handleJoinReplyMessage)
	d.babel.RegisterMessageHandler(d.ID(), JoinAsChildMessage{}, d.handleJoinAsChildMessage)
	d.babel.RegisterMessageHandler(d.ID(), JoinAsChildMessageReply{}, d.handleJoinAsChildMessageReply)

	// update state messages
	d.babel.RegisterMessageHandler(d.ID(), UpdateParentMessage{}, d.handleUpdateParentMessage)
	d.babel.RegisterMessageHandler(d.ID(), UpdateChildMessage{}, d.handleUpdateChildMessage)
	d.babel.RegisterMessageHandler(d.ID(), AbsorbMessage{}, d.handleAbsorbMessage)
	d.babel.RegisterMessageHandler(d.ID(), DisconnectAsChildMessage{}, d.handleDisconnectAsChildMsg)
	d.babel.RegisterMessageHandler(d.ID(), RandomWalkMessage{}, d.handleRandomWalkMessage)
	d.babel.RegisterMessageHandler(d.ID(), WalkReplyMessage{}, d.handleWalkReplyMessage)

	d.babel.RegisterNotificationHandler(d.ID(), PeerMeasuredNotification{}, d.handlePeerMeasuredNotification)
	d.babel.RegisterNotificationHandler(d.ID(), LandmarkMeasuredNotification{}, d.handleLandmarkMeasuredNotification)
	d.babel.RegisterNotificationHandler(d.ID(), SuspectNotification{}, d.handlePeerDownNotification)

	d.babel.RegisterTimerHandler(d.ID(), joinTimerID, d.handleJoinTimer)
	d.babel.RegisterTimerHandler(d.ID(), landmarkRedialTimerID, d.handleLandmarkRedialTimer)
	d.babel.RegisterTimerHandler(d.ID(), parentRefreshTimerID, d.handleRefreshParentTimer)
	d.babel.RegisterTimerHandler(d.ID(), updateChildTimerID, d.handleUpdateChildTimer)
	d.babel.RegisterTimerHandler(d.ID(), checkChidrenSizeTimerID, d.handleCheckChildrenSizeTimer)
	d.babel.RegisterTimerHandler(d.ID(), externalNeighboringTimerID, d.handleExternalNeighboringTimer)
	d.babel.RegisterTimerHandler(d.ID(), measureNewPeersTimerID, d.handleMeasureNewPeersTimer)
	d.babel.RegisterTimerHandler(d.ID(), evalMeasuredPeersTimerID, d.handleEvalMeasuredPeersTimer)
	d.babel.RegisterTimerHandler(d.ID(), peerJoinMessageResponseTimeoutID, d.handleJoinMessageResponseTimeout)

	// other
	d.babel.RegisterTimerHandler(d.ID(), debugTimerID, d.handleDebugTimer)
	d.babel.RegisterMessageHandler(d.ID(), BroadcastMessage{}, d.handleBroadcastMessage)
}

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {
	if !d.joined {
		d.logger.Info("-------------Rejoining overlay---------------")
		d.joinOverlay()
	}
}

// notification handlers

func (d *DemmonTree) handlePeerMeasuredNotification(n notification.Notification) {
	peerMeasuredNotification := n.(PeerMeasuredNotification)

	if peerMeasuredNotification.join {
		d.handleMeasuringPeerInJoinFinish(peerMeasuredNotification.peerMeasured)
		return
	}

	d.handleMeasuringPeerInEViewFinish(peerMeasuredNotification.peerMeasured)

}

func (d *DemmonTree) handleMeasuringPeerInJoinFinish(peerMeasured *PeerWithIDChain) {
	if p, ok := d.joinMap[peerMeasured.String()]; ok {
		currNodeStats, err := d.nodeWatcher.GetNodeInfo(peerMeasured)
		if err != nil {
			d.logger.Panic(err.Reason())
			return
		}

		d.logger.Infof(
			"New peer in join measured: %s, latency: %s",
			peerMeasured,
			currNodeStats.LatencyCalc().CurrValue(),
		)

		p.peer.MeasuredLatency = currNodeStats.LatencyCalc().CurrValue()
		d.attemptProgress()
	} else {
		d.logger.Warnf("Got peer measured notification in join but target %s is not in joinMap", peerMeasured.String())
	}
}

func (d *DemmonTree) handleMeasuringPeerInEViewFinish(peerMeasured *PeerWithIDChain) {
	delete(d.measuringPeers, peerMeasured.String())
	if d.isNeighbour(peerMeasured.Peer) {
		d.logger.Warnf("New peer measured: %s is a neighbor", peerMeasured)
		return
	}

	currNodeStats, err := d.nodeWatcher.GetNodeInfo(peerMeasured)
	if err != nil {
		d.logger.Panic(err.Reason())
		return
	}

	d.logger.Infof(
		"New peer measured: %s, latency: %s",
		peerMeasured,
		currNodeStats.LatencyCalc().CurrValue(),
	)

	d.addToMeasuredPeers(
		NewMeasuredPeer(
			peerMeasured,
			currNodeStats.LatencyCalc().CurrValue(),
		),
	)

	d.nodeWatcher.Unwatch(peerMeasured.Peer, d.ID())
	d.logger.Infof("d.measuredPeers:  %+v:", d.measuredPeers)
}

func (d *DemmonTree) handleLandmarkMeasuredNotification(nGeneric notification.Notification) {
	n := nGeneric.(LandmarkMeasuredNotification)

	for idx, l := range d.config.Landmarks {
		if !peer.PeersEqual(l, n.landmarkMeasured) {
			continue
		}

		landmarkStats, err := d.nodeWatcher.GetNodeInfo(l)
		if err != nil {
			d.logger.Panic("landmark was measured but has no measurement...")
		}

		d.self.Coordinates[idx] = uint64(landmarkStats.LatencyCalc().CurrValue().Milliseconds())
		d.updateSelfVersion()
		d.logger.Infof("My Coordinates: %+v", d.self.Coordinates)
		return
	}

	d.logger.Panic("handleLandmarkMeasuredNotification for non-landmark peer")
}

// timer handlers

func (d *DemmonTree) handleLandmarkRedialTimer(t timer.Timer) {
	redialTimer := t.(*landmarkRedialTimer)
	landmark := redialTimer.LandmarkToRedial
	d.logger.Infof("Redialing landmark %s", landmark.String())
	d.mySiblings[landmark.String()] = landmark
	d.nodeWatcher.Watch(landmark, d.ID())
	c := nodeWatcher.Condition{
		Repeatable:                false,
		CondFunc:                  func(nodeWatcher.NodeInfo) bool { return true },
		EvalConditionTickDuration: 1000 * time.Millisecond,
		Notification:              LandmarkMeasuredNotification{landmarkMeasured: landmark},
		Peer:                      landmark,
		EnableGracePeriod:         false,
		ProtoId:                   d.ID(),
	}
	d.nodeWatcher.NotifyOnCondition(c)
	d.babel.Dial(d.ID(), landmark, landmark.ToTCPAddr())
}

func (d *DemmonTree) handleRefreshParentTimer(t timer.Timer) {
	for _, child := range d.myChildren {
		if !child.outConnActive {
			d.logger.Warnf("Could not send message to children because there is no active conn to it %s", child.StringWithFields())
			continue
		}
		childrenArr := getMapAsPeerWithIDChainArray(d.myChildren, child)
		d.logger.Infof("Sending children: %+v to %s", childrenArr, child.StringWithFields())

		toSend := NewUpdateParentMessage(
			d.myParent,
			d.self,
			child.Chain()[len(child.Chain())-1],
			childrenArr,
		)
		d.sendMessage(toSend, child.Peer)
	}
}

func (d *DemmonTree) handleUpdateChildTimer(t timer.Timer) {
	// d.logger.Info("UpdateChildTimer trigger")
	if d.myParent != nil {
		d.sendUpdateChildMessage(d.myParent)
	}
}

func (d *DemmonTree) sendUpdateChildMessage(dest peer.Peer) {
	measuredSiblings := d.getPeerMapAsPeerMeasuredArr(d.mySiblings)
	d.logger.Infof("Sending measured siblings: %+v", measuredSiblings)
	toSend := NewUpdateChildMessage(d.self, measuredSiblings)
	d.sendMessage(toSend, dest)
}

func (d *DemmonTree) handleExternalNeighboringTimer(joinTimer timer.Timer) {
	d.logger.Info("ExternalNeighboringTimer trigger")

	if !d.joined {
		return
	}

	r := utils.GetRandFloat64()
	if r > d.config.EmitWalkProbability {
		return
	}

	neighbors := d.getNeighborsAsPeerWithIDChainArray()
	// d.logger.Infof("d.possibilitiesToSend: %+v:", neighbours)

	sample := getRandSample(d.config.NrPeersInWalkMessage-1, append(neighbors, peerMapToArr(d.eView)...)...)
	sample[d.self.String()] = d.self

	// r = rand.Float32()
	var msgToSend message.Message

	var peerToSendTo *PeerWithIDChain
	// if r < d.config.BiasedWalkProbability {
	// 	msgToSend = NewBiasedWalkMessage(uint16(d.config.RandomWalkTTL), selfPeerWithChain, sample)
	// 	peerToSendTo = getBiasedPeerExcluding(possibilitiesToSend, selfPeerWithChain)
	// } else {
	msgToSend = NewRandomWalkMessage(uint16(d.config.RandomWalkTTL), d.self, peerMapToArr(sample))
	peerToSendTo = getRandomExcluding(
		getExcludingDescendantsOf(neighbors, d.self.Chain()),
		map[string]interface{}{d.self.String(): nil},
	)
	// }

	if peerToSendTo == nil {
		d.logger.Error("peerToSendTo is nil")
		return
	}
	d.logger.Infof("sending random walk to %s", peerToSendTo.StringWithFields())
	d.sendMessage(msgToSend, peerToSendTo.Peer)
}

func (d *DemmonTree) handleEvalMeasuredPeersTimer(evalMeasuredPeersTimer timer.Timer) {

	d.logger.Info("EvalMeasuredPeersTimer trigger...")

	if len(d.self.Chain()) == 0 ||
		d.myParent == nil ||
		d.myPendingParentInImprovement != nil ||
		d.myPendingParentInAbsorb != nil ||
		d.myPendingParentInJoin != nil {
		d.logger.Info("EvalMeasuredPeersTimer returning because already have pending parent")
		return
	}

	r := utils.GetRandFloat64()
	if r > d.config.AttemptImprovePositionProbability {
		d.logger.Info("returning due to  r > d.config.AttemptImprovePositionProbability")
		return
	}

	measuredPeersArr := make(MeasuredPeersByLat, 0, len(d.measuredPeers))

	for _, p := range d.measuredPeers {
		if d.self.IsDescendentOf(p.Chain()) {
			delete(d.measuredPeers, p.String())
			continue
		}

		measuredPeersArr = append(measuredPeersArr, p)
	}

	sort.Sort(measuredPeersArr)

	d.logger.Info("Evaluating measuredPeers...")
	d.logger.Infof("d.measuredPeers:  %s:", measuredPeersArr.String())

	if len(measuredPeersArr) == 0 {
		d.logger.Warn("returning due to len(measuredPeersArr) == 0")
		return
	}

	parentStats, err := d.nodeWatcher.GetNodeInfo(d.myParent)
	if err != nil {
		d.logger.Errorf("Do not have latency measurement for parent")
		return
	}

	parentLatency := parentStats.LatencyCalc().CurrValue()
	for _, measuredPeer := range measuredPeersArr {

		// parent latency is higher than latency to peer
		latencyImprovement := parentLatency - measuredPeer.MeasuredLatency

		if latencyImprovement < d.config.MinLatencyImprovementToImprovePosition {
			continue
		}

		if measuredPeer.nChildren >= d.config.MaxGrpSize {
			continue
		}

		if d.self.Chain().Level() <= measuredPeer.Chain().Level() {
			continue
		}

		// if len(d.myChildren) > 0 {
		// 	// cannot join as child because due to having children
		// 	if measuredPeer.Chain().Level() >= d.self.Chain().Level() {
		// 		continue // TODO possibly send message for the other node to join me as child
		// 	}
		// }

		d.logger.Infof("Improving position towards: %s", measuredPeer.String())
		d.logger.Infof("self level: %d", d.self.Chain().Level())
		d.logger.Infof("target peer level: %d", measuredPeer.Chain().Level())

		d.logger.Infof("latencyImprovement: %s", latencyImprovement)
		d.logger.Infof("parentLatency: %s", parentLatency)
		d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
		aux := *measuredPeer
		d.myPendingParentInImprovement = &aux
		d.sendJoinAsChildMsg(measuredPeer.PeerWithIDChain, measuredPeer.MeasuredLatency, false, true)

		return
	}
}

func (d *DemmonTree) handleMeasureNewPeersTimer(measureNewPeersTimer timer.Timer) {

	if len(d.eView) == 0 {
		d.logger.Infof("returning because len(eView) == 0")
		return
	}

	peersSorted := peerMapToArr(d.eView)
	sort.Slice(
		peersSorted, func(i, j int) (res bool) {
			var err error
			var d1, d2 float64
			if d1, err = EuclideanDist(peersSorted[i].Coordinates, d.self.Coordinates); err == nil {
				if d2, err = EuclideanDist(peersSorted[j].Coordinates, d.self.Coordinates); err == nil {
					return d1 < d2
				}
			}
			d.logger.Panic(err)
			return false
		},
	)

	nrMeasuredBiased := 0
	for i := 0; i < len(peersSorted) && nrMeasuredBiased < d.config.NrPeersToMeasureBiased; i++ {
		p := peersSorted[i]
		if d.measurePeerExternalProcedure(p) {
			nrMeasuredBiased++
		}
	}

	peersRandom := peerMapToArr(d.eView)
	rand.Shuffle(len(peersRandom), func(i, j int) { peersRandom[i], peersRandom[j] = peersRandom[j], peersRandom[i] })

	nrMeasuredRand := 0
	for i := 0; i < d.config.NrPeersToMeasureRandom && nrMeasuredRand < d.config.NrPeersToMeasureRandom; i++ {
		p := peersRandom[i]
		if d.measurePeerExternalProcedure(p) {
			nrMeasuredRand++
		}
	}
}

func (d *DemmonTree) measurePeerExternalProcedure(p *PeerWithIDChain) bool {

	if _, isMeasuring := d.measuringPeers[p.String()]; isMeasuring {
		return false
	}

	if _, alreadyMeasured := d.measuredPeers[p.String()]; alreadyMeasured {
		return false
	}

	d.logger.Infof("measuring peer: %s", p.String())
	d.nodeWatcher.Watch(p, d.ID())
	c := nodeWatcher.Condition{
		Repeatable:                false,
		CondFunc:                  func(nodeWatcher.NodeInfo) bool { return true },
		EvalConditionTickDuration: 1000 * time.Millisecond,
		Notification:              NewPeerMeasuredNotification(p, false),
		Peer:                      p,
		EnableGracePeriod:         false,
		ProtoId:                   d.ID(),
	}
	// d.logger.Infof("Doing NotifyOnCondition for node %s...", p.String())
	d.nodeWatcher.NotifyOnCondition(c)
	d.measuringPeers[p.String()] = true
	return true
}

// func (d *DemmonTree) sendChildrenToLowerLevels() {

// 	childrenAsMeasuredPeers := d.getPeerMapAsPeerMeasuredArr(d.myChildren)
// 	peersToKickPerAbsorber := make(map[*MeasuredPeer]map[string]*MeasuredPeer) // bit weird, but works for what i need

// 	for i := 0; len(peersToKickPerAbsorber) < d.config.NrPeersToBecomeParentInAbsorb && i < len(childrenAsMeasuredPeers); i++ {
// 		if childrenAsMeasuredPeers[i].NrChildren() > 0 {
// 			peersToKickPerAbsorber[childrenAsMeasuredPeers[i]] = make(map[string]*MeasuredPeer)
// 		}
// 	}

// 	nrPeersKicked := func() int {
// 		nrPeersKicked := 0
// 		for _, v := range peersToKickPerAbsorber {
// 			nrPeersKicked += len(v)
// 		}
// 		return nrPeersKicked
// 	}

// 	alreadyKicked := func(toFind string) bool {
// 		for _, v := range peersToKickPerAbsorber {
// 			if _, ok := v[toFind]; ok {
// 				return true
// 			}
// 		}
// 		return false
// 	}

// 	kicked := true
// 	for kicked && len(d.myChildren)-nrPeersKicked() > int(d.config.MinGrpSize) {
// 		kicked = false
// 		for peerAbsorber := range peersToKickPerAbsorber {
// 			peerAbsorberSiblingLatencies := d.myChildrenLatencies[peerAbsorber.String()]
// 			sort.Sort(peerAbsorberSiblingLatencies)

// 			for _, candidateToKick := range peerAbsorberSiblingLatencies {

// 				if candidateToKick == nil {
// 					continue
// 				}

// 				if _, isChild := d.myChildren[candidateToKick.String()]; !isChild {
// 					d.logger.Errorf("Candidate %s to be absorbed is not a child", candidateToKick.String())
// 					continue
// 				}

// 				if peer.PeersEqual(candidateToKick, peerAbsorber) {
// 					continue
// 				}

// 				if candidateToKick.MeasuredLatency == 0 {
// 					continue
// 				}

// 				if alreadyKicked(candidateToKick.String()) {
// 					continue
// 				}

// 				if peerAbsorber.MeasuredLatency-candidateToKick.MeasuredLatency > d.config.MinLatencyImprovementToImprovePosition {
// 					peersToKickPerAbsorber[peerAbsorber][candidateToKick.String()] = candidateToKick
// 					d.logger.Infof(
// 						"Sending absorb message with peerToAbsorb: %s, to: %s",
// 						candidateToKick.StringWithFields(),
// 						peerAbsorber.StringWithFields(),
// 					)
// 					toSend := NewAbsorbMessage(peerAbsorber.PeerWithIDChain)
// 					d.sendMessage(toSend, candidateToKick)
// 					kicked = true
// 				}
// 			}
// 		}
// 	}
// }

func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {

	// nrPeersToKick := d.config.NrPeersToBecomeParentInAbsorb * int(d.config.NrPeersToBecomeChildrenPerParentInAbsorb)

	if len(d.myChildren) < d.config.NrPeersToBecomeChildrenPerParentInAbsorb*d.config.NrPeersToBecomeParentInAbsorb {
		d.logger.Info("handleCheckChildrenSize timer trigger returning due to goup size being too small")
		return
	}
	childrenAsMeasuredPeers := d.getPeerMapAsPeerMeasuredArr(d.myChildren)
	sort.Sort(childrenAsMeasuredPeers)
	peersToKickPerAbsorber := make([]*struct {
		totalLatency int
		absorber     *MeasuredPeer
		peersToKick  MeasuredPeersByLat
	}, 0) // bit weird, but works for what i need

	for idx, child := range childrenAsMeasuredPeers {
		if idx == d.config.NrPeersToBecomeChildrenPerParentInAbsorb {
			break
		}
		peersToKickPerAbsorber = append(peersToKickPerAbsorber, &struct {
			totalLatency int
			absorber     *MeasuredPeer
			peersToKick  MeasuredPeersByLat
		}{
			totalLatency: 0,
			absorber:     child,
			peersToKick:  []*MeasuredPeer{},
		})
	}

	// nrPeersKicked := func() int {
	// 	nrPeersKicked := 0
	// 	for _, v := range peersToKickPerAbsorber {
	// 		nrPeersKicked += len(v)
	// 	}
	// 	return nrPeersKicked
	// }

	alreadyKicked := func(toFind string) bool {
		for _, v := range peersToKickPerAbsorber {
			if v.absorber.String() == toFind {
				return true
			}
			for _, v2 := range v.peersToKick {
				if v2.String() == toFind {
					return true
				}
			}
		}
		return false
	}

	for i := 0; i < d.config.NrPeersToBecomeChildrenPerParentInAbsorb*d.config.NrPeersToBecomeParentInAbsorb; i++ {
		peerAbsorberStats := peersToKickPerAbsorber[i%d.config.NrPeersToBecomeParentInAbsorb]
		peerAbsorber := peerAbsorberStats.absorber
		peerAbsorberSiblingLatencies := d.myChildrenLatencies[peerAbsorber.String()]
		sort.Sort(peerAbsorberSiblingLatencies)
		d.logger.Infof("peer %s sibling latencies: %s", peerAbsorber.StringWithFields(), peerAbsorberSiblingLatencies.String())
		for _, candidateToKick := range peerAbsorberSiblingLatencies {

			if len(peerAbsorberStats.peersToKick) == int(d.config.NrPeersToBecomeChildrenPerParentInAbsorb) {
				break
			}

			if candidateToKick == nil {
				continue
			}

			if _, isChild := d.myChildren[candidateToKick.String()]; !isChild {
				continue
			}

			if peer.PeersEqual(candidateToKick, peerAbsorber) {
				continue
			}

			if candidateToKick.MeasuredLatency == 0 {
				continue
			}

			if alreadyKicked(candidateToKick.String()) {
				continue
			}

			// nodeInfo, err := d.nodeWatcher.GetNodeInfo(candidateToKick)
			// if err != nil {
			// 	continue
			// }

			// if d.self.NrChildren() < d.config.MaxGrpSize && nodeInfo.LatencyCalc().CurrValue() < candidateToKick.MeasuredLatency {
			// 	continue
			// }

			peerAbsorberStats.peersToKick = append(peerAbsorberStats.peersToKick, candidateToKick)
			peerAbsorberStats.totalLatency += int(candidateToKick.MeasuredLatency)

			// if alreadyKicked(candidateToKick.String()) {
			// 	continue
			// }

			// if candidateToKick.MeasuredLatency < lowestLatPairLat {
			// 	lowestLatPairLat = candidateToKick.MeasuredLatency
			// 	bestCandidateToAbsorb = peerAbsorber
			// 	bestCandidateToKick = candidateToKick
			// }
		}
	}

	for _, absorberStats := range peersToKickPerAbsorber {
		absorber := absorberStats.absorber
		if len(absorberStats.peersToKick) < int(d.config.MinGrpSize) {
			d.logger.Infof("peer %s does not have enough siblings (%d/%d) to become parent in absorb", absorber.StringWithFields(), len(absorberStats.peersToKick), d.config.MinGrpSize)
			continue
		}
		for _, toKickPeer := range absorberStats.peersToKick {
			d.logger.Infof(
				"Sending absorb message with peerToAbsorb: %s, peerAbsorber: %s",
				toKickPeer.StringWithFields(),
				absorber.StringWithFields(),
			)
			toSend := NewAbsorbMessage(absorber.PeerWithIDChain)
			d.sendMessage(toSend, toKickPeer)
		}
		break
	}

}

// message handlers

func (d *DemmonTree) handleRandomWalkMessage(sender peer.Peer, m message.Message) {
	randWalkMsg := m.(RandomWalkMessage)

	hopsTaken := d.config.RandomWalkTTL - int(randWalkMsg.TTL)
	if hopsTaken < d.config.NrHopsToIgnoreWalk {
		randWalkMsg.TTL--
		sampleToSend, neighboursWithoutSenderDescendants := d.mergeSampleWithEview(
			randWalkMsg.Sample,
			randWalkMsg.Sender,
			0,
			d.config.NrPeersToMergeInWalkSample,
		)
		p := getRandomExcluding(
			neighboursWithoutSenderDescendants,
			map[string]interface{}{d.self.String(): nil, randWalkMsg.Sender.String(): nil, sender.String(): nil},
		)
		if p == nil {
			walkReply := NewWalkReplyMessage(sampleToSend)

			d.logger.Infof(
				"have no peers to forward message... merging and sending random walk reply %+v to %s",
				randWalkMsg,
				randWalkMsg.Sender.String(),
			)
			d.sendMessageTmpTCPChan(walkReply, randWalkMsg.Sender)
			return
		}
		randWalkMsg.Sample = sampleToSend
		d.logger.Infof(
			"hopsTaken < d.config.NrHopsToIgnoreWalk, adding peers and forwarding random walk message %+v to: %s",
			randWalkMsg,
			p.String(),
		)
		d.sendMessage(randWalkMsg, p)
		return
	}

	if randWalkMsg.TTL > 0 {
		// toPrint := ""
		// for _, peer := range neighboursWithoutSenderDescendants {
		// 	d.logger.Infof("%+v", peer)
		// 	// d.logger.Infof("neighboursWithoutSenderDescendants peer: %s", peer.String())
		// }
		// d.logger.Infof("neighboursWithoutSenderDescendants: %s", toPrint)
		sampleToSend, neighboursWithoutSenderDescendants := d.mergeSampleWithEview(
			randWalkMsg.Sample,
			randWalkMsg.Sender,
			d.config.NrPeersToMergeInWalkSample,
			d.config.NrPeersToMergeInWalkSample,
		)
		randWalkMsg.TTL--
		p := getRandomExcluding(
			neighboursWithoutSenderDescendants,
			map[string]interface{}{randWalkMsg.Sender.String(): nil, d.self.String(): nil, sender.String(): nil},
		)
		if p == nil {
			d.logger.Infof(
				"have no peers to forward message... merging and sending random walk reply to %s",
				randWalkMsg.Sender.String(),
			)
			walkReply := NewWalkReplyMessage(sampleToSend)
			d.sendMessageTmpTCPChan(walkReply, randWalkMsg.Sender)
			return
		}
		randWalkMsg.Sample = sampleToSend
		d.logger.Infof(
			"hopsTaken >= d.config.NrHopsToIgnoreWalk, merging and forwarding random walk message %+v to: %s",
			randWalkMsg,
			p.String(),
		)
		d.sendMessage(randWalkMsg, p)
		return
	}

	// TTL == 0
	if randWalkMsg.TTL == 0 {
		sampleToSend, _ := d.mergeSampleWithEview(
			randWalkMsg.Sample,
			randWalkMsg.Sender,
			d.config.NrPeersToMergeInWalkSample,
			d.config.NrPeersToMergeInWalkSample,
		)

		d.logger.Infof("random walk TTL is 0. Sending random walk reply to %s", randWalkMsg.Sender.String())
		d.sendMessageTmpTCPChan(NewWalkReplyMessage(sampleToSend), randWalkMsg.Sender)
		return
	}
}

func (d *DemmonTree) handleWalkReplyMessage(sender peer.Peer, m message.Message) {
	walkReply := m.(WalkReplyMessage)
	// d.logger.Infof("Got walkReplyMessage: %+v from %s", walkReply, sender.String())
	sample := walkReply.Sample
	d.updateAndMergeSampleEntriesWithEView(sample, d.config.NrPeersToMergeInWalkSample)
}

func (d *DemmonTree) handleAbsorbMessage(sender peer.Peer, m message.Message) {
	absorbMessage := m.(AbsorbMessage)
	if !peer.PeersEqual(d.myParent, sender) {
		d.logger.Warnf(
			"Got absorbMessage: %+v from not my parent %s (my parent: %s)",
			m,
			sender.String(),
			getStringOrNil(d.myParent),
		)
		return
	}

	d.logger.Infof("Got absorbMessage: %+v from %s", m, sender.String())
	newParent := absorbMessage.peerAbsorber

	if sibling, ok := d.mySiblings[newParent.String()]; ok {
		delete(d.mySiblings, newParent.String())
		var peerLat time.Duration
		if nodeInfo, err := d.nodeWatcher.GetNodeInfo(sibling); err == nil {
			peerLat = nodeInfo.LatencyCalc().CurrValue()
		}
		d.myPendingParentInAbsorb = newParent
		toSend := NewJoinAsChildMessage(d.self, peerLat, newParent.Chain(), false, false)
		d.sendMessage(toSend, newParent)
	} else {
		d.myPendingParentInAbsorb = newParent
		toSend := NewJoinAsChildMessage(d.self, 0, newParent.Chain(), false, false)
		d.sendMessageTmpTCPChan(toSend, newParent)
	}
}

func (d *DemmonTree) handleDisconnectAsChildMsg(sender peer.Peer, m message.Message) {
	dacMsg := m.(DisconnectAsChildMessage)
	d.logger.Infof("got DisconnectAsChildMsg %+v from %s", dacMsg, sender.String())
	d.removeChild(sender)
}

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {
	toSend := NewJoinReplyMessage(getMapAsPeerWithIDChainArray(d.myChildren), d.self, d.myParent)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinMessageResponseTimeout(t timer.Timer) {
	p := t.(*peerJoinMessageResponseTimeout).Peer

	if _, ok := d.joinTimeoutTimerIds[p.String()]; !ok {
		return
	}

	delete(d.joinTimeoutTimerIds, p.String())
	if d.joined {
		d.logger.Warnf("ingnoring timeout from join message from %s", p.String())
		return
	}
	d.logger.Warnf("timeout from join message from %s", p.String())
	delete(d.joinMap, p.String())
	d.nodeWatcher.Unwatch(p, d.ID())
	d.attemptProgress()
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(JoinReplyMessage)

	d.logger.Infof("Got joinReply: %+v from %s", replyMsg, sender.String())

	if _, ok := d.joinTimeoutTimerIds[sender.String()]; ok {
		d.babel.CancelTimer(d.joinTimeoutTimerIds[sender.String()])
		delete(d.joinTimeoutTimerIds, sender.String())
	}

	if d.joined {
		d.logger.Errorf("Got joinReply: %+v but already joined overylay... %s", replyMsg, sender.String())
		return
	}

	p, ok := d.joinMap[sender.String()]
	if !ok {
		panic("curr lvl join peer should be set")
	}

	p.children = replyMsg.Children
	p.parent = replyMsg.Parent
	p.peer.PeerWithIDChain = replyMsg.Sender
	if !p.peer.chain.Equal(replyMsg.Sender.Chain()) {
		d.logger.Errorf("Discarding peer %s from join process as its chain changed", p.parent.StringWithFields())
		delete(d.joinMap, sender.String())
	}

	d.attemptProgress()
}

func (d *DemmonTree) canBecomeParentOf(other *PeerWithIDChain, isRecovery, isImprovement bool) bool {

	if len(d.self.Chain()) == 0 {
		d.logger.Warnf("cannot become parent of %s because my chain is nil", other.StringWithFields())
		return false
	}

	if d.self.IsDescendentOf(other.Chain()) {
		d.logger.Warnf(
			"cannot become parent of %s because im descendent of sender, sender chain: %+v",
			other.StringWithFields(),
			other.Chain(),
		)
		return false
	}

	if isImprovement && len(d.myChildren) == 0 {
		d.logger.Warn("cannot become parent of %s because it is an improvemnt and i do not have enough children",
			other.StringWithFields())
		return false
	}

	return true
}

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {
	jacMsg := m.(JoinAsChildMessage)
	d.logger.Infof("got JoinAsChildMessage %+v from %s", jacMsg, sender.String())

	if !d.canBecomeParentOf(jacMsg.Sender, jacMsg.Urgent, jacMsg.Improvement) {
		toSend := NewJoinAsChildMessageReply(false, PeerID{}, d.self.Chain().Level(), d.self, nil, nil)
		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	_, isSibling := d.mySiblings[sender.String()]
	if isSibling {
		delete(d.mySiblings, sender.String())
	}

	newChildID := d.addChild(jacMsg.Sender, jacMsg.MeasuredLatency)
	toSend := NewJoinAsChildMessageReply(
		true,
		newChildID,
		d.self.Chain().Level(),
		d.self,
		getMapAsPeerWithIDChainArray(d.myChildren, jacMsg.Sender),
		d.myGrandParent,
	)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinAsChildMessageReply(sender peer.Peer, m message.Message) {
	japrMsg := m.(JoinAsChildMessageReply)
	d.logger.Infof("got JoinAsChildMessageReply %+v from %s", japrMsg, sender.String())

	if japrMsg.Accepted {
		myNewID := append(japrMsg.Parent.Chain(), japrMsg.ProposedID)
		d.addParent(
			japrMsg.Parent,
			japrMsg.GrandParent,
			myNewID,
		)
		return
	}

	if d.myPendingParentInImprovement != nil && peer.PeersEqual(japrMsg.Parent, d.myPendingParentInImprovement) {
		d.logger.Warnf("Pending Parent In Improvement join as child nessage")
		if mp, ok := d.measuredPeers[d.myPendingParentInImprovement.String()]; ok {
			delete(d.measuredPeers, japrMsg.Parent.String())
			d.addToMeasuredPeers(&MeasuredPeer{PeerWithIDChain: japrMsg.Parent, MeasuredLatency: mp.MeasuredLatency})
		}
		d.myPendingParentInImprovement = nil
		return
	}

	// not accepted
	d.handlePeerDown(japrMsg.Parent)
}

func getStringOrNil(p *PeerWithIDChain) string {
	if p == nil {
		return "nil"
	}
	return p.StringWithFields()
}

func (d *DemmonTree) handleUpdateParentMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(UpdateParentMessage)
	d.logger.Infof("got UpdateParentMessage %+v from %s", upMsg, sender.String())

	if !peer.PeersEqual(sender, d.myParent) {
		d.logger.Errorf(
			"Received UpdateParentMessage from not my parent (parent:%s sender:%s)",
			getStringOrNil(d.myParent),
			upMsg.Parent.StringWithFields(),
		)
		d.sendMessageTmpTCPChan(NewDisconnectAsChildMessage(), sender)
		return
	}

	if upMsg.GrandParent != nil {
		if !peer.PeersEqual(d.myGrandParent, upMsg.GrandParent) {
			d.logger.Infof(
				"My grandparent changed : (%s -> %s)",
				getStringOrNil(d.myGrandParent),
				getStringOrNil(upMsg.GrandParent),
			)
			d.myGrandParent = upMsg.GrandParent
		}
	}

	d.myGrandParent = upMsg.GrandParent
	d.myParent = upMsg.Parent

	myNewChain := append(upMsg.Parent.Chain(), upMsg.ProposedID)
	if !myNewChain.Equal(d.self.Chain()) {
		d.logger.Infof("My chain changed: (%+v -> %+v)", d.self.Chain(), myNewChain)
		d.logger.Infof(
			"My level changed: (%d -> %d)",
			d.self.Chain().Level(),
			upMsg.Parent.Chain().Level()+1,
		) // IMPORTANT FOR VISUALIZER

		d.self = NewPeerWithIDChain(myNewChain, d.self.Peer, d.self.nChildren, d.self.Version()+1, d.self.Coordinates)
	}
	d.mergeSiblingsWith(upMsg.Siblings)
}

func (d *DemmonTree) handleUpdateChildMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(UpdateChildMessage)
	d.logger.Infof("got updateChildMessage %+v from %s", m, sender.String())
	child, ok := d.myChildren[sender.String()]

	if !ok {
		d.logger.Errorf(
			"got updateChildMessage %+v from not my children, or my pending children: %s",
			m,
			sender.String(),
		)
		return
	}
	d.myChildren[sender.String()] = upMsg.Child
	d.myChildren[sender.String()].inConnActive = child.inConnActive
	d.myChildren[sender.String()].outConnActive = child.outConnActive
	d.myChildrenLatencies[sender.String()] = upMsg.Siblings
}

func (d *DemmonTree) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	if dialerProto != d.ID() {
		d.logger.Infof("Not accepting dial from other proto: %d", dialerProto)
		return false
	}

	if d.myParent != nil && peer.PeersEqual(d.myParent, p) {
		d.myParent.inConnActive = true
		d.logger.Infof("My parent dialed me")
		return true
	}

	child, isChildren := d.myChildren[p.String()]
	if isChildren {
		child.inConnActive = true
		d.logger.Infof("My children (%s) dialed me ", child.StringWithFields())
		return true
	}

	sibling, isSibling := d.mySiblings[p.String()]
	if isSibling {
		sibling.inConnActive = true
		d.logger.Infof("My sibling (%s) dialed me ", sibling.StringWithFields())
		return true
	}

	d.logger.Warnf("Conn requested by unknown peer: %s", p.String())
	return true
}

func (d *DemmonTree) isLandmark(p peer.Peer) (*PeerWithIDChain, bool) {
	for _, l := range d.config.Landmarks {
		if peer.PeersEqual(l, p) {
			return l, true
		}
	}
	return nil, false
}

func (d *DemmonTree) installNotifyOnCondition(p *PeerWithIDChain) {
	c := nodeWatcher.Condition{
		Repeatable:                false,
		CondFunc:                  d.isNodeDown,
		EvalConditionTickDuration: 1000 * time.Millisecond,
		Notification:              SuspectNotification{peerDown: p},
		Peer:                      p,
		EnableGracePeriod:         false,
		ProtoId:                   d.ID(),
	}
	d.nodeWatcher.NotifyOnCondition(c)
}

func (d *DemmonTree) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {
	if sourceProto != d.ID() {
		d.logger.Infof("Not accepting dial from other proto: %d", sourceProto)
		return false
	}

	// d.logger.Infof("Dialed peer with success: %s", p.String())
	if d.myParent != nil && peer.PeersEqual(d.myParent, p) {
		d.logger.Infof("Dialed parent with success, parent: %s", d.myParent.StringWithFields())
		d.myParent.outConnActive = true
		d.installNotifyOnCondition(d.myParent)
		d.sendUpdateChildMessage(d.myParent)
		d.babel.SendNotification(NewNodeUpNotification(d.myParent, d.getInView()))
		return true
	}

	child, isChildren := d.myChildren[p.String()]
	if isChildren {
		child.outConnActive = true
		d.logger.Infof("Dialed children with success: %s", child.StringWithFields())
		toSend := NewUpdateParentMessage(
			d.myParent,
			d.self,
			child.Chain()[len(child.Chain())-1],
			getMapAsPeerWithIDChainArray(d.myChildren, child),
		)
		d.sendMessage(toSend, child)
		d.installNotifyOnCondition(child)
		d.babel.SendNotification(NewNodeUpNotification(child, d.getInView()))
		d.updateSelfVersion()
		return true
	}

	sibling, isSibling := d.mySiblings[p.String()]
	if isSibling {
		sibling.outConnActive = true
		d.logger.Infof("Dialed sibling with success: %s", sibling.StringWithFields())
		d.babel.SendNotification(NewNodeUpNotification(sibling, d.getInView()))
		d.installNotifyOnCondition(sibling)
		return true
	}

	d.logger.Errorf("Dialed unknown peer: %s", p.String())
	return false
}

func (d *DemmonTree) DialFailed(p peer.Peer) {
	d.logger.Errorf("Failed to dial %s", p.String())
	d.handlePeerDown(p)
}

func (d *DemmonTree) OutConnDown(p peer.Peer) {
	d.handlePeerDown(p)
}

func (d *DemmonTree) handlePeerDownNotification(n notification.Notification) {
	p := n.(SuspectNotification).peerDown
	d.logger.Errorf("peer down %s (PHI >= %f)", p.String(), d.config.PhiLevelForNodeDown)
	d.handlePeerDown(p.Peer)
}

func (d *DemmonTree) handlePeerDown(p peer.Peer) {

	// special case for parent in recovery
	d.nodeWatcher.Unwatch(p, d.ID())
	d.babel.Disconnect(d.ID(), p)

	if peer.PeersEqual(p, d.myPendingParentInJoin) {
		d.logger.Warnf("Falling back from Pending Parent In join procedure")
		d.myPendingParentInJoin = nil
		d.fallbackToPreviousLevel(p)
		return
	}

	if peer.PeersEqual(p, d.myPendingParentInRecovery) {
		d.logger.Warnf("Falling back from Pending Parent In Recovery")
		d.removeFromMeasuredPeers(d.myPendingParentInRecovery)
		d.fallbackToMeasuredPeers()
		return
	}

	if peer.PeersEqual(p, d.myPendingParentInAbsorb) {
		d.logger.Warnf("Falling back from Pending Parent In Absorb")
		d.myPendingParentInAbsorb = nil
		if d.myGrandParent != nil {
			d.logger.Warnf("falling back to grandparent")
			d.myPendingParentInRecovery = d.myGrandParent
			d.sendJoinAsChildMsg(d.myGrandParent, 0, true, false)
			d.myGrandParent = nil
		} else {
			d.fallbackToMeasuredPeers()
		}
	}

	if peer.PeersEqual(p, d.myParent) {
		d.logger.Warnf("Parent down %s", p.String())
		aux := d.myParent
		d.myParent = nil
		d.babel.SendNotification(NewNodeDownNotification(aux, d.getInView()))
		d.nodeWatcher.Unwatch(p, d.ID())
		if d.myGrandParent != nil {
			d.logger.Warnf("Falling back to grandparent %s", d.myGrandParent.String())
			d.myPendingParentInRecovery = d.myGrandParent
			d.sendJoinAsChildMsg(d.myGrandParent, 0, true, false)
			d.myGrandParent = nil
			return
		}
		d.logger.Warnf("Grandparent is nil... falling back to measured peers")
		d.fallbackToMeasuredPeers()
		return
	}

	if child, isChildren := d.myChildren[p.String()]; isChildren {
		d.logger.Warnf("Child down %s", p.String())
		d.removeChild(child)
		return
	}

	if sibling, isSibling := d.mySiblings[p.String()]; isSibling {
		d.logger.Warnf("Sibling down %s", p.String())
		d.removeSibling(sibling)
		if l, ok := d.isLandmark(p); d.landmark && ok {
			d.logger.Warnf("registering redial for sibling %s", p.String())
			d.babel.RegisterTimer(d.ID(), NewLandmarkRedialTimer(d.config.LandmarkRedialTimer, l))
			return
		}
		return
	}

	d.logger.Errorf("Unknown peer down %s", p.String())
}

func (d *DemmonTree) MessageDelivered(msg message.Message, p peer.Peer) {
	d.logger.Infof("Message of type %s delivered to: %s", reflect.TypeOf(msg), p.String())
}

func (d *DemmonTree) MessageDeliveryErr(msg message.Message, p peer.Peer, err errors.Error) {
	d.logger.Errorf("Message of type %s (%+v) failed to deliver to: %s because: %s", reflect.TypeOf(msg), msg, p.String(), err.Reason())

	switch msg := msg.(type) {
	case JoinMessage:
		if !d.joined {
			d.logger.Warnf(
				"Deleting peer %s from currLevelPeers because could not deliver join msg",
				p.String(),
			)
			delete(d.joinMap, p.String())
			d.nodeWatcher.Unwatch(p, d.ID())
			d.attemptProgress()
		}
	case JoinAsChildMessage:
		d.handlePeerDown(msg.Sender)
	case RandomWalkMessage:
		d.sendMessageTmpUDPChan(msg, p)

	case DisconnectAsChildMessage:
		d.babel.Disconnect(d.ID(), p)
	}
}

func (d *DemmonTree) joinOverlay() {
	d.logger.Info("Rejoining overlay")
	d.myPendingParentInAbsorb = nil
	d.myPendingParentInImprovement = nil
	d.myPendingParentInJoin = nil
	d.myPendingParentInRecovery = nil
	d.bestPeerlastLevel = nil
	d.joinMap = make(map[string]*PeerWithParentAndChildren)
	d.joined = false

	if d.landmark {
		return
	}

	d.logger.Infof("Landmarks:")
	for i, landmark := range d.config.Landmarks {
		d.logger.Infof("%d :%s", i, landmark.String())
		d.lastLevelProgress = time.Now()
		d.joinMap[landmark.String()] = &PeerWithParentAndChildren{parent: nil, peer: NewMeasuredPeer(landmark, math.MaxInt64), children: nil}
		joinMsg := JoinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark)
	}
}

func (d *DemmonTree) sendJoinAsChildMsg(
	newParent *PeerWithIDChain,
	newParentLat time.Duration,
	urgent, improvement bool) {

	d.logger.Infof("Pending parent: %s", newParent.String())
	d.logger.Infof("Joining level %d", uint16(len(newParent.Chain())))
	toSend := NewJoinAsChildMessage(d.self, newParentLat, newParent.Chain(), urgent, improvement)
	d.sendMessageTmpTCPChan(toSend, newParent)
}

func (d *DemmonTree) attemptProgress() {
	var lowestLatencyPeer *PeerWithParentAndChildren
	var idx int
	canProgress, nextLevelPeers := d.getPeersInNextLevelByLat(d.bestPeerlastLevel)
	if !canProgress {
		return
	}

	d.logger.Infof("Getting lowest latency peer...")
	for i, nextLevelPeer := range nextLevelPeers {
		_, isLandmark := d.isLandmark(nextLevelPeer.peer)
		if isLandmark {
			idx = i
			lowestLatencyPeer = nextLevelPeer
			break
		}

		currPeerParent := nextLevelPeer.parent
		if currPeerParent == nil {
			continue
		}
		prevParent, ok := d.joinMap[currPeerParent.String()]
		if !ok {
			continue
		}

		if !peer.PeersEqual(currPeerParent, prevParent.peer) {
			d.logger.Infof("Discarding peer %s because parent switched (%s/%s) in the meantime", nextLevelPeer.peer, getStringOrNil(currPeerParent), getStringOrNil(prevParent.parent))
			continue
		}
		idx = i
		lowestLatencyPeer = nextLevelPeer
	}

	if lowestLatencyPeer == nil { //cannot progres to next level
		d.logger.Infof("Lowest latency peer is nil")
		if d.bestPeerlastLevel == nil { // cannot fallback to previous level
			d.babel.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
			return
		}
		lowestLatencyPeerParent := d.joinMap[d.bestPeerlastLevel.peer.String()]
		d.bestPeerlastLevel = lowestLatencyPeerParent
		d.attemptProgress()
		return
	}

	d.lastLevelProgress = time.Now()
	d.logger.Infof(
		"Lowest Latency Peer: %s , Latency: %d",
		lowestLatencyPeer.peer.String(),
		lowestLatencyPeer.peer.MeasuredLatency,
	)

	if lowestLatencyPeer.parent == nil { // landmarks
		if lowestLatencyPeer.peer.NrChildren() < d.config.MinGrpSize {
			d.logger.Infof("Joining under peer %s because nodes in this level have not enough members", lowestLatencyPeer.peer.StringWithFields())
			d.myPendingParentInJoin = lowestLatencyPeer.peer
			d.sendJoinAsChildMsg(lowestLatencyPeer.peer.PeerWithIDChain, lowestLatencyPeer.peer.MeasuredLatency, false, false)
			return
		}
		for _, v := range nextLevelPeers[idx:] {
			d.unwatchPeers(v.peer)
		}
		d.progressToNextLevel(lowestLatencyPeer)
		return
	}

	lowestLatencyPeerParent := d.joinMap[lowestLatencyPeer.parent.String()]
	if lowestLatencyPeerParent.peer.MeasuredLatency < lowestLatencyPeer.peer.MeasuredLatency ||
		lowestLatencyPeer.peer.NrChildren() == 0 {

		if lowestLatencyPeerParent.peer.MeasuredLatency < lowestLatencyPeer.peer.MeasuredLatency {
			d.logger.Infof("Joining under peer %s because latency to parent is lower than to its children", lowestLatencyPeerParent.peer.StringWithFields())
		} else {
			d.logger.Infof("Joining under peer %s lowest latency peer does not have children", lowestLatencyPeerParent.peer.StringWithFields())
		}

		d.myPendingParentInJoin = lowestLatencyPeerParent.peer
		d.sendJoinAsChildMsg(lowestLatencyPeerParent.peer.PeerWithIDChain, lowestLatencyPeerParent.peer.MeasuredLatency, false, false)
		return
	}

	for _, v := range nextLevelPeers {
		d.unwatchPeers(v.peer)
	}

	d.logger.Infof("Advancing to children of %s because the lowest latency peer is in lower level and current level node has enough members", lowestLatencyPeer.peer.StringWithFields())
	d.progressToNextLevel(lowestLatencyPeer)
}

func (d *DemmonTree) unwatchPeers(toUnwatch ...*MeasuredPeer) {
	for _, currPeer := range toUnwatch {
		d.logger.Infof("Unwatching peer %s", toUnwatch)
		d.addToMeasuredPeers(currPeer)
		d.nodeWatcher.Unwatch(currPeer, d.ID())
	}
}

func (d *DemmonTree) progressToNextLevel(lowestLatencyPeer *PeerWithParentAndChildren) {
	d.logger.Infof("Advancing to next level under %s ", lowestLatencyPeer.peer.StringWithFields())
	d.bestPeerlastLevel = lowestLatencyPeer
	for _, p := range lowestLatencyPeer.children {

		d.joinTimeoutTimerIds[p.String()] = d.babel.RegisterTimer(
			d.ID(),
			NewJoinMessageResponseTimeout(d.config.JoinMessageTimeout, p),
		)
		d.joinMap[p.String()] = &PeerWithParentAndChildren{
			peer: NewMeasuredPeer(p, math.MaxInt64),
		}
		d.sendMessageAndMeasureLatency(NewJoinMessage(), p)
	}
}

// aux functions

func (d *DemmonTree) sendMessageAndMeasureLatency(toSend message.Message, destPeer *PeerWithIDChain) {
	d.nodeWatcher.Watch(destPeer, d.ID())
	c := nodeWatcher.Condition{
		Repeatable:                false,
		CondFunc:                  func(nodeWatcher.NodeInfo) bool { return true },
		EvalConditionTickDuration: 1000 * time.Millisecond,
		Notification:              NewPeerMeasuredNotification(destPeer, true),
		Peer:                      destPeer,
		EnableGracePeriod:         false,
		ProtoId:                   d.ID(),
	}
	// d.logger.Infof("Doing NotifyOnCondition for node %s...", p.String())
	d.nodeWatcher.NotifyOnCondition(c)
	d.sendMessageTmpTCPChan(toSend, destPeer)
}

func (d *DemmonTree) sendMessageTmpTCPChan(toSend message.Message, destPeer peer.Peer) {
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	d.babel.SendMessageSideStream(toSend, destPeer, destPeer.ToTCPAddr(), d.ID(), d.ID())
}

func (d *DemmonTree) sendMessageTmpUDPChan(toSend message.Message, destPeer peer.Peer) {
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	d.babel.SendMessageSideStream(toSend, destPeer, destPeer.ToUDPAddr(), d.ID(), d.ID())
}

func (d *DemmonTree) sendMessage(toSend message.Message, destPeer peer.Peer) {
	d.logger.Infof("Sending message type %s to: %s", reflect.TypeOf(toSend), destPeer.String())
	d.babel.SendMessage(toSend, destPeer, d.ID(), d.ID(), false)
}

func (d *DemmonTree) sendMessageAndDisconnect(toSend message.Message, destPeer peer.Peer) {
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	d.babel.SendMessageAndDisconnect(toSend, destPeer, d.ID(), d.ID())
}

func (d *DemmonTree) getPeersInNextLevelByLat(lastLevelPeer *PeerWithParentAndChildren) (bool, []*PeerWithParentAndChildren) {
	measuredPeersInLvl := make([]*PeerWithParentAndChildren, 0)

	if lastLevelPeer == nil {
		d.logger.Infof("Getting peers in next level with lastLevelPeer == nil")
		for idx, l := range d.config.Landmarks {

			peerWithChildren, responded := d.joinMap[l.String()]
			if !responded {
				return false, nil
			}
			if peerWithChildren.peer.MeasuredLatency == math.MaxInt64 {
				return false, nil
			}

			d.self.Coordinates[idx] = uint64(peerWithChildren.peer.MeasuredLatency)
			d.updateSelfVersion()
			d.logger.Infof("My Coordinates: %+v", d.self.Coordinates)
			measuredPeersInLvl = append(measuredPeersInLvl, peerWithChildren)
		}
		sort.SliceStable(measuredPeersInLvl, func(i, j int) bool {
			return measuredPeersInLvl[i].peer.MeasuredLatency < measuredPeersInLvl[j].peer.MeasuredLatency
		})
		return true, measuredPeersInLvl
	}

	d.logger.Infof("Getting peers in next level with lastLevelPeer: %s", getStringOrNil(lastLevelPeer.peer.PeerWithIDChain))
	for _, aux := range lastLevelPeer.children {
		childrenStr := aux.String()
		if c, ok := d.joinMap[childrenStr]; ok {
			if c.peer.MeasuredLatency == math.MaxInt64 {
				return false, nil
			}
			measuredPeersInLvl = append(measuredPeersInLvl, c)
		}
	}
	sort.SliceStable(measuredPeersInLvl, func(i, j int) bool {
		return measuredPeersInLvl[i].peer.MeasuredLatency < measuredPeersInLvl[j].peer.MeasuredLatency
	})

	return true, measuredPeersInLvl
}

func (d *DemmonTree) fallbackToPreviousLevel(node peer.Peer) {
	peerParent, ok := d.joinMap[node.String()]
	if !ok {
		d.joinOverlay()
		return
	}
	info, err := d.nodeWatcher.GetNodeInfo(peerParent.parent)
	var peerLat time.Duration
	if err != nil {
		d.logger.Errorf("Peer %s has no latency measurement", node.String())
		peerLat = 0
	} else {
		peerLat = info.LatencyCalc().CurrValue()
	}

	d.myPendingParentInJoin = peerParent.peer
	d.sendJoinAsChildMsg(peerParent.parent, peerLat, true, false)
}

func (d *DemmonTree) fallbackToMeasuredPeers() {
	var bestPeer *MeasuredPeer

	measuredPeersArr := make(MeasuredPeersByLat, 0, len(d.measuredPeers))
	for _, p := range d.measuredPeers {

		if d.isNeighbour(p.Peer) || p.IsDescendentOf(d.self.Chain()) || peer.PeersEqual(p, d.babel.SelfPeer()) {
			delete(d.measuredPeers, p.String())
			continue
		}

		measuredPeersArr = append(measuredPeersArr, p)
	}
	sort.Sort(measuredPeersArr)
	if len(measuredPeersArr) == 0 {
		d.logger.Warn("Rejoining overlay due to not having more measured peers")
		d.joinOverlay()
		return
	}

	bestPeer = measuredPeersArr[0]
	d.logger.Infof(" falling back to %s : %s", bestPeer.String(), bestPeer.MeasuredLatency)
	d.myPendingParentInRecovery = bestPeer.PeerWithIDChain
	d.sendJoinAsChildMsg(bestPeer.PeerWithIDChain, bestPeer.MeasuredLatency, true, false)
}
