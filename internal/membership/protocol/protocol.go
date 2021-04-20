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

const ProtoID = 1000
const ProtoName = "DemonTree"

type DemmonTreeConfig = struct {
	MaxDiffForBWScore int
	BandwidthScore    int
	UseBwScore        bool

	Landmarks                   []*PeerWithIDChain
	LandmarkRedialTimer         time.Duration
	JoinMessageTimeout          time.Duration
	MaxRetriesJoinMsg           int
	ParentRefreshTickDuration   time.Duration
	ChildrenRefreshTickDuration time.Duration
	RejoinTimerDuration         time.Duration

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
	EmitWalkProbability                    float64
	BiasedWalkProbability                  float64
	AttemptImprovePositionProbability      float64
	MinGrpSize                             uint16
	MaxGrpSize                             uint16

	UnderpopulatedGroupTimerDuration time.Duration
}

type PeerWithParentAndChildren struct {
	parent   *PeerWithIDChain
	peer     *MeasuredPeer
	children []*PeerWithIDChain
	replied  bool
}

func (p *PeerWithParentAndChildren) String() string {
	return p.peer.String()
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
	myPendingParentInJoin *PeerWithParentAndChildren
	rejoinTimerActive     bool

	// state TODO refactor to class
	self                         *PeerWithIDChain
	myGrandParent                *PeerWithIDChain
	myParent                     *PeerWithIDChain
	myPendingParentInRecovery    *PeerWithIDChain
	myPendingParentInAbsorb      *PeerWithIDChain
	myPendingParentInClimb       *PeerWithIDChain
	myPendingParentInImprovement *MeasuredPeer

	myChildren          map[string]*PeerWithIDChain
	mySiblings          map[string]*PeerWithIDChain
	myChildrenLatencies map[string]MeasuredPeersByLat

	measuringPeers map[string]bool
	measuredPeers  map[string]*MeasuredPeer
	eView          map[string]*PeerWithIDChain

	joinTimeoutTimerIds map[string]int
	landmark            bool
}

const (
	DebugTimerDuration = 3 * time.Second
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

	d.self = NewPeerWithIDChain(nil, d.babel.SelfPeer(), 0, 0, make(Coordinates, len(d.config.Landmarks)), d.config.BandwidthScore, 0)
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
			d.config.BandwidthScore,
			0,
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

		d.babel.RegisterPeriodicTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration), false)
		d.babel.RegisterPeriodicTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration), false)
		d.babel.RegisterPeriodicTimer(d.ID(), NewDebugTimer(DebugTimerDuration), false)
		d.babel.RegisterPeriodicTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration), false)
		return
	}

	d.babel.RegisterPeriodicTimer(d.ID(), NewUnderpopupationTimer(d.config.UnderpopulatedGroupTimerDuration), false)
	d.babel.RegisterPeriodicTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration), false)
	d.babel.RegisterPeriodicTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration), false)
	d.babel.RegisterPeriodicTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration), false)
	d.babel.RegisterPeriodicTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout), false)
	d.babel.RegisterPeriodicTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration), false)
	d.babel.RegisterPeriodicTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration), false)
	d.babel.RegisterPeriodicTimer(d.ID(), NewDebugTimer(DebugTimerDuration), false)
	d.joinOverlay()
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
	d.babel.RegisterTimerHandler(d.ID(), underpopulationTimerID, d.handleUnderpopulatedTimer)

	// other
	d.babel.RegisterTimerHandler(d.ID(), debugTimerID, d.handleDebugTimer)
	d.babel.RegisterMessageHandler(d.ID(), BroadcastMessage{}, d.handleBroadcastMessage)
}

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {
	d.rejoinTimerActive = false
	if !d.joined {
		d.logger.Info("-------------Rejoining overlay---------------")
		d.joinOverlay()
	}
}

func (d *DemmonTree) handleUnderpopulatedTimer(joinTimer timer.Timer) {
	// d.logger.Info("underpop timer...")
	nrPeersInGrp := len(d.mySiblings) + 1
	if d.myGrandParent == nil || d.myParent == nil {
		d.logger.Info("underpop timer returning ( d.myGrandParent == nil || d.myParent == nil )")
		return
	}

	if !d.joined || d.myPendingParentInAbsorb != nil || d.myPendingParentInImprovement != nil {
		d.logger.Info("underpop timer returning (!d.joined || d.myPendingParentInAbsorb != nil || d.myPendingParentInImprovement != nil)")
		return
	}

	if nrPeersInGrp >= int(d.config.MinGrpSize) {
		d.logger.Info("underpop timer returning (nrPeersInGrp < MinGrpSize)")
		return
	}

	r := rand.Float64()
	probToStay := float64(nrPeersInGrp) / float64(d.config.MinGrpSize)
	if r < probToStay {
		d.logger.Infof("underpop timer returning (r=%f > prob=%f)", r, probToStay)
		return
	}

	d.logger.Info("climbing...")
	d.myPendingParentInClimb = d.myGrandParent
	d.sendJoinAsChildMsg(d.myGrandParent, 0, false, false)
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
	p, ok := d.joinMap[peerMeasured.String()]
	if !ok {
		d.logger.Warnf("Got peer measured notification in join but target %s is not in joinMap", peerMeasured.String())
		return
	}
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
	p.peer.Latency = currNodeStats.LatencyCalc().CurrValue()
	d.attemptProgress()
}

func (d *DemmonTree) handleMeasuringPeerInEViewFinish(peerMeasured *PeerWithIDChain) {
	delete(d.measuringPeers, peerMeasured.String())
	if d.isNeighbour(peerMeasured.Peer) {
		return
	}

	defer d.nodeWatcher.Unwatch(peerMeasured.Peer, d.ID())
	currNodeStats, err := d.nodeWatcher.GetNodeInfo(peerMeasured)
	if err != nil {
		d.logger.Error(err.Reason())
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

	// d.logger.Infof("d.measuredPeers:  %+v:", d.measuredPeers)
}

func (d *DemmonTree) getAvgChildrenBW() int {
	if len(d.myChildren) == 0 {
		return 0
	}
	tmp := 0
	for _, c := range d.myChildren {
		tmp += c.bandwidth
	}
	return tmp / len(d.myChildren)

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
		coordsCopy := d.self.Coordinates
		coordsCopy[idx] = uint64(landmarkStats.LatencyCalc().CurrValue().Milliseconds())
		d.self = NewPeerWithIDChain(d.self.chain, d.self, d.self.nChildren, d.self.version+1, coordsCopy, d.config.BandwidthScore, d.getAvgChildrenBW())
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
		// d.logger.Infof("Sending children: %+v to %s", childrenArr, child.StringWithFields())
		toSend := NewUpdateParentMessage(
			d.myParent,
			d.self,
			child.Chain()[len(child.Chain())-1],
			getMapAsPeerWithIDChainArray(d.myChildren, child),
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
	measuredSiblings := d.getPeerWithIDChainMapAsPeerMeasuredArr(d.mySiblings)
	d.logger.Infof("Sending measured siblings: %+v", measuredSiblings)
	toSend := NewUpdateChildMessage(d.self, measuredSiblings)
	d.sendMessage(toSend, dest)
}

func (d *DemmonTree) handleExternalNeighboringTimer(joinTimer timer.Timer) {
	if !d.joined {
		return
	}

	r := utils.GetRandFloat64()
	if r > d.config.EmitWalkProbability {
		return
	}

	d.logger.Info("ExternalNeighboringTimer trigger")

	neighbors := d.getNeighborsAsPeerWithIDChainArray()
	// d.logger.Infof("d.possibilitiesToSend: %+v:", neighbours)

	sample := getRandSample(d.config.NrPeersInWalkMessage-1, append(neighbors, PeerWithIDChainMapToArr(d.eView)...)...)
	sample[d.self.String()] = d.self

	// r = rand.Float32()
	var msgToSend message.Message
	var peerToSendTo *PeerWithIDChain
	// if r < d.config.BiasedWalkProbability {
	// 	msgToSend = NewBiasedWalkMessage(uint16(d.config.RandomWalkTTL), selfPeerWithChain, sample)
	// 	peerToSendTo = getBiasedPeerExcluding(possibilitiesToSend, selfPeerWithChain)
	// } else {
	sampleToSend := PeerWithIDChainMapToArr(sample)
	msgToSend = NewRandomWalkMessage(uint16(d.config.RandomWalkTTL), d.self, sampleToSend)
	peerToSendTo = getRandomExcluding(
		getExcludingDescendantsOf(neighbors, d.self.Chain()),
		map[string]bool{d.self.String(): true},
	)

	duplChecker := map[string]bool{}
	for _, elem := range sampleToSend {
		k := elem.String()
		if _, ok := duplChecker[k]; ok {
			d.logger.Panicf("Duplicate entry in sampleToSend: %+v", sampleToSend)
		}
		duplChecker[k] = true
	}

	// }

	if peerToSendTo == nil {
		d.logger.Error("peerToSendTo is nil")
		return
	}
	d.logger.Infof("sending random walk to %s", peerToSendTo.StringWithFields())
	d.sendMessage(msgToSend, peerToSendTo.Peer)
}

func (d *DemmonTree) handleEvalMeasuredPeersTimer(evalMeasuredPeersTimer timer.Timer) {

	if d.isChangingParent() {
		d.logger.Warn("EvalMeasuredPeersTimer returning due to already being changing parent..")
		return
	}

	// r := utils.GetRandFloat64()
	// if r > d.config.AttemptImprovePositionProbability {
	// 	d.logger.Info("EvalMeasuredPeersTimer returning due to  r > d.config.AttemptImprovePositionProbability")
	// 	return
	// }

	measuredPeersArr := make(MeasuredPeersByLat, 0, len(d.measuredPeers))

	for _, p := range d.measuredPeers {
		if d.self.IsDescendentOf(p.chain) {
			delete(d.measuredPeers, p.String())
			continue
		}

		if d.isNeighbour(p) {
			delete(d.measuredPeers, p.String())
			continue
		}

		// if d.self.IsDescendentOf(p.chain) {
		// delete(d.measuredPeers, p.String())
		// continue
		// }

		measuredPeersArr = append(measuredPeersArr, p)
	}

	if len(measuredPeersArr) == 0 {
		d.logger.Warn("returning due to len(measuredPeersArr) == 0")
		return
	}

	sort.Sort(measuredPeersArr)
	parentAsMeasuredPeer, err := d.getPeerWithChainAsMeasuredPeer(d.myParent)
	if err != nil {
		d.logger.Errorf("Do not have latency measurement for parent")
		return
	}

	d.logger.Info("Evaluating measuredPeers...")
	d.logger.Infof("d.measuredPeers:  %s:", measuredPeersArr.String())

	for _, measuredPeer := range measuredPeersArr {

		// parent latency is higher than latency to peer

		if !measuredPeer.HasBetterLatencyThan(parentAsMeasuredPeer, d.config.MinLatencyImprovementToImprovePosition) {
			continue // can break because measuredPeersArr is sorted, therefore, any remaining measured peer's latencies are higher
		}

		if measuredPeer.nChildren == 0 {
			// d.logger.Infof("Not improving towards %s because it has no children", measuredPeer.StringWithFields())
			continue
		}

		// go down ( if my level is 3, and measured peer is 5, i am higher in tree)
		if d.self.Chain().Level() <= measuredPeer.Chain().Level() {
			if len(d.myChildren) > 0 {
				d.logger.Infof("Not improving towards %s because i have too many children", measuredPeer.StringWithFields())
				continue
			}
			if d.config.UseBwScore {
				if measuredPeer.bandwidth <= d.self.bandwidth {
					d.logger.Infof("Not improving towards %s because the peer bandwidth and level is lower than mine", measuredPeer.StringWithFields())
					continue
				}
			}
		}

		if d.config.UseBwScore {
			if d.self.Chain().Level() > measuredPeer.Chain().Level()+1 {
				if d.self.bandwidth < measuredPeer.avgChildrenBW {
					d.logger.Infof("Not improving towards %s because the peer is level is higher than mine and avg children bandwidth is higher than my bw", measuredPeer.StringWithFields())
					diff := measuredPeer.avgChildrenBW - d.self.bandwidth
					if diff > d.config.MaxDiffForBWScore {
						continue
					}
				}
			}
		}

		if len(d.myChildren) > 0 {
			// cannot join as child because due to having children
			if measuredPeer.Chain().Level() >= d.self.Chain().Level() {
				continue // TODO possibly send message for the other node to join me as child
			}
		}

		d.logger.Infof("Improving position towards: %s", measuredPeer.StringWithFields())
		d.logger.Infof("self level: %d", d.self.Chain().Level())
		d.logger.Infof("target peer level: %d", measuredPeer.Chain().Level())

		d.logger.Infof("latencyImprovement: %s", parentAsMeasuredPeer.Latency-measuredPeer.Latency)
		d.logger.Infof("parentLatency: %s", parentAsMeasuredPeer.Latency)
		d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.Latency)
		aux := *measuredPeer
		d.myPendingParentInImprovement = &aux
		d.sendJoinAsChildMsg(measuredPeer.PeerWithIDChain, measuredPeer.Latency, false, true)
		return
	}
}

func (d *DemmonTree) handleMeasureNewPeersTimer(measureNewPeersTimer timer.Timer) {

	if len(d.eView) == 0 {
		d.logger.Infof("handleMeasureNewPeersTimer returning because len(eView) == 0")
		return
	}

	for k, p := range d.eView {
		if d.self.IsDescendentOf(p.chain) || d.isNeighbour(p) {
			delete(d.eView, k)
		}
	}

	peersSorted := PeerWithIDChainMapToArr(d.eView)
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

	peersRandom := PeerWithIDChainMapToArr(d.eView)
	rand.Shuffle(len(peersRandom), func(i, j int) { peersRandom[i], peersRandom[j] = peersRandom[j], peersRandom[i] })
	nrMeasuredRand := 0

	for i := 0; i < len(peersRandom) &&
		i < d.config.NrPeersToMeasureRandom &&
		nrMeasuredRand < d.config.NrPeersToMeasureRandom; i++ {

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

	d.measuringPeers[p.String()] = true
	delete(d.eView, p.String())
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
	return true
}

func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {

	type absorbStats struct {
		TotalLatency time.Duration
		Absorber     *MeasuredPeer
		PeersToKick  MeasuredPeersByLat
	}

	// nrPeersToKick := d.config.NrPeersToBecomeParentInAbsorb * int(d.config.NrPeersToBecomeChildrenPerParentInAbsorb)

	if d.isChangingParent() {
		d.logger.Warn("handleCheckChildrenSize timer trigger returning due to being changing parent")
		return
	}

	if len(d.myChildren) < d.config.NrPeersToBecomeChildrenPerParentInAbsorb+int(d.config.MinGrpSize) {
		d.logger.Info("handleCheckChildrenSize timer trigger returning due to goup size being too small")
		return
	}

	peersToKickPerAbsorber := make([]*absorbStats, 0)

	if d.config.UseBwScore {
		childrenAsMeasuredPeers := d.getPeerWithIDChainMapAsPeerMeasuredArr(d.myChildren)
		sort.SliceStable(childrenAsMeasuredPeers, func(i, j int) bool {
			return childrenAsMeasuredPeers[i].bandwidth > childrenAsMeasuredPeers[j].bandwidth
		})
		// d.logger.Infof("after sorting childrenAsMeasuredPeers: %+v", childrenAsMeasuredPeers)
		for _, child := range childrenAsMeasuredPeers {
			// if len(peersToKickPerAbsorber) == d.config.NrPeersToBecomeParentInAbsorb {
			// 	break
			// }
			peersToKickPerAbsorber = append([]*absorbStats{{
				TotalLatency: 0,
				Absorber:     child,
				PeersToKick:  []*MeasuredPeer{},
			}}, peersToKickPerAbsorber...)
		}
	} else {
		childrenAsMeasuredPeers := d.getPeerWithIDChainMapAsPeerMeasuredArr(d.myChildren)
		for _, child := range childrenAsMeasuredPeers {
			// if len(peersToKickPerAbsorber) == d.config.NrPeersToBecomeParentInAbsorb {
			// 	break
			// }
			peersToKickPerAbsorber = append([]*absorbStats{{
				TotalLatency: 0,
				Absorber:     child,
				PeersToKick:  []*MeasuredPeer{}},
			}, peersToKickPerAbsorber...)
		}
	}

	d.logger.Infof("peersToKickPerAbsorber: %+v", peersToKickPerAbsorber)
	// alreadyKicked := func(toFind string) bool {
	// 	for _, v := range peersToKickPerAbsorber {
	// 		if v.Absorber.String() == toFind {
	// 			return true
	// 		}

	// 		for _, v2 := range v.PeersToKick {
	// 			if v2.String() == toFind {
	// 				return true
	// 			}
	// 		}
	// 	}
	// 	return false
	// }

	type edge struct {
		peer1   *MeasuredPeer
		peer2   *MeasuredPeer
		latency time.Duration
	}

	edgeList := []edge{}

	for _, peerAbsorberStats := range peersToKickPerAbsorber {

		peerAbsorber := peerAbsorberStats.Absorber
		peerAbsorberSiblingLatencies := d.myChildrenLatencies[peerAbsorber.String()]
		sort.Sort(peerAbsorberSiblingLatencies)

		// d.logger.Infof("peer %s sibling latencies: %s", peerAbsorber.StringWithFields(), peerAbsorberSiblingLatencies.String())
		for _, candidateToKick := range peerAbsorberSiblingLatencies {

			if candidateToKick == nil {
				continue
			}

			if _, isChild := d.myChildren[candidateToKick.String()]; !isChild {
				continue
			}

			if peer.PeersEqual(candidateToKick, peerAbsorber) {
				continue
			}

			if candidateToKick.Latency == 0 || candidateToKick.Latency == math.MaxInt64 {
				continue
			}

			candidateToKickFromSelfPOV, err := d.getPeerWithChainAsMeasuredPeer(candidateToKick.PeerWithIDChain)
			if err != nil {
				continue
			}

			if candidateToKickFromSelfPOV.Latency < candidateToKick.Latency {
				// latencyDowngrade := candidateToKick.Latency - candidateToKickFromSelfPOV.Latency
				// if latencyDowngrade > d.config.MinLatencyImprovementToImprovePosition {
				continue
				// }
			}

			edgeList = append(edgeList, edge{
				peer1:   peerAbsorber,
				peer2:   candidateToKick,
				latency: candidateToKick.Latency,
			})

			peerAbsorberStats.PeersToKick = append(peerAbsorberStats.PeersToKick, candidateToKick)
			peerAbsorberStats.TotalLatency += candidateToKick.Latency

			// if len(peerAbsorberStats.PeersToKick) == d.config.NrPeersToBecomeChildrenPerParentInAbsorb {
			// 	break
			// }
		}
	}

	sort.SliceStable(edgeList, func(i, j int) bool {
		return edgeList[i].latency < edgeList[j].latency
	})
	alreadyKicked := map[string]bool{}
	alreadyParent := map[string]bool{}
	potentialChildren := map[string][]*PeerWithIDChain{}

	deleteFromPotentialChildren := func(toCompare string) {
		for k, potentialChildrenArr := range potentialChildren {
			i := 0
			for _, c := range potentialChildrenArr {
				if c.String() == toCompare {
					continue
				}
				potentialChildrenArr[i] = c
				i += 1
			}
			potentialChildren[k] = potentialChildrenArr[:i]
		}
	}

	for _, edge := range edgeList {
		if _, ok := alreadyKicked[edge.peer1.String()]; ok {
			continue
		}
		if _, ok := alreadyKicked[edge.peer2.String()]; ok {
			continue
		}
		if edge.peer1.bandwidth > edge.peer2.bandwidth {
			if edge.peer1.nChildren > 0 {
				d.sendMessage(NewAbsorbMessage(edge.peer1.PeerWithIDChain), edge.peer2)
				alreadyKicked[edge.peer2.String()] = true
				alreadyParent[edge.peer1.String()] = true
				continue
			}
			potentialChildren[edge.peer1.String()] = append(potentialChildren[edge.peer1.String()], edge.peer2.PeerWithIDChain)
			if len(potentialChildren[edge.peer1.String()]) == int(d.config.MinGrpSize) {
				alreadyParent[edge.peer1.String()] = true
				for _, newC := range potentialChildren[edge.peer1.String()] {
					d.sendMessage(NewAbsorbMessage(edge.peer1.PeerWithIDChain), newC)
					alreadyKicked[newC.String()] = true
					deleteFromPotentialChildren(newC.String())
				}
				deleteFromPotentialChildren(edge.peer1.String())
				potentialChildren[edge.peer1.String()] = []*PeerWithIDChain{}
			}
		} else {
			if edge.peer2.nChildren > 0 {
				d.sendMessage(NewAbsorbMessage(edge.peer2.PeerWithIDChain), edge.peer1)
				alreadyKicked[edge.peer1.String()] = true
				alreadyParent[edge.peer2.String()] = true
				continue
			}
			potentialChildren[edge.peer2.String()] = append(potentialChildren[edge.peer2.String()], edge.peer1.PeerWithIDChain)
			if len(potentialChildren[edge.peer2.String()]) == int(d.config.MinGrpSize) {
				alreadyParent[edge.peer2.String()] = true
				for _, newC := range potentialChildren[edge.peer2.String()] {
					d.sendMessage(NewAbsorbMessage(edge.peer2.PeerWithIDChain), newC)
					alreadyKicked[newC.String()] = true
					deleteFromPotentialChildren(newC.String())
				}
				deleteFromPotentialChildren(edge.peer2.String())
				potentialChildren[edge.peer2.String()] = []*PeerWithIDChain{}
			}
		}
	}
	// return

	// d.logger.Infof("handleCheckChildrenSizeTimer results: %+v", peersToKickPerAbsorber)
	// sort.SliceStable(peersToKickPerAbsorber, func(i, j int) bool {
	// 	return peersToKickPerAbsorber[i].TotalLatency < peersToKickPerAbsorber[j].TotalLatency
	// })

	// for _, absorberStats := range peersToKickPerAbsorber {
	// 	if absorberStats.Absorber.nChildren == 0 {
	// 		if len(absorberStats.PeersToKick) < int(d.config.NrPeersToBecomeChildrenPerParentInAbsorb) {
	// 			d.logger.Infof("peer %s does not have enough peers (%d/%d) to become parent in absorb", absorberStats.Absorber.StringWithFields(), len(peersToKickPerAbsorber), int(d.config.NrPeersToBecomeChildrenPerParentInAbsorb))
	// 			continue
	// 		}
	// 	}

	// 	absorber := absorberStats.Absorber
	// 	for _, toKickPeer := range absorberStats.PeersToKick {
	// 		d.logger.Infof(
	// 			"Sending absorb message with peerToAbsorb: %s, peerAbsorber: %s",
	// 			toKickPeer.StringWithFields(),
	// 			absorber.StringWithFields(),
	// 		)
	// 		toSend := NewAbsorbMessage(absorber.PeerWithIDChain)
	// 		d.sendMessage(toSend, toKickPeer)
	// 	}
	// 	return
	// }
}

// func (d *DemmonTree) computeChildrenGraphMST() {
// 	intMapper := map[string]int{}
// 	childrenAsMeasuredPeers := d.getPeerWithIDChainMapAsPeerMeasuredArr(d.myChildren)
// 	g := graph.New(len(childrenAsMeasuredPeers) + 1)
// 	intMapper[d.self.String()] = 0
// 	i := 1
// 	for _, childrenAsMeasuredPeer := range childrenAsMeasuredPeers {
// 		intMapper[childrenAsMeasuredPeer.String()] = i
// 		i += 1
// 	}

// 	for _, childrenAsMeasuredPeer := range childrenAsMeasuredPeers {
// 		origVertex := intMapper[childrenAsMeasuredPeer.String()]
// 		g.AddBothCost(origVertex, 0, childrenAsMeasuredPeer.Latency.Nanoseconds())
// 		childrenSiblingLatencies, ok := d.myChildrenLatencies[childrenAsMeasuredPeer.String()]
// 		if !ok {
// 			continue
// 		}
// 		for _, v := range childrenSiblingLatencies {
// 			destVertex := intMapper[v.String()]
// 			g.AddBothCost(origVertex, destVertex, v.Latency.Nanoseconds())
// 		}
// 	}
// 	fmt.Println(g.String())

// 	graph.MST(g)
// }

// message handlers

func (d *DemmonTree) handleRandomWalkMessage(sender peer.Peer, m message.Message) {
	randWalkMsg := m.(RandomWalkMessage)

	nrPeersToMerge := d.config.NrPeersToMergeInWalkSample
	nrPeersToAdd := d.config.NrPeersToMergeInWalkSample
	var sampleToSend,
		neighboursWithoutSenderDescendants []*PeerWithIDChain

	duplChecker := map[string]bool{}
	for _, elem := range randWalkMsg.Sample {
		k := elem.String()
		if _, ok := duplChecker[k]; ok {
			d.logger.Panicf("Duplicate entry in randWalkMsg.Sample: %+v", randWalkMsg.Sample)
		}
		duplChecker[k] = true
	}

	defer func() {
		duplChecker := map[string]bool{}
		for _, elem := range sampleToSend {
			k := elem.String()
			if _, ok := duplChecker[k]; ok {
				d.logger.Panicf("Duplicate entry in sampleToSend: %+v", sampleToSend)
			}
			duplChecker[k] = true
		}
	}()

	// TTL == 0
	if randWalkMsg.TTL == 0 {
		sampleToSend, _ = d.mergeSampleWithEview(
			randWalkMsg.Sample,
			randWalkMsg.Sender,
			nrPeersToMerge,
			nrPeersToAdd,
		)
		// d.logger.Infof("random walk TTL is 0. Sending random walk reply to original sender: %s", randWalkMsg.Sender.String())
		d.sendMessageTmpTCPChan(NewWalkReplyMessage(sampleToSend), randWalkMsg.Sender)
		return
	}

	if int(randWalkMsg.TTL) > d.config.NrHopsToIgnoreWalk {
		nrPeersToMerge = 0
	}

	sampleToSend, neighboursWithoutSenderDescendants = d.mergeSampleWithEview(
		randWalkMsg.Sample,
		randWalkMsg.Sender,
		nrPeersToMerge,
		nrPeersToAdd,
	)

	p := getRandomExcluding(
		neighboursWithoutSenderDescendants,
		map[string]bool{randWalkMsg.Sender.String(): true, d.self.String(): true, sender.String(): true},
	)

	if p == nil {
		d.logger.Infof(
			"have no peers to forward message... merging and sending random walk reply to %s",
			randWalkMsg.Sender.String(),
		)
		d.sendMessageTmpTCPChan(NewWalkReplyMessage(sampleToSend), randWalkMsg.Sender)
		return
	}

	d.sendMessage(NewRandomWalkMessage(randWalkMsg.TTL-1, randWalkMsg.Sender, sampleToSend), p)
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

	if d.isChangingParent() {
		d.logger.Warn("Got absorbMessage but returning due to already having pending")
		return
	}

	newParent := absorbMessage.peerAbsorber
	d.logger.Infof("Got absorbMessage with peer absorber: %+v from %s", newParent.StringWithFields(), sender.String())

	if sibling, ok := d.mySiblings[newParent.String()]; ok && sibling.outConnActive {
		delete(d.mySiblings, newParent.String())
		var peerLat time.Duration
		if nodeInfo, err := d.nodeWatcher.GetNodeInfo(sibling); err == nil {
			peerLat = nodeInfo.LatencyCalc().CurrValue()
		}
		d.myPendingParentInAbsorb = newParent
		d.myPendingParentInAbsorb.inConnActive = sibling.inConnActive
		d.myPendingParentInAbsorb.outConnActive = sibling.outConnActive
		toSend := NewJoinAsChildMessage(d.config.BandwidthScore, d.self, peerLat, newParent.Chain(), false, false)
		d.sendMessage(toSend, newParent)
	} else {
		d.myPendingParentInAbsorb = newParent
		toSend := NewJoinAsChildMessage(d.config.BandwidthScore, d.self, 0, newParent.Chain(), false, false)
		d.sendMessageTmpTCPChan(toSend, newParent)
	}
}

func (d *DemmonTree) handleDisconnectAsChildMsg(sender peer.Peer, m message.Message) {
	dacMsg := m.(DisconnectAsChildMessage)
	d.logger.Infof("got DisconnectAsChildMsg %+v from %s", dacMsg, sender.String())
	if _, ok := d.myChildren[sender.String()]; ok {
		d.removeChild(sender, false)
	}
}

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {
	toSend := NewJoinReplyMessage(getMapAsPeerWithIDChainArray(d.myChildren), d.self, d.myParent)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) isChangingParent() bool {
	return d.myPendingParentInJoin != nil ||
		d.myPendingParentInRecovery != nil ||
		d.myPendingParentInAbsorb != nil ||
		d.myPendingParentInClimb != nil ||
		d.myPendingParentInImprovement != nil
}

func (d *DemmonTree) handleJoinMessageResponseTimeout(t timer.Timer) {
	p := t.(*peerJoinMessageResponseTimeout).Peer
	if _, ok := d.joinTimeoutTimerIds[p.String()]; ok {
		d.logger.Warnf("Peer %s timed out responding to join message", p.String())
		d.handlePeerDownInJoin(p)
		return
	}
	d.logger.Warnf("Got join message response timeout for node %s but node responded meanwhile...", p.String())
}

func (d *DemmonTree) handlePeerDownInJoin(p peer.Peer) {
	delete(d.joinMap, p.String())
	d.nodeWatcher.Unwatch(p, d.ID())
	if _, ok := d.joinTimeoutTimerIds[p.String()]; ok {
		d.babel.CancelTimer(d.joinTimeoutTimerIds[p.String()])
		delete(d.joinTimeoutTimerIds, p.String())
	}
	_, isLandmark := d.isLandmark(p)
	if isLandmark {
		if !d.rejoinTimerActive {
			d.rejoinTimerActive = true
			d.babel.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
		}
		return
	}
	d.attemptProgress()
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(JoinReplyMessage)

	if timerID, ok := d.joinTimeoutTimerIds[sender.String()]; ok {
		d.babel.CancelTimer(timerID)
		delete(d.joinTimeoutTimerIds, sender.String())
	}

	d.logger.Infof("Got joinReply: %+v from %s", replyMsg, sender.String())
	if d.joined {
		d.logger.Errorf("Got joinReply: %+v but already joined overylay... %s", replyMsg, sender.String())
		return
	}

	p, ok := d.joinMap[sender.String()]
	if !ok {
		d.logger.Errorf("Discarding peer %s from join process as it is not in join map", sender.String())
		return
	}

	if !p.peer.chain.Equal(replyMsg.Sender.Chain()) {
		d.logger.Errorf("Discarding peer %s from join process as its chain changed", p.peer.StringWithFields())
		d.handlePeerDownInJoin(replyMsg.Sender)
		return
	}

	p.children = replyMsg.Children
	p.parent = replyMsg.Parent
	p.peer.PeerWithIDChain = replyMsg.Sender
	p.replied = true
	d.attemptProgress()
}

func (d *DemmonTree) canBecomeParentOf(other *PeerWithIDChain, expectedChain PeerIDChain, isRecovery, isImprovement bool) bool {

	if d.self.IsDescendentOf(other.Chain()) {
		d.logger.Warnf(
			"cannot become parent of %s because im descendent of sender, sender chain: %+v",
			other.StringWithFields(),
			other.Chain(),
		)
		return false
	}

	if isImprovement {

		if len(d.myChildren) == 0 {
			d.logger.Warnf("cannot become parent of %s because it is an improvement and i do not have enough children",
				other.StringWithFields())
			return false
		}

		if d.config.UseBwScore {
			// go down ( if other level is 3, and my level is 5, i am lower in tree)
			if other.Chain().Level() <= d.self.Chain().Level() {
				if other.bandwidth > d.self.bandwidth {
					return false
				}
			}

			// go up ( if other level is 5, and my level is 3, i am higher in tree)
			if other.Chain().Level() > d.self.Chain().Level()+1 {
				if other.bandwidth < d.self.avgChildrenBW {
					d.logger.Infof("Not improving towards %s because the peer is level is higher than mine and avg children bandwidth is higher than my bw", d.self.StringWithFields())
					diff := d.self.avgChildrenBW - other.bandwidth
					if diff > d.config.MaxDiffForBWScore {
						return false
					}
				}
			}
		}

	}
	return true
}

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {
	jacMsg := m.(JoinAsChildMessage)
	d.logger.Infof("got JoinAsChildMessage %+v from %s", jacMsg, sender.String())

	if !d.canBecomeParentOf(jacMsg.Sender, jacMsg.ExpectedID, jacMsg.Urgent, jacMsg.Improvement) {
		toSend := NewJoinAsChildMessageReply(false, PeerID{}, d.self.Chain().Level(), d.self, nil, nil)
		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	sibling, isSibling := d.mySiblings[sender.String()]
	var outConnActive, inConnActive bool
	if isSibling {
		delete(d.mySiblings, sender.String())
		d.babel.SendNotification(NewNodeDownNotification(sibling, d.getInView(), false))
		outConnActive = sibling.outConnActive
		inConnActive = sibling.inConnActive
	}

	newChildID := d.addChild(jacMsg.Sender, jacMsg.BWScore, jacMsg.MeasuredLatency, outConnActive, inConnActive)
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
		d.logger.Warnf("Pending Parent In Improvement denied join as child nessage")
		if mp, ok := d.measuredPeers[d.myPendingParentInImprovement.String()]; ok {
			delete(d.measuredPeers, japrMsg.Parent.String())
			d.addToMeasuredPeers(&MeasuredPeer{PeerWithIDChain: japrMsg.Parent, Latency: mp.Latency})
		}
		d.myPendingParentInImprovement = nil
		return
	}

	if d.myPendingParentInClimb != nil && peer.PeersEqual(japrMsg.Parent, d.myPendingParentInClimb) {
		d.logger.Warnf("Pending Parent In climb denied join as child nessage")
		d.myPendingParentInClimb = nil
		return
	}

	// not accepted
	d.handlePeerDown(japrMsg.Parent, false)
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

	if !peer.PeersEqual(sender, d.myParent) &&
		!peer.PeersEqual(sender, d.myPendingParentInAbsorb) &&
		!peer.PeersEqual(sender, d.myPendingParentInClimb) &&
		!peer.PeersEqual(sender, d.myPendingParentInRecovery) &&
		!(d.myPendingParentInJoin != nil && peer.PeersEqual(sender, d.myPendingParentInJoin.peer)) {
		d.logger.Errorf(
			"Received UpdateParentMessage from not my parent (parent:%s sender:%s)",
			getStringOrNil(d.myParent),
			upMsg.Parent.StringWithFields(),
		)
		d.sendMessageTmpTCPChan(NewDisconnectAsChildMessage(), sender)
		return
	}

	if d.myPendingParentInAbsorb != nil {
		d.logger.Infof(
			"Discarding UpdateParentMessage because d.myPendingParentInAbsorb != nil (myPendingParentInAbsorb:%s sender:%s)",
			getStringOrNil(d.myPendingParentInAbsorb),
			upMsg.Parent.StringWithFields(),
		)
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

	d.logger.Infof("Conns before: in: %+v, out: %+v", d.myParent.inConnActive, d.myParent.outConnActive)
	d.myGrandParent = upMsg.GrandParent
	aux := d.myParent
	d.myParent = upMsg.Parent
	d.myParent.outConnActive = aux.outConnActive
	d.myParent.inConnActive = aux.inConnActive
	d.logger.Infof("Conns after: in: %+v, out: %+v", d.myParent.inConnActive, d.myParent.outConnActive)

	myNewChain := append(upMsg.Parent.Chain(), upMsg.ProposedID)
	if !myNewChain.Equal(d.self.Chain()) {
		d.logger.Infof("My chain changed: (%+v -> %+v)", d.self.Chain(), myNewChain)
		d.logger.Infof(
			"My level changed: (%d -> %d)",
			d.self.Chain().Level(),
			upMsg.Parent.Chain().Level()+1,
		) // IMPORTANT FOR VISUALIZER

		d.self = NewPeerWithIDChain(myNewChain, d.self.Peer, d.self.nChildren, d.self.Version()+1, d.self.Coordinates, d.config.BandwidthScore, d.getAvgChildrenBW())
	}

	d.mergeSiblingsWith(upMsg.Siblings, d.myParent)
}

func (d *DemmonTree) handleUpdateChildMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(UpdateChildMessage)
	// d.logger.Infof("got updateChildMessage %+v from %s", m, sender.String())
	child, ok := d.myChildren[sender.String()]

	if !ok {
		d.logger.Errorf(
			"got updateChildMessage %+v from not my children, or my pending children: %s",
			m,
			sender.String(),
		)
		return
	}
	tmp := upMsg.Child
	tmp.inConnActive = child.inConnActive
	tmp.outConnActive = child.outConnActive
	d.myChildren[sender.String()] = tmp
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
	d.handlePeerDown(p, true)
}

func (d *DemmonTree) OutConnDown(p peer.Peer) {
	d.handlePeerDown(p, true)
}

func (d *DemmonTree) handlePeerDownNotification(n notification.Notification) {
	p := n.(SuspectNotification).peerDown
	d.logger.Errorf("peer down %s (PHI >= %f)", p.String(), d.config.PhiLevelForNodeDown)
	d.handlePeerDown(p.Peer, true)
}

func (d *DemmonTree) handlePeerDown(p peer.Peer, crash bool) {

	// special case for parent in recovery
	d.nodeWatcher.Unwatch(p, d.ID())
	d.babel.Disconnect(d.ID(), p)

	if d.myPendingParentInJoin != nil && peer.PeersEqual(p, d.myPendingParentInJoin.peer) {
		d.logger.Warnf("Falling back from Pending Parent In join procedure")
		d.fallbackToPreviousLevel(d.myPendingParentInJoin)
		d.myPendingParentInJoin = nil
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
		return
	}

	if peer.PeersEqual(p, d.myPendingParentInClimb) {
		d.logger.Warnf("Falling back from Pending Parent In climb")
		d.myPendingParentInClimb = nil
		d.myGrandParent = nil
		if d.myParent != nil {
			d.logger.Warnf("falling back to parent")
			d.myPendingParentInRecovery = d.myParent
			d.sendJoinAsChildMsg(d.myParent, 0, true, false)
		} else {
			d.fallbackToMeasuredPeers()
		}
	}

	if peer.PeersEqual(p, d.myParent) {
		d.logger.Warnf("Parent down %s", p.String())
		aux := d.myParent
		d.myParent = nil
		d.babel.SendNotification(NewNodeDownNotification(aux, d.getInView(), crash))
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
		d.removeChild(child, crash)
		return
	}

	if sibling, isSibling := d.mySiblings[p.String()]; isSibling {
		d.logger.Warnf("Sibling down %s", p.String())
		d.removeSibling(sibling, crash)
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
	// d.logger.Infof("Message of type %s delivered to: %s", reflect.TypeOf(msg), p.String())
}

func (d *DemmonTree) MessageDeliveryErr(msg message.Message, p peer.Peer, err errors.Error) {
	d.logger.Warnf("Message of type %s (%+v) failed to deliver to: %s because: %s", reflect.TypeOf(msg), msg, p.String(), err.Reason())
	switch msg := msg.(type) {
	case JoinMessage:
		if !d.joined {
			d.handlePeerDownInJoin(p)
		}
	case JoinAsChildMessage:
		d.handlePeerDown(p, true)
	case RandomWalkMessage:
		d.sendMessageTmpUDPChan(msg, p)
	case WalkReplyMessage:
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
	d.myPendingParentInClimb = nil
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
		d.joinMap[landmark.String()] = &PeerWithParentAndChildren{parent: nil, peer: NewMeasuredPeer(landmark, math.MaxInt64), children: nil, replied: false}
		joinMsg := JoinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark)
	}
}

func (d *DemmonTree) sendJoinAsChildMsg(
	newParent *PeerWithIDChain,
	newParentLat time.Duration,
	urgent, improvement bool) {

	d.logger.Infof("Joining level %d", uint16(len(newParent.Chain())))
	toSend := NewJoinAsChildMessage(d.config.BandwidthScore, d.self, newParentLat, newParent.Chain(), urgent, improvement)
	d.logger.Infof("Sending join as child message %+v to peer %s", toSend, newParent.String())
	d.sendMessageTmpTCPChan(toSend, newParent)
}

func (d *DemmonTree) attemptProgress() {
	if d.myPendingParentInJoin != nil {
		return
	}
	canProgress, nextLevelPeers := d.getPeersInNextLevelByLat(d.bestPeerlastLevel)
	if !canProgress {
		return
	}

	if len(nextLevelPeers) == 0 {
		d.fallbackToPreviousLevel(d.bestPeerlastLevel)
		return
	}

	lowestLatencyPeer := nextLevelPeers[0]
	d.lastLevelProgress = time.Now()
	d.logger.Infof(
		"Lowest Latency Peer: %s , Latency: %d",
		lowestLatencyPeer.peer.String(),
		lowestLatencyPeer.peer.Latency,
	)

	if lowestLatencyPeer.peer.NrChildren() < d.config.MinGrpSize {
		d.logger.Infof("Joining under peer %s because nodes in this level do not have enough members", lowestLatencyPeer.peer.StringWithFields())
		d.myPendingParentInJoin = lowestLatencyPeer
		d.sendJoinAsChildMsg(lowestLatencyPeer.peer.PeerWithIDChain, lowestLatencyPeer.peer.Latency, false, false)
		return
	}

	if d.bestPeerlastLevel == nil {
		progress := d.progressToNextLevel(lowestLatencyPeer)
		if progress {
			return
		}
		d.myPendingParentInJoin = lowestLatencyPeer
		d.sendJoinAsChildMsg(lowestLatencyPeer.peer.PeerWithIDChain, lowestLatencyPeer.peer.Latency, false, false)
		return
	}

	if d.bestPeerlastLevel.peer.Latency < lowestLatencyPeer.peer.Latency {
		d.logger.Infof("Joining under peer %s because latency to parent is lower than to its children", d.bestPeerlastLevel.peer.StringWithFields())
		d.myPendingParentInJoin = d.bestPeerlastLevel
		d.sendJoinAsChildMsg(d.bestPeerlastLevel.peer.PeerWithIDChain, d.bestPeerlastLevel.peer.Latency, false, false)
		return
	}
	progress := d.progressToNextLevel(lowestLatencyPeer)
	if progress {
		return
	}
	d.logger.Infof("Joining under peer %s because nodes in next level do not have enough children", d.bestPeerlastLevel.peer.StringWithFields())
	d.myPendingParentInJoin = d.bestPeerlastLevel
	d.sendJoinAsChildMsg(d.bestPeerlastLevel.peer.PeerWithIDChain, d.bestPeerlastLevel.peer.Latency, false, false)

	// for _, v := range nextLevelPeers {
	// 	d.unwatchPeers(v.peer)
	// }
}

func (d *DemmonTree) progressToNextLevel(lowestLatencyPeer *PeerWithParentAndChildren) bool {
	d.logger.Infof("Advancing to next level under %s ", lowestLatencyPeer.peer.StringWithFields())
	d.bestPeerlastLevel = lowestLatencyPeer
	canProgress := false

	for _, p := range lowestLatencyPeer.children {
		if p.NrChildren() == 0 {
			continue
		}
		d.joinTimeoutTimerIds[p.String()] = d.babel.RegisterTimer(
			d.ID(),
			NewJoinMessageResponseTimeout(d.config.JoinMessageTimeout, p),
		)
		d.joinMap[p.String()] = &PeerWithParentAndChildren{
			peer:    NewMeasuredPeer(p, math.MaxInt64),
			replied: false,
		}
		d.logger.Infof("Sending JoinMessage to %s ", p.StringWithFields())
		d.sendMessageAndMeasureLatency(NewJoinMessage(), p)
		canProgress = true
	}
	return canProgress
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
	// d.logger.Infof("Sending message type %s to: %s", reflect.TypeOf(toSend), destPeer.String())
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
			peerWithChildren, ok := d.joinMap[l.String()]
			if !ok {
				d.logger.Infof("Cannot progress because landmark %s has gone down", l.String())
				return false, nil
			}

			if !peerWithChildren.replied {
				d.logger.Infof("Cannot progress because peer %s has not replied", peerWithChildren.peer.String())
				return false, nil
			}

			if peerWithChildren.peer.Latency == math.MaxInt64 {
				d.logger.Infof("Cannot progress because peer %s has not been measured", peerWithChildren.peer.String())
				return false, nil
			}
			coordsCopy := d.self.Coordinates
			coordsCopy[idx] = uint64(peerWithChildren.peer.Latency.Milliseconds())
			d.self = NewPeerWithIDChain(d.self.chain, d.self, d.self.nChildren, d.self.version+1, coordsCopy, d.config.BandwidthScore, d.getAvgChildrenBW())
			d.logger.Infof("My Coordinates: %+v", d.self.Coordinates)
			measuredPeersInLvl = append(measuredPeersInLvl, peerWithChildren)
		}
		sort.SliceStable(measuredPeersInLvl, func(i, j int) bool {
			return measuredPeersInLvl[i].peer.Latency < measuredPeersInLvl[j].peer.Latency
		})
		d.logger.Infof("My Coordinates: %+v", d.self.Coordinates)
		return true, measuredPeersInLvl
	}

	d.logger.Infof("Getting peers in next level with lastLevelPeer: %s", getStringOrNil(lastLevelPeer.peer.PeerWithIDChain))
	for _, aux := range lastLevelPeer.children {
		childrenStr := aux.String()
		c, ok := d.joinMap[childrenStr]
		if !ok {
			d.logger.Infof("Cannot progress because peer %s has went down", childrenStr)
			continue
		}
		if !c.replied {
			d.logger.Infof("Cannot progress because peer %s has not replied yet", c.peer.String())
			return false, nil
		}
		if c.peer.Latency == math.MaxInt64 {
			d.logger.Infof("Cannot progress because peer %s has not been measured", c.peer.String())
			return false, nil
		}
		measuredPeersInLvl = append(measuredPeersInLvl, c)
	}
	sort.SliceStable(measuredPeersInLvl, func(i, j int) bool {
		return measuredPeersInLvl[i].peer.Latency < measuredPeersInLvl[j].peer.Latency
	})

	return true, measuredPeersInLvl
}

func (d *DemmonTree) fallbackToPreviousLevel(node *PeerWithParentAndChildren) {
	if node.parent == nil {
		if !d.rejoinTimerActive {
			d.rejoinTimerActive = true
			d.babel.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
		}
		return
	}

	// info, err := d.nodeWatcher.GetNodeInfo(node.parent)
	// var peerLat time.Duration
	// if err != nil {
	// 	d.logger.Errorf("Peer %s has no latency measurement", node.peer.String())
	// 	peerLat = 0
	// } else {
	// 	peerLat = info.LatencyCalc().CurrValue()
	// }

	d.myPendingParentInJoin = d.joinMap[node.parent.String()]
	d.sendJoinAsChildMsg(node.parent, d.myPendingParentInJoin.peer.Latency, true, false)
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
	d.logger.Infof(" falling back to %s : %s", bestPeer.String(), bestPeer.Latency)
	d.myPendingParentInRecovery = bestPeer.PeerWithIDChain
	d.sendJoinAsChildMsg(bestPeer.PeerWithIDChain, bestPeer.Latency, true, false)
}
