package protocol

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon/internal/utils"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/nodeWatcher"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const ProtoID = 2000
const ProtoName = "DemonTree"

type DemmonTreeConfig = struct {
	Landmarks                              []*PeerWithIDChain
	LandmarkRedialTimer                    time.Duration
	JoinMessageTimeout                     time.Duration
	MaxTimeToProgressToNextLevel           time.Duration
	MaxRetriesJoinMsg                      int
	ParentRefreshTickDuration              time.Duration
	ChildrenRefreshTickDuration            time.Duration
	RejoinTimerDuration                    time.Duration
	NrPeersToBecomeParentInAbsorb          int
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
	NrPeersToKickPerParent                 uint16
	LimitFirstLevelGroupSize               bool
}

type DemmonTree struct {
	config                       *DemmonTreeConfig
	lastLevelProgress            time.Time
	currLevelPeers               []map[string]peer.Peer
	currLevelPeersDone           []map[string]*MeasuredPeer
	nodeWatcher                  nodeWatcher.NodeWatcher
	babel                        protocolManager.ProtocolManager
	logger                       *logrus.Logger
	myPendingParentInJoin        *MeasuredPeer
	self                         *PeerWithIDChain
	myGrandParent                *PeerWithIDChain
	myParent                     *PeerWithIDChain
	myChildren                   map[string]*PeerWithIDChain
	myPendingChildren            map[string]*PeerWithIDChain
	myChildrenLatencies          map[string]MeasuredPeersByLat
	mySiblings                   map[string]*PeerWithIDChain
	myPendingSiblings            map[string]*PeerWithIDChain
	myPendingParentInRecovery    *PeerWithIDChain
	children                     map[string]map[string]*PeerWithIDChain
	parents                      map[string]*PeerWithIDChain
	joinTimeoutTimerIds          map[string]int
	absorbTimerID                int
	improveTimerID               int
	measuringPeers               map[string]bool
	measuredPeers                map[string]*MeasuredPeer
	eView                        map[string]*PeerWithIDChain
	myPendingParentInImprovement *MeasuredPeer
	myPendingParentInAbsorb      *PeerWithIDChain
	joinLevel                    uint16
	landmark                     bool
}

const (
	DebugTimerDuration = 5 * time.Second
)

func New(config *DemmonTreeConfig, babel protocolManager.ProtocolManager, nw nodeWatcher.NodeWatcher) protocol.Protocol {
	return &DemmonTree{
		nodeWatcher: nw,
		babel:       babel,
		logger:      logs.NewLogger(ProtoName),
		config:      config,

		// join state
		joinLevel:          0,
		parents:            make(map[string]*PeerWithIDChain),
		currLevelPeers:     make([]map[string]peer.Peer, 0),
		currLevelPeersDone: make([]map[string]*MeasuredPeer, 0),
		children:           make(map[string]map[string]*PeerWithIDChain),

		// node state
		myPendingParentInRecovery: nil,
		self:                      nil,
		myGrandParent:             nil,
		myParent:                  nil,
		mySiblings:                make(map[string]*PeerWithIDChain),
		myPendingSiblings:         make(map[string]*PeerWithIDChain),
		myChildren:                make(map[string]*PeerWithIDChain),
		myPendingChildren:         make(map[string]*PeerWithIDChain),
		myChildrenLatencies:       make(map[string]MeasuredPeersByLat),
		joinTimeoutTimerIds:       make(map[string]int),

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

		if d.config.LimitFirstLevelGroupSize {
			d.absorbTimerID = d.babel.RegisterTimer(
				d.ID(),
				NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration),
			)
		}

		d.babel.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
		d.babel.RegisterTimer(d.ID(), NewDebugTimer(DebugTimerDuration))
		return
	}
	d.babel.RegisterTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
	d.babel.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
	d.babel.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
	d.babel.RegisterTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration))
	d.babel.RegisterTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout))
	d.improveTimerID = d.babel.RegisterTimer(
		d.ID(),
		NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration),
	)
	d.absorbTimerID = d.babel.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
	// d.switchTimerId = d.babel.RegisterTimer(d.ID(), NewSwitchTimer(d.config.CheckSwitchOportunityTimeout))
	d.babel.RegisterTimer(d.ID(), NewDebugTimer(DebugTimerDuration))
	d.joinOverlay()
}

func (d *DemmonTree) Init() {
	d.babel.RegisterRequestHandler(d.ID(), GetNeighboursReqID, d.handleGetInView)
	d.babel.RegisterRequestHandler(d.ID(), BroadcastMessageReqID, d.handleBroadcastMessageReq)

	d.babel.RegisterMessageHandler(d.ID(), JoinMessage{}, d.handleJoinMessage)
	d.babel.RegisterMessageHandler(d.ID(), JoinReplyMessage{}, d.handleJoinReplyMessage)
	d.babel.RegisterMessageHandler(d.ID(), JoinAsChildMessage{}, d.handleJoinAsChildMessage)
	d.babel.RegisterMessageHandler(d.ID(), JoinAsChildMessageReply{}, d.handleJoinAsChildMessageReply)

	d.babel.RegisterMessageHandler(d.ID(), UpdateParentMessage{}, d.handleUpdateParentMessage)
	d.babel.RegisterMessageHandler(d.ID(), UpdateChildMessage{}, d.handleUpdateChildMessage)
	d.babel.RegisterMessageHandler(d.ID(), AbsorbMessage{}, d.handleAbsorbMessage)
	d.babel.RegisterMessageHandler(d.ID(), DisconnectAsChildMessage{}, d.handleDisconnectAsChildMsg)

	d.babel.RegisterMessageHandler(d.ID(), BroadcastMessage{}, d.handleBroadcastMessage)

	// d.babel.RegisterMessageHandler(d.ID(), switchMessage{}, d.handleSwitchMessage)
	// d.babel.RegisterMessageHandler(d.ID(), joinAsParentMessage{}, d.handleJoinAsParentMessage)
	// d.babel.RegisterMessageHandler(d.ID(), biasedWalkMessage{}, d.handleBiasedWalkMessage)

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
	// d.babel.RegisterTimerHandler(d.ID(), switchTimerID, d.handleSwitchTimer)
	d.babel.RegisterTimerHandler(d.ID(), debugTimerID, d.handleDebugTimer)
}

// request handlers

func (d *DemmonTree) handleGetInView(req request.Request) request.Reply {
	getInViewReq := req.(GetNeighboursReq)
	view := InView{
		Grandparent: d.myGrandParent,
		Parent:      d.myParent,
		Children:    d.peerMapToArr(d.myChildren),
		Siblings:    d.peerMapToArr(d.mySiblings),
	}
	return NewGetNeighboursReqReply(getInViewReq.Key, view)
}

func (d *DemmonTree) handleBroadcastMessageReq(req request.Request) request.Reply {
	bcastReq := req.(BroadcastMessageRequest)
	wrapperMsg := NewBroadcastMessage(body_types.Message{
		ID:      bcastReq.Message.ID,
		TTL:     bcastReq.Message.TTL - 1,
		Content: bcastReq.Message.Content,
	})
	d.broadcastMessage(wrapperMsg, true, true, true)
	return nil
}

// notification handlers

func (d *DemmonTree) handlePeerMeasuredNotification(n notification.Notification) {
	peerMeasuredNotification := n.(PeerMeasuredNotification)
	delete(d.measuringPeers, peerMeasuredNotification.peerMeasured.String())

	if d.isNeighbour(peerMeasuredNotification.peerMeasured.Peer) {
		d.logger.Warnf("New peer measured: %s is a neighbor", peerMeasuredNotification.peerMeasured)
		return
	}

	currNodeStats, err := d.nodeWatcher.GetNodeInfo(peerMeasuredNotification.peerMeasured)
	if err != nil {
		d.logger.Panic(err.Reason())
		return
	}
	d.logger.Infof(
		"New peer measured: %s, latency: %s",
		peerMeasuredNotification.peerMeasured,
		currNodeStats.LatencyCalc().CurrValue(),
	)
	d.addToMeasuredPeers(
		NewMeasuredPeer(
			peerMeasuredNotification.peerMeasured,
			currNodeStats.LatencyCalc().CurrValue(),
		),
	)
	d.nodeWatcher.Unwatch(peerMeasuredNotification.peerMeasured.Peer, d.ID())
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
		d.self = NewPeerWithIDChain(
			d.self.Chain(),
			d.self.Peer,
			d.self.nChildren,
			d.self.Version()+1,
			d.self.Coordinates,
		)
		d.logger.Infof("My Coordinates: %+v", d.self.Coordinates)
		return
	}
	d.logger.Panic("handleLandmarkMeasuredNotification for non-landmark peer")
}

// timer handlers

func (d *DemmonTree) handleDebugTimer(joinTimer timer.Timer) {
	d.babel.RegisterTimer(d.ID(), NewDebugTimer(DebugTimerDuration))
	inView := make([]*PeerWithIDChain, 0)

	for _, child := range d.myChildren {
		inView = append(inView, child)
	}

	for _, sibling := range d.mySiblings {
		inView = append(inView, sibling)
	}

	if !d.landmark && d.myParent != nil {
		inView = append(inView, d.myParent)
	}

	toPrint := ""
	for _, neighbor := range inView {
		toPrint = toPrint + " " + neighbor.String()
	}
	d.logger.Infof(" %s InView: %s", d.self.String(), toPrint)
}

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {
	// if d.joinLevel == 0 && len(d.currLevelPeers[d.joinLevel]) == 0 {
	// 	d.logger.Info("-------------Rejoining overlay---------------")
	// 	d.joinOverlay()
	// }
	d.babel.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
}

func (d *DemmonTree) handleLandmarkRedialTimer(t timer.Timer) {
	redialTimer := t.(*landmarkRedialTimer)
	d.nodeWatcher.Watch(redialTimer.LandmarkToRedial, d.ID())
	c := nodeWatcher.Condition{
		Repeatable:                false,
		CondFunc:                  func(nodeWatcher.NodeInfo) bool { return true },
		EvalConditionTickDuration: 1000 * time.Millisecond,
		Notification:              LandmarkMeasuredNotification{landmarkMeasured: redialTimer.LandmarkToRedial},
		Peer:                      redialTimer.LandmarkToRedial,
		EnableGracePeriod:         false,
		ProtoId:                   d.ID(),
	}
	d.nodeWatcher.NotifyOnCondition(c)
	d.babel.Dial(d.ID(), redialTimer.LandmarkToRedial, redialTimer.LandmarkToRedial.ToTCPAddr())
}

func (d *DemmonTree) handleRefreshParentTimer(t timer.Timer) {
	// d.logger.Info("RefreshParentTimer trigger")
	for _, child := range d.myChildren {
		toSend := NewUpdateParentMessage(
			d.myParent,
			d.self,
			child.Chain()[child.Chain().Level()],
			d.getChildrenAsPeerWithIDChainArray(child),
		)
		d.sendMessage(toSend, child.Peer)
	}
	d.babel.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
}

func (d *DemmonTree) handleUpdateChildTimer(t timer.Timer) {
	// d.logger.Info("UpdateChildTimer trigger")
	if d.myParent != nil {
		d.sendUpdateChildMessage(d.myParent)
	}
	d.babel.RegisterTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
}

func (d *DemmonTree) sendUpdateChildMessage(dest peer.Peer) {
	if len(d.self.Chain()) > 0 {
		measuredSiblings := make(MeasuredPeersByLat, 0, len(d.mySiblings))

		for _, sibling := range d.mySiblings {
			nodeStats, err := d.nodeWatcher.GetNodeInfo(sibling.Peer)

			var currLat time.Duration

			if err != nil {
				d.logger.Warnf("Do not have latency measurement for %s", sibling.String())
				currLat = math.MaxInt64
			} else {
				currLat = nodeStats.LatencyCalc().CurrValue()
			}

			measuredSiblings = append(measuredSiblings, NewMeasuredPeer(sibling, currLat))
		}
		toSend := NewUpdateChildMessage(d.self, measuredSiblings)
		d.sendMessage(toSend, dest)
	}
}

func (d *DemmonTree) handleExternalNeighboringTimer(joinTimer timer.Timer) {
	d.babel.RegisterTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout))

	d.logger.Info("ExternalNeighboringTimer trigger")

	if d.myParent == nil ||
		len(d.self.Chain()) == 0 ||
		d.myPendingParentInImprovement != nil ||
		d.myPendingParentInAbsorb != nil ||
		d.myPendingParentInJoin != nil {
		d.logger.Infof("Not progressing because already have pending parent")
		return
	}

	r := utils.GetRandFloat64()
	if r > d.config.EmitWalkProbability {
		return
	}

	neighbors := d.getNeighborsAsPeerWithIDChainArray()
	// d.logger.Infof("d.possibilitiesToSend: %+v:", neighbours)

	sample := getRandSample(d.config.NrPeersInWalkMessage-1, append(neighbors, d.peerMapToArr(d.eView)...)...)
	sample[d.self.String()] = d.self

	// r = rand.Float32()
	var msgToSend message.Message

	var peerToSendTo *PeerWithIDChain
	// if r < d.config.BiasedWalkProbability {
	// 	msgToSend = NewBiasedWalkMessage(uint16(d.config.RandomWalkTTL), selfPeerWithChain, sample)
	// 	peerToSendTo = getBiasedPeerExcluding(possibilitiesToSend, selfPeerWithChain)
	// } else {
	msgToSend = NewRandomWalkMessage(uint16(d.config.RandomWalkTTL), d.self, d.peerMapToArr(sample))
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

	if d.improveTimerID == -1 {
		d.improveTimerID = d.babel.RegisterTimer(
			d.ID(),
			NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration),
		)
		return
	}

	d.improveTimerID = d.babel.RegisterTimer(
		d.ID(),
		NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration),
	)

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
		d.logger.Panic("Do not have latency measurement for parent")
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
		d.sendJoinAsChildMsg(measuredPeer.PeerWithIDChain, measuredPeer.MeasuredLatency, false)

		// if measuredPeer.Chain().Level() < d.self.Chain().Level()-1 {
		// 	// if measuredPeer.NrChildren() < d.config.MaxGrpSize { // must go UP in levels
		// 	aux := *measuredPeer
		// 	d.myPendingParentInImprovement = &aux
		// 	d.sendJoinAsChildMsg(measuredPeer.PeerWithIDChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
		// 	d.logger.Infof("Improving position towards: %s", measuredPeer.String())
		// 	d.logger.Infof("latencyImprovement: %s", latencyImprovement)
		// 	d.logger.Infof("parentLatency: %s", parentLatency)
		// 	d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
		// 	// return
		// 	// }
		// }
		// d.logger.Infof("Not improving position towards best peer: %s", measuredPeer.String())
		// d.logger.Infof("latencyImprovement: %s", latencyImprovement)
		// d.logger.Infof("parentLatency: %s", parentLatency)
		// d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)

		return
	}
}

func (d *DemmonTree) handleMeasureNewPeersTimer(measureNewPeersTimer timer.Timer) {
	d.babel.RegisterTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration))

	if len(d.eView) == 0 {
		d.logger.Infof("returning because len(eView) == 0")
		return
	}

	peersSorted := d.peerMapToArr(d.eView)
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
		if d.measurePeer(p) {
			nrMeasuredBiased++
		}
	}

	peersRandom := d.peerMapToArr(d.eView)
	rand.Shuffle(len(peersRandom), func(i, j int) { peersRandom[i], peersRandom[j] = peersRandom[j], peersRandom[i] })

	nrMeasuredRand := 0
	for i := 0; i < d.config.NrPeersToMeasureRandom && nrMeasuredRand < d.config.NrPeersToMeasureRandom; i++ {
		p := peersRandom[i]
		if d.measurePeer(p) {
			nrMeasuredRand++
		}
	}
}

func (d *DemmonTree) measurePeer(p *PeerWithIDChain) bool {
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
		Notification:              PeerMeasuredNotification{peerMeasured: p},
		Peer:                      p,
		EnableGracePeriod:         false,
		ProtoId:                   d.ID(),
	}
	// d.logger.Infof("Doing NotifyOnCondition for node %s...", p.String())
	d.nodeWatcher.NotifyOnCondition(c)
	d.measuringPeers[p.String()] = true
	return true
}

// VERSION where nodes with highest latency node gets kicked towards its lowest latency peer.
func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {

	d.absorbTimerID = d.babel.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))

	// d.logger.Info("handleCheckChildrenSize timer trigger")

	if d.self.NrChildren() == 0 ||
		d.myPendingParentInImprovement != nil ||
		d.myPendingParentInAbsorb != nil ||
		d.myPendingParentInJoin != nil {
		// d.logger.Info("d.self.NrChildren() == 0, returning...")
		return
	}

	if uint16(len(d.myChildren)) <= d.config.MaxGrpSize {
		// d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
		return
	}

	childrenAsMeasuredPeers := make(MeasuredPeersByLat, 0, d.self.NrChildren())

	for _, children := range d.myChildren {
		node, err := d.getPeerWithChainAsMeasuredPeer(children)
		if err != nil {
			d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", children.String())
			return
		}

		childrenAsMeasuredPeers = append(childrenAsMeasuredPeers, node)
	}

	sort.Sort(childrenAsMeasuredPeers)
	peerAbsorbers := childrenAsMeasuredPeers[:d.config.NrPeersToBecomeParentInAbsorb]
	peersAbsorbedPerAbsorber := make(map[string]int)
	peersAlreadyKicked := make(map[string]bool)

	for _, p := range peerAbsorbers {
		peersAlreadyKicked[p.String()] = true
	}

	for i := uint16(0); i < d.config.NrPeersToKickPerParent; i++ {
		for j := d.config.NrPeersToBecomeParentInAbsorb; j >= 0; j-- {

			if len(peerAbsorbers) < 1 {
				return
			}

			var (
				lowestLatPairLat      = time.Duration(math.MaxInt64)
				bestCandidateToKick   *PeerWithIDChain
				bestCandidateToAbsorb = peerAbsorbers[0].PeerWithIDChain
			)

			for _, peerAbsorber := range peerAbsorbers {
				nrPeersAbsorbed, ok := peersAbsorbedPerAbsorber[peerAbsorber.String()]
				if !ok {
					peersAbsorbedPerAbsorber[peerAbsorber.String()] = 0
					nrPeersAbsorbed = 0
				}

				if uint16(nrPeersAbsorbed) == d.config.NrPeersToKickPerParent {
					continue
				}

				peerAbsorberSiblingLatencies := d.myChildrenLatencies[peerAbsorber.String()]
				sort.Sort(peerAbsorberSiblingLatencies)
				for _, candidateToKick := range peerAbsorberSiblingLatencies {
					if candidateToKick == nil {
						continue
					}

					if _, isChild := d.myChildren[candidateToKick.String()]; !isChild {
						d.logger.Errorf("Candidate %s to be absorbed is not a child", candidateToKick.String())
						continue
					}

					if peer.PeersEqual(candidateToKick, peerAbsorber) {
						continue
					}

					if candidateToKick.MeasuredLatency == 0 {
						continue
					}

					if alreadyKicked := peersAlreadyKicked[candidateToKick.String()]; alreadyKicked {
						continue
					}

					if candidateToKick.MeasuredLatency < lowestLatPairLat {
						bestCandidateToKick = candidateToKick.PeerWithIDChain
						lowestLatPairLat = candidateToKick.MeasuredLatency
						bestCandidateToAbsorb = peerAbsorber.PeerWithIDChain
					}
				}
			}

			if bestCandidateToKick == nil {
				return
			}

			peersAlreadyKicked[bestCandidateToKick.String()] = true
			peersAbsorbedPerAbsorber[bestCandidateToAbsorb.String()]++
			d.logger.Infof(
				"Sending absorb message with peerToAbsorb: %s, peerAbsorber: %s",
				bestCandidateToKick.StringWithFields(),
				bestCandidateToAbsorb.StringWithFields(),
			)
			toSend := NewAbsorbMessage(bestCandidateToKick, bestCandidateToAbsorb)
			d.sendMessage(toSend, bestCandidateToKick)
			d.resetImproveTimer()
		}
	}
}

// message handlers

func (d *DemmonTree) handleRandomWalkMessage(sender peer.Peer, m message.Message) {
	randWalkMsg := m.(RandomWalkMessage)
	d.logger.Infof("Got randomWalkMessage: %+v from %s", randWalkMsg, sender.String())
	// toPrint := ""
	// for _, peer := range randWalkMsg.Sample {
	// 	toPrint = toPrint + "; " + peer.String()
	// }
	// d.logger.Infof("randomWalkMessage peers: %s", toPrint)

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
	d.logger.Infof("Got walkReplyMessage: %+v from %s", walkReply, sender.String())
	sample := walkReply.Sample
	d.updateAndMergeSampleEntriesWithEView(sample, d.config.NrPeersToMergeInWalkSample)
}

func (d *DemmonTree) handleAbsorbMessage(sender peer.Peer, m message.Message) {
	absorbMessage := m.(AbsorbMessage)

	d.logger.Infof("Got absorbMessage: %+v from %s", m, sender.String())

	if !peer.PeersEqual(d.myParent, sender) {
		d.logger.Warnf(
			"Got absorbMessage: %+v from not my parent %s (my parent: %s)",
			m,
			sender.String(),
			d.myParent.String(),
		)
		return
	}

	if len(d.self.Chain()) == 0 ||
		d.myPendingParentInImprovement != nil ||
		d.myPendingParentInAbsorb != nil ||
		d.myPendingParentInJoin != nil {
		d.logger.Errorf("Got absorbMessage but already have pending parent")
		return
	}

	if sibling, ok := d.mySiblings[absorbMessage.PeerAbsorber.String()]; ok {
		d.removeSibling(sibling, false)
		var peerLat time.Duration
		if nodeInfo, err := d.nodeWatcher.GetNodeInfo(sibling); err == nil {
			peerLat = nodeInfo.LatencyCalc().CurrValue()
		}
		d.myPendingParentInAbsorb = absorbMessage.PeerAbsorber
		toSend := NewJoinAsChildMessage(d.self, peerLat, absorbMessage.PeerAbsorber.Chain(), false)
		d.sendMessage(toSend, absorbMessage.PeerAbsorber)
	} else {
		d.myPendingParentInAbsorb = absorbMessage.PeerAbsorber
		toSend := NewJoinAsChildMessage(d.self, 0, absorbMessage.PeerAbsorber.Chain(), false)
		d.sendMessageTmpTCPChan(toSend, absorbMessage.PeerAbsorber)
	}
}

func (d *DemmonTree) handleDisconnectAsChildMsg(sender peer.Peer, m message.Message) {
	dacMsg := m.(DisconnectAsChildMessage)
	d.logger.Infof("got DisconnectAsChildMsg %+v from %s", dacMsg, sender.String())
	d.removeChild(sender, true, true)
}

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {
	toSend := NewJoinReplyMessage(d.getChildrenAsPeerWithIDChainArray(), d.self)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinMessageResponseTimeout(t timer.Timer) {
	p := t.(*peerJoinMessageResponseTimeout).Peer
	d.logger.Warnf("timeout from join message from %s", p.String())

	if d.joinLevel == math.MaxUint16 {
		return
	}

	delete(d.currLevelPeers[d.joinLevel], p.String())
	delete(d.currLevelPeersDone[d.joinLevel], p.String())
	d.nodeWatcher.Unwatch(p, d.ID())
	if len(d.currLevelPeers[d.joinLevel]) == 0 {
		d.logger.Warn("Have no more peers in current level, falling back to parent")
		d.fallbackToParentInJoin(p)
		return
	}
	if d.canProgressToNextStep() {
		d.progressToNextStep()
	}
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(JoinReplyMessage)

	d.logger.Infof("Got joinReply: %+v from %s", replyMsg, sender.String())

	if d.joinLevel == math.MaxUint16 {
		d.logger.Errorf("Got joinReply: %+v but already joined... %s", replyMsg, sender.String())
		return
	}

	if _, ok := d.currLevelPeers[d.joinLevel][sender.String()]; ok {
		d.babel.CancelTimer(d.joinTimeoutTimerIds[sender.String()])
	} else {
		d.logger.Errorf("Got joinReply: %+v from timed out peer... %s", replyMsg, sender.String())
		return
	}

	if (d.joinLevel != replyMsg.Sender.Chain().Level()) || (d.joinLevel > 1 && !d.parents[sender.String()].IsParentOf(replyMsg.Sender)) {
		if d.joinLevel != replyMsg.Sender.Chain().Level() {
			d.logger.Warnf(
				"Discarding joinReply %+v from %s because joinLevel is not mine: %d",
				replyMsg,
				sender.String(),
				d.joinLevel,
			)
		}

		if d.joinLevel > 1 && !d.parents[sender.String()].IsParentOf(replyMsg.Sender) {
			d.logger.Warnf(
				"Discarding joinReply %+v from %s because node does not have the same parent... should be: %s",
				replyMsg,
				sender.String(),
				d.parents[sender.String()].String(),
			)
		}

		// discard old repeated messages
		delete(d.currLevelPeers[d.joinLevel], sender.String())
		delete(d.currLevelPeersDone[d.joinLevel], sender.String())

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

	d.currLevelPeersDone[d.joinLevel][sender.String()] = NewMeasuredPeer(replyMsg.Sender, math.MaxInt64)
	d.children[sender.String()] = make(map[string]*PeerWithIDChain, len(replyMsg.Children))
	for _, c := range replyMsg.Children {
		d.children[sender.String()][c.String()] = c
	}

	for _, children := range replyMsg.Children {
		d.parents[children.String()] = replyMsg.Sender
	}

	if d.canProgressToNextStep() {
		d.progressToNextStep()
	}
}

// func (d *DemmonTree) handleJoinAsParentMessage(sender peer.Peer, m message.Message) {
// 	japMsg := m.(joinAsParentMessage)
// 	d.logger.Infof("got JoinAsParentMessage %+v from %s", japMsg, sender.String())
// 	if !ChainsEqual(d.myIDChain, japMsg.ExpectedId) {
// 		d.logger.Warnf("Discarding parent %s because it was trying to optimize with previous position", sender.String())
// 		return
// 	}
// 	d.addParent(sender, japMsg.ProposedId, japMsg.Level, 0, false, false)
// 	d.mergeSiblingsWith(japMsg.Siblings)
// }

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {
	jacMsg := m.(JoinAsChildMessage)
	d.logger.Infof("got JoinAsChildMessage %+v from %s", jacMsg, sender.String())

	if len(d.self.Chain()) == 0 ||
		d.self.IsDescendentOf(jacMsg.Sender.Chain()) ||
		d.myPendingParentInImprovement != nil ||
		d.myPendingParentInRecovery != nil {

		toSend := NewJoinAsChildMessageReply(false, PeerID{}, d.self.Chain().Level(), d.self, nil, nil)

		if len(d.self.Chain()) == 0 {
			d.logger.Info("denying joinAsChildReply from because my chain is nil")
		}

		if d.myPendingParentInImprovement != nil {
			d.logger.Info("denying joinAsChildReply from because d.myPendingParentInImprovement != nil ")
		}

		if d.self.IsDescendentOf(jacMsg.Sender.Chain()) {
			d.logger.Infof(
				"denying joinAsChildReply from because im descendent of sender, sender chain: %+v",
				jacMsg.Sender.Chain(),
			)
		}

		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	if !jacMsg.Urgent {
		if !jacMsg.ExpectedID.Equal(d.self.Chain()) {
			d.logger.Info("denying joinAsChildReply because expected id does not match my id")
			toSend := NewJoinAsChildMessageReply(false, PeerID{}, d.self.Chain().Level(), d.self, nil, nil)
			d.sendMessageTmpTCPChan(toSend, sender)
			return
		}
	}

	_, isSibling := d.mySiblings[sender.String()]
	if isSibling {
		delete(d.mySiblings, sender.String())
	}
	_, isPendingSibling := d.myPendingSiblings[sender.String()]
	if isPendingSibling {
		delete(d.mySiblings, sender.String())
	}
	newChildID := d.addChild(jacMsg.Sender, !isSibling, jacMsg.MeasuredLatency)
	childrenToSend := make([]*PeerWithIDChain, 0, d.self.NrChildren())

	for _, child := range d.myChildren {
		childrenToSend = append(childrenToSend, child)
	}
	toSend := NewJoinAsChildMessageReply(
		true,
		newChildID,
		d.self.Chain().Level(),
		d.self,
		childrenToSend,
		d.myGrandParent,
	)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinAsChildMessageReply(sender peer.Peer, m message.Message) {
	japrMsg := m.(JoinAsChildMessageReply)
	d.logger.Infof("got JoinAsChildMessageReply %+v from %s", japrMsg, sender.String())

	if japrMsg.Accepted {
		myNewID := append(japrMsg.Parent.Chain(), japrMsg.ProposedID)

		if d.myPendingParentInImprovement != nil && peer.PeersEqual(sender, d.myPendingParentInImprovement) {
			if measuredPeer, err := d.getPeerWithChainAsMeasuredPeer(d.myParent); err != nil {
				d.addToMeasuredPeers(measuredPeer)
			}
			d.addParent(
				japrMsg.Parent,
				japrMsg.GrandParent,
				myNewID,
				d.myPendingParentInImprovement.MeasuredLatency,
				true,
				true,
			)
			d.myPendingParentInImprovement = nil
			return
		}

		if d.myPendingParentInAbsorb != nil && peer.PeersEqual(sender, d.myPendingParentInAbsorb) {
			if measuredPeer, err := d.getPeerWithChainAsMeasuredPeer(d.myParent); err != nil {
				d.addToMeasuredPeers(measuredPeer)
			}
			d.addParent(japrMsg.Parent, japrMsg.GrandParent, myNewID, 0, true, true)
			d.myPendingParentInAbsorb = nil
			return
		}

		if d.myPendingParentInRecovery != nil && peer.PeersEqual(sender, d.myPendingParentInRecovery) {
			d.addParent(japrMsg.Parent, japrMsg.GrandParent, myNewID, 0, false, false)
			d.myPendingParentInRecovery = nil
			return
		}

		if d.myPendingParentInJoin != nil && peer.PeersEqual(sender, d.myPendingParentInJoin) {
			d.addParent(
				japrMsg.Parent,
				japrMsg.GrandParent,
				myNewID,
				d.myPendingParentInJoin.MeasuredLatency,
				false,
				false,
			)
			d.myPendingParentInJoin = nil
			return
		}
		return
	}

	// not accepted
	// special case for parent in recovery
	if d.myPendingParentInRecovery != nil && peer.PeersEqual(sender, d.myPendingParentInRecovery) {
		d.logger.Warnf("Pending parent in recovery denied request")
		d.removeFromMeasuredPeers(d.myPendingParentInRecovery)
		d.fallbackToMeasuredPeers()
		return
	}

	if d.myPendingParentInImprovement != nil && peer.PeersEqual(sender, d.myPendingParentInImprovement) {
		d.logger.Warnf("Pending parent in improvement denied request")
		if mp, ok := d.measuredPeers[d.myPendingParentInImprovement.String()]; ok {
			d.measuredPeers[d.myPendingParentInImprovement.String()] = NewMeasuredPeer(
				japrMsg.Parent,
				mp.MeasuredLatency,
			)
		}
		d.myPendingParentInImprovement = nil
		return
	}

	if d.myPendingParentInAbsorb != nil && peer.PeersEqual(sender, d.myPendingParentInAbsorb) {
		if d.myGrandParent != nil {
			d.logger.Warnf("Pending parent in absorb denied request.. falling back to grandparent")
			d.myPendingParentInRecovery = d.myGrandParent
			d.sendJoinAsChildMsg(d.myGrandParent, 0, true)
			d.myGrandParent = nil
		} else {
			d.fallbackToMeasuredPeers()
		}
		d.myPendingParentInAbsorb = nil
		return
	}

	if d.myPendingParentInJoin != nil && peer.PeersEqual(sender, d.myPendingParentInJoin) {
		d.logger.Warnf("Pending parent in join denied request.. falling back to parent")
		d.myPendingParentInJoin = nil
		d.fallbackToParentInJoin(sender)
		return
	}
	d.logger.Panicf("got join as child reply but cause for it is not known")
}

func (d *DemmonTree) handleUpdateParentMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(UpdateParentMessage)
	d.logger.Infof("got UpdateParentMessage %+v from %s", upMsg, sender.String())
	if d.myParent == nil || !peer.PeersEqual(sender, d.myParent) {
		if d.myParent != nil {
			d.logger.Errorf(
				"Received UpdateParentMessage from not my parent (parent:%s sender:%s)",
				d.myParent.StringWithFields(),
				upMsg.Parent.StringWithFields(),
			)
			return
		}
		d.logger.Errorf(
			"Received UpdateParentMessage from not my parent (parent:%s sender:%s)",
			d.myParent.StringWithFields(),
			upMsg.Parent.StringWithFields(),
		)
		return
	}

	if upMsg.GrandParent != nil {
		if d.myGrandParent == nil {
			d.logger.Warnf("My grandparent changed : (<nil> -> %s)", upMsg.GrandParent.StringWithFields())
			d.myGrandParent = upMsg.GrandParent
		}
		if !peer.PeersEqual(d.myGrandParent, upMsg.GrandParent) {
			if upMsg.GrandParent == nil {
				d.logger.Warnf("My grandparent changed : (%s -> <nil>)", d.myGrandParent.StringWithFields())
				d.myGrandParent = upMsg.GrandParent
			} else {
				d.logger.Warnf(
					"My grandparent changed : (%s -> %s)",
					d.myGrandParent.StringWithFields(),
					upMsg.GrandParent.StringWithFields(),
				)
				d.myGrandParent = upMsg.GrandParent
			}
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

		for childStr, child := range d.myChildren {
			childID := child.Chain()[len(child.Chain())-1]
			d.myChildren[childStr] = NewPeerWithIDChain(
				append(myNewChain, childID),
				child.Peer,
				child.nChildren,
				child.Version()+1,
				child.Coordinates,
			)
		}
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
	d.myChildrenLatencies[child.String()] = upMsg.Siblings
}

func (d *DemmonTree) handleBroadcastMessage(sender peer.Peer, m message.Message) {
	bcastMsg := m.(BroadcastMessage)
	d.logger.Infof("got broadcastMessage %+v from %s", m, sender.String())
	d.babel.SendNotification(NewBroadcastMessageReceivedNotification(bcastMsg.Message))
	if bcastMsg.Message.TTL > 0 { // propagate
		bcastMsg.Message.TTL--
		sibling, child, parent := d.getPeerRelationshipType(sender)
		if parent || sibling {
			d.broadcastMessage(bcastMsg, false, true, false)
			return
		}

		if child {
			d.broadcastMessage(bcastMsg, true, false, true)
			return
		}
	}
}

func (d *DemmonTree) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	if dialerProto != d.ID() {
		d.logger.Infof("Not accepting dial from other proto: %d", dialerProto)
		return false
	}

	if d.myParent != nil && peer.PeersEqual(d.myParent, p) {
		d.logger.Infof("My parent dialed me")
		return true
	}

	child, isChildren := d.myChildren[p.String()]
	if isChildren {
		d.logger.Infof("My children (%s) dialed me ", child.StringWithFields())
		return true
	}

	sibling, isSibling := d.mySiblings[p.String()]
	if isSibling {
		d.logger.Infof("My sibling (%s) dialed me ", sibling.StringWithFields())
		return true
	}

	d.logger.Warnf("Conn requested by unknown peer: %s", p.String())
	return true
}

func (d *DemmonTree) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {
	if sourceProto != d.ID() {
		d.logger.Infof("Not accepting dial from other proto: %d", sourceProto)
		return false
	}

	if d.landmark {
		for _, l := range d.config.Landmarks {
			if peer.PeersEqual(l, p) {
				d.babel.SendNotification(NewNodeUpNotification(l, d.getInView()))
				return true
			}
		}
	}

	d.logger.Infof("Dialed peer with success: %s", p.String())
	if d.myParent != nil && peer.PeersEqual(d.myParent, p) {
		d.logger.Infof("Dialed parent with success, parent: %s", d.myParent.StringWithFields())
		c := nodeWatcher.Condition{
			Repeatable:                false,
			CondFunc:                  d.isNodeDown,
			EvalConditionTickDuration: 1000 * time.Millisecond,
			Notification:              SuspectNotification{peerDown: d.myParent},
			Peer:                      d.myParent,
			EnableGracePeriod:         false,
			ProtoId:                   d.ID(),
		}
		d.nodeWatcher.NotifyOnCondition(c)
		d.sendUpdateChildMessage(d.myParent)
		d.babel.SendNotification(NewNodeUpNotification(d.myParent, d.getInView()))
		return true
	}

	child, isChildren := d.myPendingChildren[p.String()]
	if isChildren {
		delete(d.myPendingChildren, child.String())
		d.myChildren[child.String()] = child
		d.logger.Infof("Dialed children with success: %s", child.StringWithFields())
		toSend := NewUpdateParentMessage(
			d.myParent,
			d.self,
			child.Chain()[len(child.Chain())-1],
			d.getChildrenAsPeerWithIDChainArray(child),
		)
		d.sendMessage(toSend, child)
		d.babel.SendNotification(NewNodeUpNotification(child, d.getInView()))
		c := nodeWatcher.Condition{
			Repeatable:                false,
			CondFunc:                  d.isNodeDown,
			EvalConditionTickDuration: 1000 * time.Millisecond,
			Notification:              SuspectNotification{peerDown: child},
			Peer:                      child,
			EnableGracePeriod:         false,
			ProtoId:                   d.ID(),
		}
		d.nodeWatcher.NotifyOnCondition(c)
		d.self = NewPeerWithIDChain(
			d.self.Chain(),
			d.self.Peer,
			d.self.nChildren+1,
			d.self.Version()+1,
			d.self.Coordinates,
		)
		for _, child := range d.myChildren {
			toSend := NewUpdateParentMessage(
				d.myParent,
				d.self,
				child.Chain()[len(child.Chain())-1],
				d.getChildrenAsPeerWithIDChainArray(child),
			)
			d.sendMessage(toSend, child.Peer)
		}
		return true
	}

	sibling, isSibling := d.myPendingSiblings[p.String()]
	if isSibling {
		delete(d.myPendingSiblings, p.String())
		d.mySiblings[p.String()] = sibling
		d.logger.Infof("Dialed sibling with success: %s", sibling.StringWithFields())
		d.babel.SendNotification(NewNodeUpNotification(sibling, d.getInView()))
		c := nodeWatcher.Condition{
			Repeatable:                false,
			CondFunc:                  d.isNodeDown,
			EvalConditionTickDuration: 1000 * time.Millisecond,
			Notification:              SuspectNotification{peerDown: sibling},
			Peer:                      sibling,
			EnableGracePeriod:         false,
			ProtoId:                   d.ID(),
		}
		d.nodeWatcher.NotifyOnCondition(c)
		return true
	}

	d.logger.Infof("d.myParent: %s", d.myParent)
	d.logger.Infof("d.myChildren: %+v", d.myChildren)
	d.logger.Panicf("Dialed unknown peer: %s", p.String())
	return false
}

func (d *DemmonTree) DialFailed(p peer.Peer) {
	d.logger.Errorf("Failed to dial %s", p.String())
	if d.landmark {
		for _, landmark := range d.config.Landmarks {
			if peer.PeersEqual(landmark, p) {
				d.babel.RegisterTimer(d.ID(), NewLandmarkRedialTimer(d.config.LandmarkRedialTimer, landmark))
				d.nodeWatcher.Unwatch(p, d.ID())
				return
			}
		}
	}

	if peer.PeersEqual(p, d.myParent) {
		d.logger.Warnf("failed to dial parent... %s", p.String())
		d.myParent = nil
		d.nodeWatcher.Unwatch(p, d.ID())
		if d.myGrandParent != nil {
			d.logger.Warnf("Falling back to grandparent %s", d.myGrandParent.String())
			d.myPendingParentInRecovery = d.myGrandParent
			d.sendJoinAsChildMsg(d.myGrandParent, 0, true)
			d.myGrandParent = nil
			return
		}
		d.logger.Warnf("Grandparent is nil... falling back to measured peers")
		d.fallbackToMeasuredPeers()
		return
	}

	if child, isChildren := d.myPendingChildren[p.String()]; isChildren {
		d.logger.Warnf("Child down %s", p.String())
		d.removeChild(child, true, true)
		return
	}

	if sibling, isSibling := d.myPendingSiblings[p.String()]; isSibling {
		d.logger.Warnf("Sibling down %s", p.String())
		d.removeSibling(sibling, true)
		return
	}
	d.logger.Panicf("Failed to dial unknown peer %s", p.String())
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
	if peer.PeersEqual(p, d.myParent) {
		d.logger.Warnf("Parent down %s", p.String())
		aux := d.myParent
		d.myParent = nil
		d.babel.SendNotification(NewNodeDownNotification(aux, d.getInView()))
		d.nodeWatcher.Unwatch(p, d.ID())
		if d.myGrandParent != nil {
			d.logger.Warnf("Falling back to grandparent %s", d.myGrandParent.String())
			d.myPendingParentInRecovery = d.myGrandParent
			d.sendJoinAsChildMsg(d.myGrandParent, 0, true)
			d.myGrandParent = nil
			return
		}
		d.logger.Warnf("Grandparent is nil... falling back to measured peers")
		d.fallbackToMeasuredPeers()
		return
	}

	if child, isChildren := d.myChildren[p.String()]; isChildren {
		d.logger.Warnf("Child down %s", p.String())
		d.removeChild(child, true, true)
		return
	}

	if sibling, isSibling := d.mySiblings[p.String()]; isSibling {
		d.logger.Warnf("Sibling down %s", p.String())
		d.removeSibling(sibling, true)
		return
	}

	d.logger.Warnf("Unknown peer down %s", p.String())
	d.nodeWatcher.Unwatch(p, d.ID())
}

func (d *DemmonTree) MessageDelivered(msg message.Message, p peer.Peer) {
	// d.logger.Infof("Message %+v delivered to: %s", message, peer.String())
}

func (d *DemmonTree) MessageDeliveryErr(msg message.Message, p peer.Peer, err errors.Error) {
	d.logger.Errorf("Message %+v failed to deliver to: %s because: %s", msg, p.String(), err.Reason())

	switch msg := msg.(type) {
	case JoinMessage:
		if d.joinLevel != math.MaxUint16 {
			// _, ok := d.retries[p.String()]
			// if !ok {
			// 	d.retries[p.String()] = 0
			// }
			// d.retries[p.String()]++
			// if d.retries[p.String()] >= d.config.MaxRetriesJoinMsg {
			d.logger.Warnf(
				"Deleting peer %s from currLevelPeers because it exceeded max retries (%d)",
				p.String(),
				d.config.MaxRetriesJoinMsg,
			)
			delete(d.currLevelPeers[d.joinLevel], p.String())
			// delete(d.retries, p.String())
			d.nodeWatcher.Unwatch(p, d.ID())
			if d.canProgressToNextStep() {
				d.progressToNextStep()
			}
			return
		}
	case JoinAsChildMessage:
		if peer.PeersEqual(p, d.myPendingParentInRecovery) { // message was a recovery from measured Peers
			d.removeFromMeasuredPeers(d.myPendingParentInRecovery)
			d.fallbackToMeasuredPeers()
			return
		}

		if d.joinLevel != math.MaxUint16 { // message was lost in join
			d.fallbackToParentInJoin(p)
			return
		}

	// case updateChildMessage:
	// 	if error.Reason() == "stream not found" && peer.PeersEqual(d.myParent, p) {
	// 		d.babel.Dial(d.myParent, d.ID(), stream.NewTCPDialer())
	// 	}

	// case updateParentMessage:
	// 	_, isChild := d.myChildren[p.String()]
	// 	if error.Reason() == "stream not found" && isChild {
	// 		d.babel.Dial(p, d.ID(), stream.NewTCPDialer())
	// 	}

	case RandomWalkMessage:
		d.sendMessageTmpUDPChan(msg, p)

	case DisconnectAsChildMessage:
		d.babel.Disconnect(d.ID(), p)
	}
}

func (d *DemmonTree) joinOverlay() {
	nrLandmarks := len(d.config.Landmarks)
	d.currLevelPeersDone = []map[string]*MeasuredPeer{make(map[string]*MeasuredPeer, nrLandmarks)} // start level 1
	d.currLevelPeers = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)}             // start level 1
	d.joinLevel = 0
	d.logger.Infof("Landmarks:")
	for i, landmark := range d.config.Landmarks {
		d.logger.Infof("%d :%s", i, landmark.String())
		d.lastLevelProgress = time.Now()
		d.currLevelPeers[d.joinLevel][landmark.String()] = landmark
		joinMsg := JoinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark)
	}
}

func (d *DemmonTree) sendJoinAsChildMsg(
	newParent *PeerWithIDChain,
	newParentLat time.Duration,
	urgent bool) {
	d.logger.Infof("Pending parent: %s", newParent.String())
	d.logger.Infof("Joining level %d", uint16(len(newParent.Chain())))
	toSend := NewJoinAsChildMessage(d.self, newParentLat, newParent.Chain(), urgent)
	d.sendMessageTmpTCPChan(toSend, newParent)
}

func (d *DemmonTree) unwatchPeersInLevelDone(level uint16, exclusions ...*PeerWithIDChain) {
	for _, currPeer := range d.currLevelPeersDone[level] {
		found := false

		for _, exclusion := range exclusions {

			if peer.PeersEqual(exclusion, currPeer) {
				found = true
				break
			}
		}
		if !found {
			d.addToMeasuredPeers(currPeer)
			d.nodeWatcher.Unwatch(currPeer, d.ID())
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
		_, ok := d.currLevelPeersDone[d.joinLevel][p.String()]
		if !ok {
			d.logger.Infof("Cannot progress because not all peers are done, missing: %s", p.String())
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
	currLevelPeersDone := d.getPeersInLevelByLat(
		d.joinLevel,
		d.lastLevelProgress.Add(d.config.MaxTimeToProgressToNextLevel),
	)
	if len(currLevelPeersDone) == 0 {
		if d.joinLevel == 0 {
			d.joinOverlay()
			return
		}
		d.joinLevel--
		currLevelPeersDone = d.getPeersInLevelByLat(
			d.joinLevel,
			d.lastLevelProgress.Add(d.config.MaxTimeToProgressToNextLevel),
		)
		lowestLatencyPeer := currLevelPeersDone[0]
		d.myPendingParentInJoin = lowestLatencyPeer
		d.sendJoinAsChildMsg(lowestLatencyPeer.PeerWithIDChain, lowestLatencyPeer.MeasuredLatency, false)
		return
	}

	if d.joinLevel == 0 { // fill in coordinates
		for idx, landmark := range d.config.Landmarks {
			found := false
			for _, p := range currLevelPeersDone {

				if peer.PeersEqual(p, landmark) {
					d.self.Coordinates[idx] = uint64(p.MeasuredLatency.Milliseconds())
					found = true
					break
				}
			}
			if !found {
				d.self.Coordinates[idx] = math.MaxUint64
			}
		}
		d.self = NewPeerWithIDChain(
			d.self.Chain(),
			d.self.Peer,
			d.self.nChildren,
			d.self.Version()+1,
			d.self.Coordinates,
		)
		d.logger.Infof("My Coordinates: %+v", d.self.Coordinates)
	}

	lowestLatencyPeer := currLevelPeersDone[0]
	d.lastLevelProgress = time.Now()

	d.logger.Infof(
		"Lowest Latency Peer: %s , Latency: %d",
		lowestLatencyPeer.String(),
		lowestLatencyPeer.MeasuredLatency,
	)
	toPrint := ""
	for _, peerDone := range currLevelPeersDone {
		toPrint = toPrint + "; " + peerDone.StringWithFields()
	}
	d.logger.Infof("d.currLevelPeersDone (level %d): %+v:", d.joinLevel, toPrint)

	if d.joinLevel == 0 {
		if lowestLatencyPeer.NrChildren() < d.config.MinGrpSize {
			d.logger.Infof("Joining level %d because nodes in this level have not enough members", d.joinLevel)
			d.myPendingParentInJoin = lowestLatencyPeer
			d.sendJoinAsChildMsg(lowestLatencyPeer.PeerWithIDChain, lowestLatencyPeer.MeasuredLatency, false)
			return
		}
		d.unwatchPeersInLevelDone(d.joinLevel, lowestLatencyPeer.PeerWithIDChain)
		d.progressToNextLevel(lowestLatencyPeer)
		return
	}

	lowestLatencyPeerParent := d.parents[lowestLatencyPeer.String()]
	info, err := d.nodeWatcher.GetNodeInfo(lowestLatencyPeerParent)
	if err != nil {
		d.logger.Panic(err.Reason())
	}

	parentLatency := info.LatencyCalc().CurrValue()
	if parentLatency < lowestLatencyPeer.MeasuredLatency {
		d.logger.Infof("Joining level %d because latency to parent is lower than to its children", d.joinLevel)
		d.unwatchPeersInLevelDone(d.joinLevel-1, lowestLatencyPeerParent)
		d.myPendingParentInJoin = NewMeasuredPeer(lowestLatencyPeerParent, parentLatency)
		d.sendJoinAsChildMsg(lowestLatencyPeerParent, info.LatencyCalc().CurrValue(), false)
		return
	}
	if lowestLatencyPeer.NrChildren() >= d.config.MinGrpSize {
		d.unwatchPeersInLevelDone(d.joinLevel - 1)
		d.unwatchPeersInLevelDone(d.joinLevel, lowestLatencyPeer.PeerWithIDChain)
		d.progressToNextLevel(lowestLatencyPeer)
		return
	}
	if lowestLatencyPeerParent.NrChildren() >= d.config.MaxGrpSize {
		d.logger.Infof("Joining level %d because ideal parent in level %d has too many children", d.joinLevel-1, d.joinLevel-1)
		d.unwatchPeersInLevelDone(d.joinLevel-1, lowestLatencyPeerParent)
		d.myPendingParentInJoin = lowestLatencyPeer
		d.sendJoinAsChildMsg(lowestLatencyPeer.PeerWithIDChain, lowestLatencyPeer.MeasuredLatency, false)
		return
	}
	d.myPendingParentInJoin = NewMeasuredPeer(lowestLatencyPeerParent, parentLatency)
	d.sendJoinAsChildMsg(lowestLatencyPeerParent, parentLatency, false)
	return
}

func (d *DemmonTree) progressToNextLevel(lowestLatencyPeer peer.Peer) {
	d.logger.Infof("Progressing from level %d to level %d ", d.joinLevel, d.joinLevel+1)
	d.joinLevel++
	if int(d.joinLevel) < len(d.currLevelPeers) {
		for _, c := range d.children[lowestLatencyPeer.String()] {
			d.currLevelPeers[d.joinLevel][c.String()] = c
		}
	} else {
		toAppend := make(map[string]peer.Peer, len(d.children[lowestLatencyPeer.String()]))
		for _, c := range d.children[lowestLatencyPeer.String()] {
			toAppend[c.String()] = c
		}
		d.currLevelPeers = append(d.currLevelPeers, toAppend)
	}

	if int(d.joinLevel) < len(d.currLevelPeersDone) {
		d.currLevelPeersDone[d.joinLevel] = make(map[string]*MeasuredPeer)
	} else {
		d.currLevelPeersDone = append(d.currLevelPeersDone, make(map[string]*MeasuredPeer))
	}

	d.joinTimeoutTimerIds = make(map[string]int)
	for _, p := range d.currLevelPeers[d.joinLevel] {
		d.joinTimeoutTimerIds[p.String()] = d.babel.RegisterTimer(
			d.ID(),
			NewJoinMessageResponseTimeout(d.config.JoinMessageTimeout, p),
		)
		d.sendMessageAndMeasureLatency(NewJoinMessage(), p)
	}
}

// aux functions

func (d *DemmonTree) sendMessageAndMeasureLatency(toSend message.Message, destPeer peer.Peer) {
	d.sendMessageTmpTCPChan(toSend, destPeer)
	d.nodeWatcher.Watch(destPeer, d.ID())
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
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	d.babel.SendMessage(toSend, destPeer, d.ID(), d.ID(), false)
}

func (d *DemmonTree) sendMessageAndDisconnect(toSend message.Message, destPeer peer.Peer) {
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	d.babel.SendMessageAndDisconnect(toSend, destPeer, d.ID(), d.ID())
}

func (d *DemmonTree) getPeersInLevelByLat(level uint16, deadline time.Time) MeasuredPeersByLat {
	if len(d.currLevelPeersDone[level]) == 0 {
		return MeasuredPeersByLat{}
	}
	measuredPeersInLvl := make(MeasuredPeersByLat, 0, len(d.currLevelPeersDone[level]))
	for peerStr, currPeer := range d.currLevelPeersDone[level] {
		currPeer := currPeer
		nodeStats, err := d.nodeWatcher.GetNodeInfoWithDeadline(currPeer.Peer, deadline)
		if err != nil {
			d.logger.Warnf("Do not have latency measurement for %s", currPeer.String())
			delete(d.currLevelPeersDone[level], peerStr)

			continue
		}
		d.currLevelPeersDone[level][peerStr].MeasuredLatency = nodeStats.LatencyCalc().CurrValue()
		measuredPeersInLvl = append(measuredPeersInLvl, d.currLevelPeersDone[level][peerStr])
	}
	sort.Sort(measuredPeersInLvl)
	return measuredPeersInLvl
}

func (d *DemmonTree) generateChildID() PeerID {
	var peerID PeerID
	occupiedIds := make(map[PeerID]bool, d.self.NrChildren())
	for _, child := range d.myChildren {
		childID := child.Chain()[len(child.Chain())-1]
		occupiedIds[childID] = true
	}

	maxID := int(math.Exp2(IDSegmentLen))
	for i := 0; i < maxID; i++ {
		binary.BigEndian.PutUint64(peerID[:], uint64(i))
		_, ok := occupiedIds[peerID]
		if !ok {
			d.logger.Infof("Generated peerID: %+v", peerID)
			return peerID
		}
	}
	d.logger.Panic("Could not generate children ID")
	return PeerID{}
}

func (d *DemmonTree) fallbackToParentInJoin(node peer.Peer) {
	peerParent, ok := d.parents[node.String()]
	if !ok {
		d.joinOverlay()
		return
	}
	info, err := d.nodeWatcher.GetNodeInfo(peerParent.Peer)
	var peerLat time.Duration
	if err != nil {
		d.logger.Errorf("Peer %s has no latency measurement", node.String())
		peerLat = 0
	} else {
		peerLat = info.LatencyCalc().CurrValue()
	}

	d.unwatchPeersInLevelDone(d.joinLevel)
	d.joinLevel--
	d.myPendingParentInJoin = NewMeasuredPeer(peerParent, 0)
	d.sendJoinAsChildMsg(peerParent, peerLat, true)
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
	d.sendJoinAsChildMsg(bestPeer.PeerWithIDChain, bestPeer.MeasuredLatency, true)
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

func (d *DemmonTree) mergeSampleWithEview(
	sample []*PeerWithIDChain,
	sender *PeerWithIDChain,
	nrPeersToMerge, nrPeersToAdd int,
) (sampleToSend, neighboursWithoutSenderDescendants []*PeerWithIDChain) {
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
			append(neighboursWithoutSenderDescendantsAndNotInSample, d.peerMapToArr(d.eView)...)...,
		)
		sampleToSend = append(d.peerMapToArr(sampleToSendMap), append(sample, d.self)...)
	} else {
		sampleToSendMap := getRandSample(
			nrPeersToAdd,
			append(neighboursWithoutSenderDescendantsAndNotInSample, d.peerMapToArr(d.eView)...)...,
		)
		sampleToSend = append(d.peerMapToArr(sampleToSendMap), sample...)
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
			d.addSibling(msgSibling, true)
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

			d.removeSibling(mySibling, true)
		}
	}
}

func (d *DemmonTree) getChildrenAsPeerWithIDChainArray(exclusions ...*PeerWithIDChain) []*PeerWithIDChain {
	toReturn := make([]*PeerWithIDChain, 0, d.self.NrChildren()-1)
	for _, child := range d.myChildren {

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

func (d *DemmonTree) isNodeDown(n nodeWatcher.NodeInfo) bool {
	// d.logger.Infof("Node %s phi: %f", n.Peer, n.Detector.Phi())
	return !n.Detector().IsAvailable()
}

func (d *DemmonTree) removeFromMeasuredPeers(p peer.Peer) {
	delete(d.measuredPeers, p.String())
}

func (d *DemmonTree) addToMeasuredPeers(p *MeasuredPeer) {
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

func (d *DemmonTree) addParent(
	newParent *PeerWithIDChain,
	newGrandParent *PeerWithIDChain,
	myNewChain PeerIDChain,
	parentLatency time.Duration,
	disconnectFromParent, sendDisconnectMsg bool,
) {
	d.logger.Warnf(
		"My level changed: (%d -> %d)",
		d.self.Chain().Level(),
		newParent.Chain().Level()+1,
	) // IMPORTANT FOR VISUALIZER
	d.logger.Warnf("My chain changed: (%+v -> %+v)", d.self.Chain(), myNewChain)
	d.logger.Warnf("My parent changed: (%+v -> %+v)", d.myParent, newParent)

	if peer.PeersEqual(newParent, d.myPendingParentInRecovery) {
		d.myPendingParentInRecovery = nil
	}

	if peer.PeersEqual(newParent, d.myPendingParentInImprovement) {
		d.myPendingParentInImprovement = nil
	}

	if peer.PeersEqual(newParent, d.myPendingParentInJoin) {
		d.myPendingParentInJoin = nil
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

	if !myNewChain.Equal(d.self.chain) {
		d.babel.SendNotification(NewIDChangeNotification(myNewChain))
	}

	d.myGrandParent = newGrandParent
	d.myParent = newParent
	d.self = NewPeerWithIDChain(myNewChain, d.self.Peer, d.self.nChildren, d.self.Version()+1, d.self.Coordinates)
	if d.joinLevel != math.MaxUint16 {
		d.joinLevel = math.MaxUint16
		d.children = nil
		d.currLevelPeersDone = nil
		d.parents = nil
		// d.retries = nil
		d.joinTimeoutTimerIds = nil
	}

	if parentLatency != 0 {
		d.nodeWatcher.WatchWithInitialLatencyValue(newParent, d.ID(), parentLatency)
	} else {
		d.nodeWatcher.Watch(newParent, d.ID())
	}
	d.babel.Dial(d.ID(), newParent, newParent.ToTCPAddr())
	d.removeFromMeasuredPeers(newParent)
	// d.resetSwitchTimer()
	d.resetImproveTimer()

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

func (d *DemmonTree) addChild(newChild *PeerWithIDChain, connect bool, childrenLatency time.Duration) PeerID {
	proposedID := d.generateChildID()
	newChildWithID := NewPeerWithIDChain(
		append(d.self.Chain(), proposedID),
		newChild.Peer,
		newChild.nChildren,
		newChild.Version(),
		newChild.Coordinates,
	)
	if connect {
		if childrenLatency != 0 {
			d.nodeWatcher.WatchWithInitialLatencyValue(newChild, d.ID(), childrenLatency)
		} else {
			d.nodeWatcher.Watch(newChild, d.ID())
		}
		d.babel.Dial(d.ID(), newChild, newChild.ToTCPAddr())
		d.myPendingChildren[newChild.String()] = newChildWithID
	} else {
		d.myChildren[newChild.String()] = newChildWithID
		d.self = NewPeerWithIDChain(
			d.self.Chain(),
			d.self.Peer,
			d.self.nChildren+1,
			d.self.Version()+1,
			d.self.Coordinates,
		)
	}
	d.removeFromMeasuredPeers(newChild)
	d.resetImproveTimer()
	return proposedID
}

func (d *DemmonTree) removeChild(toRemove peer.Peer, disconnect, unwatch bool) {

	if child, ok := d.myPendingChildren[toRemove.String()]; ok {
		delete(d.myPendingChildren, toRemove.String())
		if disconnect {
			d.babel.SendNotification(NewNodeDownNotification(child, d.getInView()))
			d.babel.Disconnect(d.ID(), toRemove)
		}
		if unwatch {
			d.nodeWatcher.Unwatch(toRemove, d.ID())
		}
		return
	}

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
		if disconnect {
			d.babel.SendNotification(NewNodeDownNotification(child, d.getInView()))
			d.babel.Disconnect(d.ID(), toRemove)
		}
		if unwatch {
			d.nodeWatcher.Unwatch(toRemove, d.ID())
		}
	} else {
		d.logger.Panic("Removing child not in myChildren or myPendingChildren")
	}
}

func (d *DemmonTree) addSibling(newSibling *PeerWithIDChain, connect bool) {
	if _, ok := d.myPendingSiblings[newSibling.String()]; !ok {
		if connect {
			d.myPendingSiblings[newSibling.String()] = newSibling
			d.nodeWatcher.Watch(newSibling.Peer, d.ID())
			d.babel.Dial(d.ID(), newSibling.Peer, newSibling.Peer.ToTCPAddr())
		} else {
			d.mySiblings[newSibling.String()] = newSibling
		}
		d.removeFromMeasuredPeers(newSibling)
	}
}

func (d *DemmonTree) removeSibling(toRemove peer.Peer, disconnect bool) {
	if sibling, ok := d.myPendingSiblings[toRemove.String()]; ok {
		delete(d.myPendingSiblings, toRemove.String())
		if disconnect {
			d.babel.SendNotification(NewNodeDownNotification(sibling, d.getInView()))
			d.babel.Disconnect(d.ID(), toRemove)
			d.nodeWatcher.Unwatch(toRemove, d.ID())
		}
		return
	}

	if sibling, ok := d.mySiblings[toRemove.String()]; ok {
		delete(d.mySiblings, toRemove.String())
		if disconnect {
			d.nodeWatcher.Unwatch(sibling, d.ID())
			d.babel.SendNotification(NewNodeDownNotification(sibling, d.getInView()))
			d.babel.Disconnect(d.ID(), toRemove)
		}
		return
	}

	d.logger.Panic("Removing sibling not in mySiblings or myPendingSiblings")
}

func (d *DemmonTree) resetImproveTimer() {
	if d.improveTimerID != -1 {
		err := d.babel.CancelTimer(d.improveTimerID)
		if err != nil {
			d.improveTimerID = -1
			return
		}
		d.improveTimerID = d.babel.RegisterTimer(
			d.ID(),
			NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration),
		)
	}
}

func (d *DemmonTree) getPeerWithChainAsMeasuredPeer(p *PeerWithIDChain) (*MeasuredPeer, errors.Error) {
	info, err := d.nodeWatcher.GetNodeInfo(p)
	if err != nil {
		return nil, err
	}
	return NewMeasuredPeer(p, info.LatencyCalc().CurrValue()), nil
}

func (d *DemmonTree) peerMapToArr(peers map[string]*PeerWithIDChain) []*PeerWithIDChain {
	toReturn := make([]*PeerWithIDChain, 0, len(peers))
	for _, p := range peers {
		toReturn = append(toReturn, p)
	}
	return toReturn
}

func (d *DemmonTree) broadcastMessage(msg message.Message, sendToSiblings, sendToChildren, sendToParent bool) {
	if sendToSiblings {
		for _, s := range d.mySiblings {
			d.babel.SendMessage(msg, s, d.ID(), d.ID(), true)
		}
	}

	if sendToChildren {
		for _, s := range d.myChildren {
			d.babel.SendMessage(msg, s, d.ID(), d.ID(), true)
		}
	}

	if sendToParent {
		if d.myParent != nil {
			d.babel.SendMessage(msg, d.myParent, d.ID(), d.ID(), true)
		}
	}
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

// func (d *DemmonTree) resetSwitchTimer() {
// 	if d.switchTimerId != -1 {
// 		err := d.babel.CancelTimer(d.switchTimerId)
// 		if err != nil {
// 			d.switchTimerId = -1
// 			return
// 		}
// 		d.switchTimerId = d.babel.RegisterTimer(d.ID(), NewSwitchTimer(d.config.CheckSwitchOportunityTimeout))
// 	}
// }

// func (d *DemmonTree) handleSwitchTimer(timer timer.Timer) {
// 	if !d.config.EnableSwitch {
// 		return
// 	}

// 	d.logger.Info("SwitchTimer trigger")
// 	if d.switchTimerId == -1 {
// 		d.switchTimerId = d.babel.RegisterTimer(d.ID(), NewSwitchTimer(d.config.CheckSwitchOportunityTimeout))
// 		return
// 	}
// 	d.switchTimerId = d.babel.RegisterTimer(d.ID(), NewSwitchTimer(d.config.CheckSwitchOportunityTimeout))

// 	if d.myParent == nil ||
// len(d.self.Chain()) == 0 ||
// d.myPendingParentInImprovement != nil ||
// d.myPendingParentInRecovery != nil ||
// uint16(len(d.myChildren)) < d.config.MinGrpSize {
// 		return
// 	}

// 	r := rand.Float32()
// 	if r < d.config.SwitchProbability {
// 		return
// 	}

// 	myLatTotal := &big.Int{}
// 	for childId, child := range d.myChildren {
// 		currNodeStats, err := d.nodeWatcher.GetNodeInfo(child.Peer)
// 		if err != nil {
// 			d.logger.Errorf("Do not yet have latency measurement for children %s", childId)
// 			return
// 		}
// 		myLatTotal.Add(myLatTotal, big.NewInt(int64(currNodeStats.LatencyCalc().CurrValue())))
// 	}

// 	var bestCandidateToSwitch *PeerWithIDChain
// 	var bestCandidateLatTotal *big.Int

// 	for childId, candidateToSwitch := range d.myChildren {
// 		candidateToSwitchSiblingLatencies := d.myChildrenLatencies[candidateToSwitch.String()]

// 		if len(candidateToSwitchSiblingLatencies) < len(d.myChildren)-1 {
// 			d.logger.Warnf("Skipping children %s because it does not have enough measured siblings", candidateToSwitch.String())
// 			continue
// 		}

// 		latTotal := &big.Int{}

// 		currNodeStats, err := d.nodeWatcher.GetNodeInfo(candidateToSwitch.Peer)
// 		if err != nil {
// 			d.logger.Errorf("Do not have latency measurement for children %s", childId)
// 			continue
// 		}
// 		latTotal.Add(latTotal, big.NewInt(int64(currNodeStats.LatencyCalc().CurrValue())))

// 		d.logger.Infof("candidateToSwitchSiblingLatencies: %+v:", candidateToSwitchSiblingLatencies)

// 		for i := 0; i < len(candidateToSwitchSiblingLatencies); i++ {
// 			currCandidateAbsorbtion := candidateToSwitchSiblingLatencies[i]
// 			latTotal.Add(latTotal, big.NewInt(int64(currCandidateAbsorbtion.MeasuredLatency)))
// 		}

// 		if bestCandidateLatTotal == nil {
// 			bestCandidateToSwitch = candidateToSwitch
// 			bestCandidateLatTotal = latTotal
// 			continue
// 		}

// 		if latTotal.Cmp(bestCandidateLatTotal) == -1 {
// 			bestCandidateToSwitch = candidateToSwitch
// 			bestCandidateLatTotal = latTotal
// 			continue
// 		}
// 	}

// 	if bestCandidateToSwitch == nil {
// 		return
// 	}

// 	if bestCandidateLatTotal.Cmp(myLatTotal) == -1 { // bestCandidateLatTotal < myLatTotal
// 		bestCandidateLatImprovement := &big.Int{}
// 		bestCandidateLatImprovement = bestCandidateLatImprovement.Sub(myLatTotal, bestCandidateLatTotal)
// 		minLatencyImprovementForSwitch := big.NewInt(int64(d.config.MinLatencyImprovementPerPeerForSwitch) * int64(len(d.myChildren)-1))
// 		if bestCandidateLatImprovement.Cmp(minLatencyImprovementForSwitch) == 1 {
// 			// bestCandidateLatImprovement > minLatencyImprovementForSwitch
// 			bestCandidateChain := bestCandidateToSwitch.Chain()
// 			for _, child := range d.myChildren {
// 				childrenToSend := d.getChildrenAsPeerWithIdChaiDArray(bestCandidateToSwitch, child)
// 				toSend := NewSwitchMessage(bestCandidateToSwitch, d.myParent, childrenToSend, true, false)
// 				d.sendMessage(toSend, child.Peer)
// 			}
// 			childrenToSend := d.getChildrenAsPeerWithIdChaiDArray(bestCandidateToSwitch)
// 			toSend := NewSwitchMessage(bestCandidateToSwitch, d.myParent, childrenToSend, false, true)
// 			for _, child := range d.myChildren {
// 				d.addSibling(child, false, false)
// 				d.removeChild(child, false, false)
// 			}
// 			d.sendMessageAndDisconnect(toSend, d.myParent)
// 			d.addParent(bestCandidateToSwitch, d.myGrandParent, bestCandidateChain, false, true, 0, false, false, false)
// 		}
// 	}
// }

// // VERSION where nodes n nodes  with lowest latency are picked, and the one with least overall latency to its siblings get picked
// func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
// 	d.absorbTimerId = d.babel.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
// 	// d.logger.Info("handleCheckChildrenSize timer trigger")

// 	if d.self.NrChildren() == 0 {
// 		// d.logger.Info("d.self.NrChildren() == 0, returning...")
// 		return
// 	}

// 	if d.self.NrChildren() <= d.config.MaxGrpSize {
// 		// d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
// 		return
// 	}

// 	// d.logger.Infof("myChildren: %+v:", d.myChildren)

// 	childrenAsMeasuredPeers := make(MeasuredPeersByLat, 0, d.self.NrChildren())
// 	for _, children := range d.myChildren {
// 		nodeStats, err := d.nodeWatcher.GetNodeInfo(children)
// 		if err != nil {
// 			d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", children.String())
// 			return
// 		}
// 		currLat := nodeStats.LatencyCalc().CurrValue()
// 		childrenAsMeasuredPeers = append(childrenAsMeasuredPeers, NewMeasuredPeer(children, currLat))
// 	}

// 	toPrint := ""
// 	for _, possibility := range childrenAsMeasuredPeers {
// 		toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.String(), possibility.MeasuredLatency)
// 	}
// 	d.logger.Infof("childrenAsMeasuredPeers: %s", toPrint)

// 	sort.Sort(childrenAsMeasuredPeers)

// 	candidatesToAbsorb := make([]*MeasuredPeer, 0, d.config.NrPeersToConsiderAsParentToAbsorb)
// 	i := 0
// 	for _, measuredChild := range childrenAsMeasuredPeers {
// 		if len(candidatesToAbsorb) == d.config.NrPeersToConsiderAsParentToAbsorb {
// 			break
// 		}
// 		if measuredChild.NrChildren()+d.config.NrPeersToAbsorb < d.config.MaxGrpSize {
// 			candidatesToAbsorb = append(candidatesToAbsorb, measuredChild)
// 		}
// 		i++
// 	}
// 	if len(candidatesToAbsorb) == 0 {
// 		d.logger.Warn("Have no candidates to send absorb message to")
// 	}

// 	toPrint = ""
// 	for _, possibility := range candidatesToAbsorb {
// 		toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.String(), possibility.MeasuredLatency)
// 	}
// 	d.logger.Infof("candidatesToAbsorb: %s:", toPrint)

// 	var bestCandidate *MeasuredPeer
// 	var bestCandidateChildrenToAbsorb MeasuredPeersByLat
// 	var bestCandidateLatTotal *big.Int

// 	for _, candidateToAbsorb := range candidatesToAbsorb {
// 		currCandidateChildren := MeasuredPeersByLat{}
// 		candidateToAbsorbSiblingLatencies := d.myChildrenLatencies[candidateToAbsorb.String()]

// 		if len(candidateToAbsorbSiblingLatencies) < int(d.config.NrPeersToAbsorb) {
// 			continue
// 		}

// 		latTotal := &big.Int{}
// 		sort.Sort(candidateToAbsorbSiblingLatencies)
// 		d.logger.Infof("candidateToAbsorbSiblingLatencies: %+v:", candidateToAbsorbSiblingLatencies)

// 		for i := uint16(0); i < d.config.NrPeersToAbsorb; i++ {
// 			currCandidateAbsorbtion := candidateToAbsorbSiblingLatencies[i]
// 			latTotal.Add(latTotal, big.NewInt(int64(currCandidateAbsorbtion.MeasuredLatency.Nanoseconds())))
// 			currCandidateChildren = append(currCandidateChildren, currCandidateAbsorbtion)
// 		}

// 		if bestCandidateLatTotal == nil {
// 			bestCandidate = candidateToAbsorb
// 			bestCandidateChildrenToAbsorb = currCandidateChildren
// 			bestCandidateLatTotal = latTotal
// 			continue
// 		}

// 		if latTotal.Cmp(bestCandidateLatTotal) == -1 && len(bestCandidateChildrenToAbsorb) == int(d.config.NrPeersToAbsorb) {
// 			bestCandidate = candidateToAbsorb
// 			bestCandidateChildrenToAbsorb = currCandidateChildren
// 			bestCandidateLatTotal = latTotal
// 			continue
// 		}
// 	}

// 	if bestCandidate == nil {
// 		d.logger.Infof("No candidates to absorb")
// 		return
// 	}

// 	toPrint = ""
// 	for _, possibility := range bestCandidateChildrenToAbsorb {
// 		toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.String(), possibility.MeasuredLatency)
// 	}
// 	// d.logger.Infof("Sending absorb message with bestCandidateChildrenToAbsorb: %s to: %s", toPrint, bestCandidate.String())
// 	toSend := NewAbsorbMessage(bestCandidateChildrenToAbsorb, bestCandidate.PeerWithIDChain)

// 	d.logger.Infof("Sending absorb message %+v to %s", toSend, bestCandidate.String())
// 	for _, newGrandChild := range toSend.PeersToAbsorb {
// 		d.sendMessageAndDisconnect(toSend, newGrandChild)
// 		d.removeChild(newGrandChild, false, true)
// 	}

// 	for _, child := range d.myChildren {
// 		d.sendMessage(toSend, child)
// 	}

// 	// d.resetSwitchTimer()
// 	d.resetImproveTimer()
// }

// VERSION where nodes 1 node with lowest latency is picked, and gets new children which are its lowest latency peers
// func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
// 	d.babel.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
// 	d.logger.Info("handleCheckChildrenSize timer trigger")

// 	if d.self.NrChildren() == 0 {
// 		d.logger.Info("d.self.NrChildren() == 0, returning...")
// 		return
// 	}

// 	if len(d.myChildren) < int(d.config.MaxGrpSize) {
// 		d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
// 		return
// 	}

// 	toPrint := ""
// 	for _, child := range d.myChildren {
// 		toPrint = toPrint + "; " + child.String()
// 	}
// 	d.logger.Infof("myChildren: %s:", toPrint)

// 	childrenAsMeasuredPeers := make(MeasuredPeersByLat, 0, d.self.NrChildren())
// 	for _, children := range d.myChildren {
// 		nodeStats, err := d.nodeWatcher.GetNodeInfo(children)
// 		if err != nil {
// 			d.logger.Warnf("Do not have latency measurement for %s", children.String())
// 			continue
// 		}
// 		currLat := nodeStats.LatencyCalc().CurrValue()
// 		childrenAsMeasuredPeers = append(childrenAsMeasuredPeers, NewMeasuredPeer(children, currLat))
// 	}

// 	toPrint = ""
// 	for _, possibility := range childrenAsMeasuredPeers {
// 		toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.String(), possibility.MeasuredLatency)
// 	}
// 	d.logger.Infof("childrenAsMeasuredPeers: %s", toPrint)

// 	if len(childrenAsMeasuredPeers) == 0 {
// 		d.logger.Warn("Have no candidates to send absorb message to")
// 		return
// 	}

// 	sort.Sort(childrenAsMeasuredPeers)
// 	for i := 0; i < len(childrenAsMeasuredPeers); i++ {
// 		candidateToAbsorb := childrenAsMeasuredPeers[i]
// 		candidateToAbsorbSiblingLatencies := d.myChildrenLatencies[candidateToAbsorb.String()]
// 		sort.Sort(candidateToAbsorbSiblingLatencies)
// 		toPrint = ""
// 		for _, possibility := range candidateToAbsorbSiblingLatencies {
// 			toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.String(), possibility.MeasuredLatency)
// 		}
// 		d.logger.Infof("candidateToAbsorbSiblingLatencies: %s:", toPrint)

// 		toAbsorb := MeasuredPeersByLat{}
// 		for i := uint16(0); i < d.config.NrPeersToAbsorb; i++ {
// 			if candidateToAbsorbSiblingLatencies[i].MeasuredLatency == math.MaxInt64 {
// 				continue
// 			}
// 			toAbsorb = append(toAbsorb, candidateToAbsorbSiblingLatencies[i])
// 		}

// 		toPrint = ""
// 		for _, possibility := range toAbsorb {
// 			toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.String(), possibility.MeasuredLatency)
// 		}

// 		d.logger.Infof("Sending absorb message with bestCandidateChildrenToAbsorb: %s to: %s", toPrint, candidateToAbsorb.String())

// 		toSend := NewAbsorbMessage(toAbsorb, candidateToAbsorb.PeerWithIDChain)
// 		d.logger.Infof("Sending absorb message %+v to %s", toSend, candidateToAbsorb.String())

// 		for _, child := range d.myChildren {
// 			d.sendMessage(toSend, child)
// 		}

// 		for _, newGrandChildren := range toAbsorb {
// 			d.addToMeasuredPeers(newGrandChildren.PeerWithIDChain)
// 			d.removeChild(newGrandChildren.PeerWithIDChain, true, true)
// 		}
// 		return
// 	}
// }

// VERSION where nodes with lowest latency pair are picked, the new children is the member of the pair with lowest latency to the parent
// func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
// 	d.absorbTimerId = d.babel.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))

// 	// d.logger.Info("handleCheckChildrenSize timer trigger")

// 	if d.self.NrChildren() == 0 ||
// d.myPendingParentInImprovement != nil ||
// d.myPendingParentInAbsorb != nil ||
// d.myPendingParentInJoin != nil {
// 		// d.logger.Info("d.self.NrChildren() == 0, returning...")
// 		return
// 	}

// 	if uint16(len(d.myChildren)) <= d.config.MaxGrpSize {
// 		// d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
// 		return
// 	}

// 	// d.logger.Infof("myChildren: %+v:", d.myChildren)

// 	// toPrint := ""
// 	// for _, possibility := range childrenAsMeasuredPeers {
// 	// 	toPrint = toPrint + "; " + fmt.Sprintf("%s:%s", possibility.String(), possibility.MeasuredLatency)
// 	// }
// 	// d.logger.Infof("childrenAsMeasuredPeers: %s", toPrint)

// 	var lowestLatPairLat time.Duration = time.Duration(math.MaxInt64)
// 	var peerAbsorber *PeerWithIDChain
// 	var peerToKick *PeerWithIDChain

// 	for _, peer1 := range d.myChildren {
// 		candidateToAbsorbSiblingLatencies := d.myChildrenLatencies[peer1.String()]
// 		// d.logger.Infof("candidateToAbsorbSiblingLatencies: %+v:", candidateToAbsorbSiblingLatencies)
// 		for _, peer2 := range candidateToAbsorbSiblingLatencies {

// 			if peer2 == nil {
// 				continue
// 			}

// 			if _, isChild := d.myChildren[peer2.String()]; !isChild {
// 				d.logger.Errorf("Candidate %s to be absorbed is not a child", peer2.String())
// 				continue
// 			}

// 			if peer.PeersEqual(peer2, peer1) {
// 				continue
// 			}

// 			if peer2.MeasuredLatency == 0 {
// 				continue
// 			}

// 			// nodeStats, err := d.nodeWatcher.GetNodeInfo(candidateToBeAbsorbed)
// 			// if err != nil {
// 			// 	d.logger.Errorf("Cannot send absorbMessage to %s as i have no latency measurement", candidateToBeAbsorbed.String())
// 			// 	continue
// 			// }
// 			// myLatToPeerToBeAbsorbed := nodeStats.LatencyCalc().CurrValue()

// 			peer1NodeStats, err := d.nodeWatcher.GetNodeInfo(peer1)
// 			if err != nil {
// 				d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", peer1.String())
// 				continue
// 			}
// 			latToPeer1 := peer1NodeStats.LatencyCalc().CurrValue()

// 			peer2NodeStats, err := d.nodeWatcher.GetNodeInfo(peer2)
// 			if err != nil {
// 				d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", peer2.String())
// 				continue
// 			}
// 			latToPeer2 := peer2NodeStats.LatencyCalc().CurrValue()

// 			if peer2.MeasuredLatency < lowestLatPairLat {
// 				if latToPeer1 < latToPeer2 && latToPeer1 < lowestLatPairLat {
// 					peerAbsorber = peer1
// 					peerToKick = peer2.PeerWithIDChain
// 					lowestLatPairLat = peer2.MeasuredLatency
// 				} else if latToPeer2 < lowestLatPairLat {
// 					peerAbsorber = peer2.PeerWithIDChain
// 					peerToKick = peer1
// 					lowestLatPairLat = peer2.MeasuredLatency
// 				}
// 			}
// 		}
// 	}

// 	if peerAbsorber == nil || peerToKick == nil {
// 		return
// 	}

// 	d.logger.Infof("Sending absorb message with peerToAbsorb: %s, peerAbsorber: %s", peerToKick.String(), peerAbsorber.String())
// 	toSend := NewAbsorbMessage(peerToKick, peerAbsorber)
// 	d.sendMessage(toSend, peerToKick)
// 	d.resetImproveTimer()
// }

// VERSION where peer with highest latency is picked, and joins as child to its nearest peer
// func (d *DemmonTree) handleEvalMeasuredPeersTimer(evalMeasuredPeersTimer timer.Timer) {

// 	if d.improveTimerId == -1 {
// 		d.improveTimerId = d.babel.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
// 		return
// 	}
// 	d.improveTimerId = d.babel.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))

// 	d.logger.Info("EvalMeasuredPeersTimer trigger...")

// 	if len(d.self.Chain()) == 0 || d.myParent == nil || d.myPendingParentInImprovement != nil {
// 		return
// 	}

// 	r := rand.Float32()
// 	if r > d.config.AttemptImprovePositionProbability {
// 		return
// 	}
// 	keys := make(map[string]bool) // remove dups
// 	i := 0
// 	for _, measuredPeer := range d.measuredPeers {
// 		_, value := keys[measuredPeer.String()]
// 		if !d.isNeighbour(measuredPeer) && !measuredPeer.IsDescendentOf(d.self.Chain()) && !value {
// 			d.logger.Infof("%s : %s", measuredPeer.String(), measuredPeer.MeasuredLatency)
// 			d.measuredPeers[i] = measuredPeer
// 			keys[measuredPeer.String()] = true
// 			i++
// 		}
// 	}
// 	for j := i; j < len(d.measuredPeers); j++ {
// 		d.measuredPeers[j] = nil
// 	}
// 	d.measuredPeers = d.measuredPeers[:i]

// 	d.logger.Info("Evaluating measuredPeers peers...")
// 	if len(d.measuredPeers) == 0 {
// 		d.logger.Info("returning due to len(d.measuredPeers) == 0")
// 		return
// 	}
// 	parentStats, err := d.nodeWatcher.GetNodeInfo(d.myParent)
// 	if err != nil {
// 		d.logger.Error(err.Reason())
// 		return
// 	}

// 	parentLatency := parentStats.LatencyCalc().CurrValue()
// 	sort.Sort(d.measuredPeers)
// 	for _, measuredPeer := range d.measuredPeers {
// 		d.logger.Infof("Least latency peer with children: %s", measuredPeer.String())
// 		// parent latency is higher than latency to peer
// 		latencyImprovement := parentLatency - measuredPeer.MeasuredLatency
// 		if latencyImprovement < 0 {
// 			return
// 		}

// 		if latencyImprovement > d.config.MinLatencyImprovementToImprovePosition {
// 			if measuredPeer.Chain().Level() >= d.self.Chain().Level()-1 {
// 				if len(d.myChildren) > 0 {
// 					continue // TODO send message
// 				}
// 			}

// 			d.logger.Infof("Improving position towards: %s", measuredPeer.String())
// 			d.logger.Infof("latencyImprovement: %s", latencyImprovement)
// 			d.logger.Infof("parentLatency: %s", parentLatency)
// 			d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
// 			aux := *measuredPeer
// 			d.myPendingParentInImprovement = &aux
// 			d.sendJoinAsChildMsg(measuredPeer.PeerWithIDChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
// 			return
// 		}

// 		if measuredPeer.Chain().Level() < d.myParent.Chain().Level() {
// 			if measuredPeer.NrChildren() < d.config.MaxGrpSize { // must go UP in levels
// 				aux := *measuredPeer
// 				d.myPendingParentInImprovement = &aux
// 				d.sendJoinAsChildMsg(measuredPeer.PeerWithIDChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
// 				d.logger.Infof("Improving position towards: %s", measuredPeer.String())
// 				d.logger.Infof("latencyImprovement: %s", latencyImprovement)
// 				d.logger.Infof("parentLatency: %s", parentLatency)
// 				d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
// 				return
// 			}
// 		}
// 		d.logger.Infof("Not improving position towards best peer: %s", measuredPeer.String())
// 		d.logger.Infof("latencyImprovement: %s", latencyImprovement)
// 		d.logger.Infof("parentLatency: %s", parentLatency)
// 		d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
// 	}
// }

// func (d *DemmonTree) handleAbsorbMessage(sender peer.Peer, m message.Message) {
// 	absorbMessage := m.(absorbMessage)

// 	if d.myParent == nil {
// 		d.logger.Errorf("Got absorbMessage but parent is nil...")
// 		return
// 	}

// 	if !peer.PeersEqual(d.myParent, sender) {
// 		d.logger.Warnf("Got absorbMessage: %+v from not my parent %s (my parent: %s)", m, sender.String(), d.myParent.String())
// 		return
// 	}

// 	d.logger.Infof("Got absorbMessage: %+v from %s", m, sender.String())
// 	toPrint := ""
// 	for _, peerToAbsorb := range absorbMessage.PeersToAbsorb {
// 		toPrint = toPrint + "; " + peerToAbsorb.String()
// 	}
// 	d.logger.Infof("peerAbsorber in absorbMessage: %s:", absorbMessage.PeerAbsorber.String())
// 	d.logger.Infof("peersToAbsorb in absorbMessage: %s:", toPrint)

// 	// if peer.PeersEqual(d.babel.SelfPeer(), absorbMessage.PeerAbsorber) {
// 	// 	for _, aux := range absorbMessage.PeersToAbsorb {
// 	// 		if newChildren, ok := d.mySiblings[aux.String()]; ok {
// 	// 			d.removeSibling(newChildren, false, false)
// 	// 			// d.addChild(newChildren, true, true, 0)
// 	// 		}
// 	// 		// d.addChild(aux, true, true, 0)
// 	// 	}
// 	// 	return
// 	// }

// 	for _, p := range absorbMessage.PeersToAbsorb {
// 		if peer.PeersEqual(d.babel.SelfPeer(), p) {
// 			if sibling, ok := d.mySiblings[absorbMessage.PeerAbsorber.String()]; ok {
// 				d.removeSibling(sibling, false, false)
// 				var peerLat time.Duration
// 				if nodeInfo, err := d.nodeWatcher.GetNodeInfo(sibling); err == nil {
// 					peerLat = nodeInfo.LatencyCalc().CurrValue()
// 				}
// 				d.myPendingParentInAbsorb = absorbMessage.PeerAbsorber
// 				toSend := NewJoinAsChildMessage(d.self, peerLat, absorbMessage.PeerAbsorber.Chain(), false)
// 				d.sendMessage(toSend, absorbMessage.PeerAbsorber)
// 			} else {
// 				toSend := NewJoinAsChildMessage(d.self, 0, absorbMessage.PeerAbsorber.Chain(), false)
// 				d.sendMessageTmpTCPChan(toSend, absorbMessage.PeerAbsorber)
// 			}
// 			// for _, sibling := range d.mySiblings {
// 			// 	found := false
// 			// 	for _, p := range absorbMessage.PeersToAbsorb {
// 			// 		if peer.PeersEqual(p, sibling) {
// 			// 			found = true
// 			// 			break
// 			// 		}
// 			// 	}
// 			// 	if !found {
// 			// 		d.removeSibling(sibling, true, true)
// 			// 	}
// 			// }
// 			// return
// 		}
// 	}

// 	// for _, p := range absorbMessage.PeersToAbsorb {
// 	// 	d.removeSibling(p, true, true)
// 	// }
// }

// func (d *DemmonTree) handleAbsorbMessage(sender peer.Peer, m message.Message) {
// 	absorbMessage := m.(absorbMessage)

// 	if d.myParent == nil {
// 		d.logger.Errorf("Got absorbMessage but parent is nil...")
// 		return
// 	}

// 	if !peer.PeersEqual(d.myParent, sender) {
// 		d.logger.Warnf("Got absorbMessage: %+v from not my parent %s (my parent: %s)", m, sender.String(), d.myParent.String())
// 		return
// 	}

// 	d.logger.Infof("Got absorbMessage: %+v from %s", m, sender.String())
// 	toPrint := ""
// 	for _, peerToAbsorb := range absorbMessage.PeersToAbsorb {
// 		toPrint = toPrint + "; " + peerToAbsorb.String()
// 	}
// 	d.logger.Infof("peerAbsorber in absorbMessage: %s:", absorbMessage.PeerAbsorber.String())
// 	d.logger.Infof("peersToAbsorb in absorbMessage: %s:", toPrint)

// 	// if peer.PeersEqual(d.babel.SelfPeer(), absorbMessage.PeerAbsorber) {
// 	// 	for _, aux := range absorbMessage.PeersToAbsorb {
// 	// 		if newChildren, ok := d.mySiblings[aux.String()]; ok {
// 	// 			d.removeSibling(newChildren, false, false)
// 	// 			// d.addChild(newChildren, true, true, 0)
// 	// 		}
// 	// 		// d.addChild(aux, true, true, 0)
// 	// 	}
// 	// 	return
// 	// }

// 	for _, p := range absorbMessage.PeersToAbsorb {
// 		if peer.PeersEqual(d.babel.SelfPeer(), p) {
// 			if sibling, ok := d.mySiblings[absorbMessage.PeerAbsorber.String()]; ok {
// 				d.removeSibling(sibling, false, false)
// 				var peerLat time.Duration
// 				if nodeInfo, err := d.nodeWatcher.GetNodeInfo(sibling); err == nil {
// 					peerLat = nodeInfo.LatencyCalc().CurrValue()
// 				}
// 				d.myPendingParentInAbsorb = absorbMessage.PeerAbsorber
// 				toSend := NewJoinAsChildMessage(d.self, peerLat, absorbMessage.PeerAbsorber.Chain(), false)
// 				d.sendMessage(toSend, absorbMessage.PeerAbsorber)
// 			} else {
// 				toSend := NewJoinAsChildMessage(d.self, 0, absorbMessage.PeerAbsorber.Chain(), false)
// 				d.sendMessageTmpTCPChan(toSend, absorbMessage.PeerAbsorber)
// 			}
// 			// for _, sibling := range d.mySiblings {
// 			// 	found := false
// 			// 	for _, p := range absorbMessage.PeersToAbsorb {
// 			// 		if peer.PeersEqual(p, sibling) {
// 			// 			found = true
// 			// 			break
// 			// 		}
// 			// 	}
// 			// 	if !found {
// 			// 		d.removeSibling(sibling, true, true)
// 			// 	}
// 			// }
// 			// return
// 		}
// 	}

// 	// for _, p := range absorbMessage.PeersToAbsorb {
// 	// 	d.removeSibling(p, true, true)
// 	// }
// }

// func (d *DemmonTree) handleEvalMeasuredPeersTimer(evalMeasuredPeersTimer timer.Timer) {

// 	if d.improveTimerId == -1 {
// 		d.improveTimerId = d.babel.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
// 		return
// 	}

// 	d.improveTimerId = d.babel.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
// 	d.logger.Info("EvalMeasuredPeersTimer trigger...")

// 	if len(d.self.Chain()) == 0 ||
// d.myParent == nil ||
// d.myPendingParentInImprovement != nil ||
// d.myPendingParentInAbsorb != nil ||
// d.myPendingParentInJoin != nil {
// 		d.logger.Info("error")
// 		return
// 	}

// 	r := rand.Float32()
// 	if r > d.config.AttemptImprovePositionProbability {
// 		d.logger.Info("returning due to  r > d.config.AttemptImprovePositionProbability")
// 		return
// 	}

// 	keys := make(map[string]bool) // remove dups
// 	i := 0
// 	for _, measuredPeer := range d.measuredPeers {
// 		_, value := keys[measuredPeer.String()]
// 		if !measuredPeer.IsDescendentOf(d.self.Chain()) && !value {
// 			d.measuredPeers[i] = measuredPeer
// 			keys[measuredPeer.String()] = true
// 			i++
// 		}
// 	}
// 	for j := i; j < len(d.measuredPeers); j++ {
// 		d.measuredPeers[j] = nil
// 	}
// 	d.measuredPeers = d.measuredPeers[:i]

// 	d.logger.Info("Evaluating measuredPeers...")
// 	toPrint := ""
// 	for _, measuredPeer := range d.measuredPeers {
// 		toPrint = toPrint + "; " + fmt.Sprintf("%s : %s", measuredPeer.String(), measuredPeer.MeasuredLatency)
// 	}
// 	d.logger.Infof("d.measuredPeers:  %s:", toPrint)

// 	if len(d.measuredPeers) == 0 {
// 		d.logger.Warn("returning due to len(d.measuredPeers) == 0")
// 		return
// 	}
// 	parentStats, err := d.nodeWatcher.GetNodeInfo(d.myParent)
// 	if err != nil {
// 		d.logger.Error("returning due to  not having latency measurement for parent")
// 		d.logger.Error(err.Reason())
// 		return
// 	}

// 	parentLatency := parentStats.LatencyCalc().CurrValue()
// 	sort.Sort(d.measuredPeers)
// 	for _, measuredPeer := range d.measuredPeers {
// 		// parent latency is higher than latency to peer
// 		latencyImprovement := parentLatency - measuredPeer.MeasuredLatency
// 		if latencyImprovement < 0 {
// 			d.logger.Infof("returning due to latencyImprovement < 0")
// 			return
// 		}

// 		if latencyImprovement > d.config.MinLatencyImprovementToImprovePosition {
// 			if measuredPeer.Chain().Level() > d.self.Chain().Level()-1 {
// 				if len(d.myChildren) > 0 {
// 					continue // TODO send message
// 				}
// 			}

// 			d.logger.Infof("Improving position towards: %s", measuredPeer.String())
// 			d.logger.Infof("latencyImprovement: %s", latencyImprovement)
// 			d.logger.Infof("parentLatency: %s", parentLatency)
// 			d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
// 			aux := *measuredPeer
// 			d.myPendingParentInImprovement = &aux
// 			d.sendJoinAsChildMsg(measuredPeer.PeerWithIDChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
// 			return
// 		}

// 		// if measuredPeer.Chain().Level() < d.self.Chain().Level()-1 {
// 		// 	// if measuredPeer.NrChildren() < d.config.MaxGrpSize { // must go UP in levels
// 		// 	aux := *measuredPeer
// 		// 	d.myPendingParentInImprovement = &aux
// 		// 	d.sendJoinAsChildMsg(measuredPeer.PeerWithIDChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
// 		// 	d.logger.Infof("Improving position towards: %s", measuredPeer.String())
// 		// 	d.logger.Infof("latencyImprovement: %s", latencyImprovement)
// 		// 	d.logger.Infof("parentLatency: %s", parentLatency)
// 		// 	d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
// 		// 	// return
// 		// 	// }
// 		// }
// 		// d.logger.Infof("Not improving position towards best peer: %s", measuredPeer.String())
// 		// d.logger.Infof("latencyImprovement: %s", latencyImprovement)
// 		// d.logger.Infof("parentLatency: %s", parentLatency)
// 		// d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
// 	}
// }

// // VERSION where nodes with highest latency node gets kicked towards its lowest latency peer
// func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
// 	d.absorbTimerId = d.babel.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))

// 	// d.logger.Info("handleCheckChildrenSize timer trigger")

// 	if d.self.NrChildren() == 0 ||
// d.myPendingParentInImprovement != nil ||
// d.myPendingParentInAbsorb != nil ||
// d.myPendingParentInJoin != nil {
// 		// d.logger.Info("d.self.NrChildren() == 0, returning...")
// 		return
// 	}

// 	if uint16(len(d.myChildren)) <= d.config.MaxGrpSize {
// 		// d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
// 		return
// 	}

// 	childrenAsMeasuredPeers := make(MeasuredPeersByLat, 0, d.self.NrChildren())
// 	for _, children := range d.myChildren {
// 		nodeStats, err := d.nodeWatcher.GetNodeInfo(children)
// 		if err != nil {
// 			d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", children.String())
// 			return
// 		}
// 		currLat := nodeStats.LatencyCalc().CurrValue()
// 		childrenAsMeasuredPeers = append(childrenAsMeasuredPeers, NewMeasuredPeer(children, currLat))
// 	}

// 	var peerAbsorber *PeerWithIDChain
// 	var peerToKick *PeerWithIDChain
// 	sort.Sort(childrenAsMeasuredPeers)

// 	for i := len(childrenAsMeasuredPeers) - 1; i >= 0; i-- {
// 		candidateToBeAbsorbed := childrenAsMeasuredPeers[i]
// 		candidateToBeAbsorbedSiblingLatencies := d.myChildrenLatencies[candidateToBeAbsorbed.String()]
// 		sort.Sort(candidateToBeAbsorbedSiblingLatencies)
// 		for _, candidateToAbsorb := range candidateToBeAbsorbedSiblingLatencies {

// 			if candidateToAbsorb == nil {
// 				continue
// 			}

// 			if _, isChild := d.myChildren[candidateToAbsorb.String()]; !isChild {
// 				d.logger.Errorf("Candidate %s to be absorbed is not a child", candidateToAbsorb.String())
// 				continue
// 			}

// 			if peer.PeersEqual(candidateToAbsorb, candidateToBeAbsorbed) {
// 				continue
// 			}

// 			if candidateToAbsorb.MeasuredLatency == 0 {
// 				continue
// 			}

// 			peerAbsorber = candidateToAbsorb.PeerWithIDChain
// 			peerToKick = candidateToBeAbsorbed.PeerWithIDChain
// 			d.logger.Infof("Sending absorb message with peerToAbsorb: %s, peerAbsorber: %s", peerToKick.String(), peerAbsorber.String())
// 			toSend := NewAbsorbMessage(peerToKick, peerAbsorber)
// 			d.sendMessage(toSend, peerToKick)
// 			d.resetImproveTimer()
// 			return
// 		}
// 	}
// }

// func (d *DemmonTree) handleSwitchMessage(sender peer.Peer, m message.Message) {
// 	swMsg := m.(switchMessage)
// 	d.logger.Infof("got switchMessage %+v from %s", m, sender.String())

// 	if peer.PeersEqual(swMsg.Parent, d.babel.SelfPeer()) {
// 		if d.myParent != nil && !peer.PeersEqual(sender, d.myParent) {
// 			// accepting children, but not adding new parent
// 			d.logger.Infof("got switchMessage %+v from not my parent %s (my parent: %s), accepting children either way...",
//							 m, sender.String(), d.myParent.StringWithFields())
// 			for _, sibling := range swMsg.Children {
// 				d.addChild(sibling, true, 0)
// 			}
// 			return
// 		}
// 		d.addParent(swMsg.GrandParent, nil, swMsg.Parent.Chain(), 0, true, true, true)
// 		for _, sibling := range swMsg.Children {
// 			d.addChild(sibling, true, 0)
// 		}
// 		return
// 	}

// 	if peer.PeersEqual(swMsg.GrandParent, d.babel.SelfPeer()) {
// 		d.addChild(swMsg.Parent, true, 0)
// 		d.removeChild(sender, true, true)
// 		return
// 	}

// 	// only case left is where i am a child, which is ignored if switch is sent by the wrong parent
// 	if !peer.PeersEqual(sender, d.myParent) {
// 		d.logger.Infof("got switchMessage %+v from not my parent %s (my parent: %s), returning",
//						 m, sender.String(), d.myParent.StringWithFields())
// 		return
// 	}

// 	for _, p := range swMsg.Children {
// 		if peer.PeersEqual(p, d.babel.SelfPeer()) {
// 			d.logger.Info("I am children of switch message parent...")
// 			d.addParent(swMsg.Parent, d.myGrandParent, p.Chain(), true, true, 0, false, false, true)
// 			for _, sibling := range swMsg.Children {
// 				if !peer.PeersEqual(sibling, d.babel.SelfPeer()) {
// 					d.addSibling(sibling, true)
// 				}
// 			}

// 		}
// 	}
// }
