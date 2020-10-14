package membership

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sort"
	"time"

	exporter "github.com/nm-morais/deMMon-exporter"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const protoID = 1000
const protoName = "DemonTree"

type DemmonTreeConfig = struct {

	// join protocol configs
	LandmarkRedialTimer          time.Duration
	JoinMessageTimeout           time.Duration
	MaxTimeToProgressToNextLevel time.Duration
	MaxRetriesJoinMsg            int
	ParentRefreshTickDuration    time.Duration
	ChildrenRefreshTickDuration  time.Duration
	Landmarks                    []*PeerWithIdChain
	MinGrpSize                   uint16
	MaxGrpSize                   uint16
	LimitFirstLevelGroupSize     bool
	RejoinTimerDuration          time.Duration

	NrPeersToBecomeParentInAbsorb int
	NrPeersToKickPerParent        uint16

	// maintenance configs
	PhiLevelForNodeDown float64

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

	EnableSwitch bool

	// switch
	SwitchProbability                     float32
	CheckSwitchOportunityTimeout          time.Duration
	MinLatencyImprovementPerPeerForSwitch time.Duration
}

type DemmonTreeMetrics struct {
	peerLatencyGauges     map[string]*exporter.InfluxGauge
	nrMessagesSentCounter *exporter.InfluxCounter
}

type DemmonTree struct {
	logger *logrus.Logger
	config DemmonTreeConfig

	// node state
	myPendingParentInJoin *MeasuredPeer
	self                  *PeerWithIdChain
	myGrandParent         *PeerWithIdChain
	myParent              *PeerWithIdChain
	landmark              bool
	myChildren            map[string]*PeerWithIdChain
	myChildrenLatencies   map[string]MeasuredPeersByLat
	mySiblings            map[string]*PeerWithIdChain

	// join state
	myPendingParentInRecovery *PeerWithIdChain
	lastLevelProgress         time.Time
	joinLevel                 uint16
	children                  map[string]map[string]*PeerWithIdChain
	parents                   map[string]*PeerWithIdChain
	currLevelResponseTimeuts  map[string]int
	currLevelPeers            []map[string]peer.Peer
	currLevelPeersDone        []map[string]*PeerWithIdChain
	retries                   map[string]int

	absorbTimerId  int
	switchTimerId  int
	improveTimerId int

	// improvement service state
	measuringPeers               map[string]bool
	measuredPeers                MeasuredPeersByLat
	eView                        []*PeerWithIdChain
	myPendingParentInImprovement *MeasuredPeer
	myPendingParentInAbsorb      *PeerWithIdChain

	metrics DemmonTreeMetrics
}

func New(config DemmonTreeConfig, e *exporter.Exporter) protocol.Protocol {
	return &DemmonTree{

		logger: logs.NewLogger(protoName),
		config: config,

		// join state
		joinLevel:                0,
		currLevelResponseTimeuts: make(map[string]int),
		parents:                  map[string]*PeerWithIdChain{},
		currLevelPeers:           []map[string]peer.Peer{},
		currLevelPeersDone:       []map[string]*PeerWithIdChain{},
		retries:                  map[string]int{},
		children:                 make(map[string]map[string]*PeerWithIdChain),

		// node state
		myPendingParentInRecovery: nil,
		self:                      nil,
		myGrandParent:             nil,
		myParent:                  nil,
		mySiblings:                map[string]*PeerWithIdChain{},
		myChildren:                make(map[string]*PeerWithIdChain),
		myChildrenLatencies:       make(map[string]MeasuredPeersByLat),

		// improvement state
		eView:                        []*PeerWithIdChain{},
		measuringPeers:               make(map[string]bool),
		measuredPeers:                MeasuredPeersByLat{},
		myPendingParentInImprovement: nil,

		metrics: DemmonTreeMetrics{
			peerLatencyGauges:     map[string]*exporter.InfluxGauge{},
			nrMessagesSentCounter: e.NewCounter("messages_sent"),
		},
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
	d.self = NewPeerWithIdChain(nil, pkg.SelfPeer(), 0, 0, make(Coordinates, len(d.config.Landmarks)))
	for _, landmark := range d.config.Landmarks {
		if peer.PeersEqual(pkg.SelfPeer(), landmark) {
			d.self = NewPeerWithIdChain(landmark.PeerIDChain, landmark.Peer, 0, 0, make(Coordinates, len(d.config.Landmarks)))
			d.landmark = true
			for _, landmark := range d.config.Landmarks {
				if !peer.PeersEqual(pkg.SelfPeer(), landmark) {
					d.mySiblings[landmark.String()] = landmark
					pkg.Dial(d.ID(), landmark, landmark.ToTCPAddr())
					pkg.GetNodeWatcher().Watch(landmark, d.ID())
					c := pkg.Condition{
						Repeatable:                false,
						CondFunc:                  func(*pkg.NodeInfo) bool { return true },
						EvalConditionTickDuration: 1000 * time.Millisecond,
						Notification:              landmarkMeasuredNotification{landmarkMeasured: landmark},
						Peer:                      landmark,
						EnableGracePeriod:         false,
						ProtoId:                   d.ID(),
					}
					pkg.GetNodeWatcher().NotifyOnCondition(c)
				}
			}

			d.logger.Infof("I am landmark, my ID is: %+v", d.self.Chain())
			d.myParent = nil
			d.myChildren = make(map[string]*PeerWithIdChain)
			if d.config.LimitFirstLevelGroupSize {
				d.absorbTimerId = pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
			}
			pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
			pkg.RegisterTimer(d.ID(), NewDebugTimer(5*time.Second))
			return
		}
	}
	pkg.RegisterTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
	pkg.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
	pkg.RegisterTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration))
	pkg.RegisterTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout))
	d.improveTimerId = pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
	d.absorbTimerId = pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
	d.switchTimerId = pkg.RegisterTimer(d.ID(), NewSwitchTimer(d.config.CheckSwitchOportunityTimeout))
	pkg.RegisterTimer(d.ID(), NewDebugTimer(5*time.Second))
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
	pkg.RegisterMessageHandler(d.ID(), switchMessage{}, d.handleSwitchMessage)

	// pkg.RegisterMessageHandler(d.ID(), joinAsParentMessage{}, d.handleJoinAsParentMessage)
	// pkg.RegisterMessageHandler(d.ID(), biasedWalkMessage{}, d.handleBiasedWalkMessage)

	pkg.RegisterMessageHandler(d.ID(), randomWalkMessage{}, d.handleRandomWalkMessage)
	pkg.RegisterMessageHandler(d.ID(), walkReplyMessage{}, d.handleWalkReplyMessage)

	pkg.RegisterNotificationHandler(d.ID(), peerMeasuredNotification{}, d.handlePeerMeasuredNotification)
	pkg.RegisterNotificationHandler(d.ID(), landmarkMeasuredNotification{}, d.handleLandmarkMeasuredNotification)
	pkg.RegisterNotificationHandler(d.ID(), peerDownNotification{}, d.handlePeerDownNotification)

	pkg.RegisterTimerHandler(d.ID(), joinTimerID, d.handleJoinTimer)
	pkg.RegisterTimerHandler(d.ID(), landmarkRedialTimerID, d.handleLandmarkRedialTimer)
	pkg.RegisterTimerHandler(d.ID(), parentRefreshTimerID, d.handleRefreshParentTimer)
	pkg.RegisterTimerHandler(d.ID(), updateChildTimerID, d.handleUpdateChildTimer)
	pkg.RegisterTimerHandler(d.ID(), checkChidrenSizeTimerID, d.handleCheckChildrenSizeTimer)
	pkg.RegisterTimerHandler(d.ID(), externalNeighboringTimerID, d.handleExternalNeighboringTimer)
	pkg.RegisterTimerHandler(d.ID(), measureNewPeersTimerID, d.handleMeasureNewPeersTimer)
	pkg.RegisterTimerHandler(d.ID(), evalMeasuredPeersTimerID, d.handleEvalMeasuredPeersTimer)
	pkg.RegisterTimerHandler(d.ID(), peerJoinMessageResponseTimeoutID, d.handleJoinMessageResponseTimeout)
	pkg.RegisterTimerHandler(d.ID(), switchTimerID, d.handleSwitchTimer)
	pkg.RegisterTimerHandler(d.ID(), debugTimerID, d.handleDebugTimer)

}

// notification handlers

func (d *DemmonTree) handlePeerMeasuredNotification(notification notification.Notification) {
	peerMeasuredNotification := notification.(peerMeasuredNotification)
	delete(d.measuringPeers, peerMeasuredNotification.peerMeasured.String())
	if !d.isNeighbour(peerMeasuredNotification.peerMeasured.Peer) {
		currNodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(peerMeasuredNotification.peerMeasured)
		if err != nil {
			d.logger.Error(err.Reason())
			return
		} else {
			d.logger.Infof("New peer measured: %s, latency: %s", peerMeasuredNotification.peerMeasured, currNodeStats.LatencyCalc.CurrValue())
			d.measuredPeers = append(d.measuredPeers, NewMeasuredPeer(peerMeasuredNotification.peerMeasured, currNodeStats.LatencyCalc.CurrValue()))
			sort.Sort(d.measuredPeers)
			d.measuredPeers = TrimMeasuredPeersToSize(d.measuredPeers, d.config.MeasuredPeersSize)
		}
		pkg.GetNodeWatcher().Unwatch(peerMeasuredNotification.peerMeasured.Peer, d.ID())
	}
	toPrint := ""
	for _, measuredPeer := range d.measuredPeers {
		toPrint = toPrint + "; " + fmt.Sprintf("%s : %s", measuredPeer.String(), measuredPeer.MeasuredLatency)
	}
	d.logger.Infof("d.measuredPeers:  %s:", toPrint)
}

func (d *DemmonTree) handleLandmarkMeasuredNotification(notification notification.Notification) {
	n := notification.(landmarkMeasuredNotification)
	for idx, l := range d.config.Landmarks {
		if peer.PeersEqual(l, n.landmarkMeasured) {
			landmarkStats, err := pkg.GetNodeWatcher().GetNodeInfo(l)
			if err != nil {
				panic("landmark was measured but has no measurement...")
			}
			d.self.Coordinates[idx] = uint64(landmarkStats.LatencyCalc.CurrValue().Milliseconds())
			d.self.SetVersion(d.self.Version() + 1)
			d.logger.Infof("My Coordinates: %+v", d.self.Coordinates)
			return
		}
	}
	panic("handleLandmarkMeasuredNotification for non-landmark peer")
}

// timer handlers

func (d *DemmonTree) handleDebugTimer(joinTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewDebugTimer(5*time.Second))
	inView := []*PeerWithIdChain{}
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
	for _, neighbour := range inView {
		toPrint = toPrint + " " + neighbour.String()
	}

	d.logger.Infof(" %s InView: %s", d.self.String(), toPrint)

}

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {
	// if d.joinLevel == 0 && len(d.currLevelPeers[d.joinLevel]) == 0 {
	// 	d.logger.Info("-------------Rejoining overlay---------------")
	// 	d.joinOverlay()
	// }
	pkg.RegisterTimer(d.ID(), NewJoinTimer(d.config.RejoinTimerDuration))
}

func (d *DemmonTree) handleLandmarkRedialTimer(t timer.Timer) {
	redialTimer := t.(*landmarkRedialTimer)
	pkg.GetNodeWatcher().Watch(redialTimer.LandmarkToRedial, d.ID())
	c := pkg.Condition{
		Repeatable:                false,
		CondFunc:                  func(*pkg.NodeInfo) bool { return true },
		EvalConditionTickDuration: 1000 * time.Millisecond,
		Notification:              landmarkMeasuredNotification{landmarkMeasured: redialTimer.LandmarkToRedial},
		Peer:                      redialTimer.LandmarkToRedial,
		EnableGracePeriod:         false,
		ProtoId:                   d.ID(),
	}
	pkg.GetNodeWatcher().NotifyOnCondition(c)
	pkg.Dial(d.ID(), redialTimer.LandmarkToRedial, redialTimer.LandmarkToRedial.ToTCPAddr())
}

func (d *DemmonTree) handleRefreshParentTimer(timer timer.Timer) {
	// d.logger.Info("RefreshParentTimer trigger")
	for _, child := range d.myChildren {
		toSend := NewUpdateParentMessage(d.myParent, d.self, child.Chain()[child.Level()], d.getChildrenAsPeerWithIdChainArray(child))
		d.sendMessage(toSend, child.Peer)
	}
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(d.config.ParentRefreshTickDuration))
}

func (d *DemmonTree) handleUpdateChildTimer(timer timer.Timer) {
	// d.logger.Info("UpdateChildTimer trigger")
	if d.myParent != nil {
		d.sendUpdateChildMessage(d.myParent)
	}
	pkg.RegisterTimer(d.ID(), NewUpdateChildTimer(d.config.ChildrenRefreshTickDuration))
}

func (d *DemmonTree) sendUpdateChildMessage(dest peer.Peer) {
	if len(d.self.Chain()) > 0 {
		measuredSiblings := make(MeasuredPeersByLat, 0, len(d.mySiblings))
		for _, sibling := range d.mySiblings {
			nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(sibling.Peer)
			var currLat time.Duration
			if err != nil {
				d.logger.Warnf("Do not have latency measurement for %s", sibling.String())
				currLat = math.MaxInt64
			} else {
				currLat = nodeStats.LatencyCalc.CurrValue()
			}
			measuredSiblings = append(measuredSiblings, NewMeasuredPeer(sibling, currLat))
		}
		toSend := NewUpdateChildMessage(d.self, measuredSiblings)
		d.sendMessage(toSend, dest)
	}
}

func (d *DemmonTree) handleExternalNeighboringTimer(joinTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewExternalNeighboringTimer(d.config.EmitWalkTimeout))

	d.logger.Info("ExternalNeighboringTimer trigger")

	if d.myParent == nil || len(d.self.Chain()) == 0 || d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil {
		d.logger.Infof("Not progressing because d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil")
		return
	}

	r := rand.Float32()
	if r > d.config.EmitWalkProbability {
		return
	}

	neighbours := d.getNeighborsAsPeerWithIdChainArray()
	// d.logger.Infof("d.possibilitiesToSend: %+v:", neighbours)

	sample := getRandSample(d.config.NrPeersInWalkMessage-1, append(neighbours, d.eView...)...)
	sample = append(sample, d.self)

	// r = rand.Float32()

	var msgToSend message.Message
	var peerToSendTo *PeerWithIdChain
	// if r < d.config.BiasedWalkProbability {
	// 	msgToSend = NewBiasedWalkMessage(uint16(d.config.RandomWalkTTL), selfPeerWithChain, sample)
	// 	peerToSendTo = getBiasedPeerExcluding(possibilitiesToSend, selfPeerWithChain)
	// } else {
	msgToSend = NewRandomWalkMessage(uint16(d.config.RandomWalkTTL), d.self, sample)
	peerToSendTo = getRandomExcluding(getExcludingDescendantsOf(neighbours, d.self.Chain()), d.self)
	// }

	if peerToSendTo == nil {
		d.logger.Error("peerToSendTo is nil")
		return
	}
	d.logger.Infof("sending random walk to %s", peerToSendTo.StringWithFields())
	d.sendMessage(msgToSend, peerToSendTo.Peer)
}

func (d *DemmonTree) handleEvalMeasuredPeersTimer(evalMeasuredPeersTimer timer.Timer) {

	if d.improveTimerId == -1 {
		d.improveTimerId = pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
		return
	}

	d.improveTimerId = pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
	d.logger.Info("EvalMeasuredPeersTimer trigger...")

	if len(d.self.Chain()) == 0 || d.myParent == nil || d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil {
		d.logger.Info("EvalMeasuredPeersTimer returning because len(d.self.Chain()) == 0 || d.myParent == nil || d.myPendingParentInImprovement != nil...")
		return
	}

	r := rand.Float32()
	if r > d.config.AttemptImprovePositionProbability {
		d.logger.Info("returning due to  r > d.config.AttemptImprovePositionProbability")
		return
	}

	keys := make(map[string]bool) // remove dups
	i := 0
	for _, measuredPeer := range d.measuredPeers {
		_, value := keys[measuredPeer.String()]
		if !measuredPeer.IsDescendentOf(d.self.Chain()) && !value {
			d.measuredPeers[i] = measuredPeer
			keys[measuredPeer.String()] = true
			i++
		}
	}
	for j := i; j < len(d.measuredPeers); j++ {
		d.measuredPeers[j] = nil
	}
	d.measuredPeers = d.measuredPeers[:i]

	d.logger.Info("Evaluating measuredPeers...")
	toPrint := ""
	for _, measuredPeer := range d.measuredPeers {
		toPrint = toPrint + "; " + fmt.Sprintf("%s : %s", measuredPeer.String(), measuredPeer.MeasuredLatency)
	}
	d.logger.Infof("d.measuredPeers:  %s:", toPrint)

	if len(d.measuredPeers) == 0 {
		d.logger.Warn("returning due to len(d.measuredPeers) == 0")
		return
	}
	parentStats, err := pkg.GetNodeWatcher().GetNodeInfo(d.myParent)
	if err != nil {
		d.logger.Error("returning due to  not having latency measurement for parent")
		d.logger.Error(err.Reason())
		return
	}

	parentLatency := parentStats.LatencyCalc.CurrValue()
	sort.Sort(d.measuredPeers)
	for _, measuredPeer := range d.measuredPeers {
		// parent latency is higher than latency to peer
		latencyImprovement := parentLatency - measuredPeer.MeasuredLatency
		if latencyImprovement < 0 {
			d.logger.Infof("returning due to latencyImprovement < 0")
			return
		}

		if latencyImprovement > d.config.MinLatencyImprovementToImprovePosition {
			if measuredPeer.nChildren >= d.config.MaxGrpSize {
				continue
			}
			if measuredPeer.Level() > d.self.Level()-1 {
				if len(d.myChildren) > 0 {
					continue // TODO send message
				}
			}

			d.logger.Infof("Improving position towards: %s", measuredPeer.String())
			d.logger.Infof("latencyImprovement: %s", latencyImprovement)
			d.logger.Infof("parentLatency: %s", parentLatency)
			d.logger.Infof("measuredPeer.MeasuredLatency: %s", measuredPeer.MeasuredLatency)
			aux := *measuredPeer
			d.myPendingParentInImprovement = &aux
			d.sendJoinAsChildMsg(measuredPeer.PeerWithIdChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
			return
		}

		// if measuredPeer.Level() < d.self.Level()-1 {
		// 	// if measuredPeer.NrChildren() < d.config.MaxGrpSize { // must go UP in levels
		// 	aux := *measuredPeer
		// 	d.myPendingParentInImprovement = &aux
		// 	d.sendJoinAsChildMsg(measuredPeer.PeerWithIdChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
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
	}
}

func (d *DemmonTree) handleMeasureNewPeersTimer(measureNewPeersTimer timer.Timer) {
	pkg.RegisterTimer(d.ID(), NewMeasureNewPeersTimer(d.config.MeasureNewPeersRefreshTickDuration))
	if len(d.eView) == 0 {
		d.logger.Infof("returning because len(eView) == 0")
		return
	}

	toPrint := ""
	i := 0
	for _, peer := range d.eView {
		if peer == nil {
			continue
		}
		d.eView[i] = peer
		toPrint = toPrint + ";" + peer.StringWithFields()
		i++
	}
	for k := i; k < len(d.eView); k++ {
		d.eView[k] = nil
	}
	d.eView = d.eView[:i]
	d.logger.Infof("d.eView: %s", toPrint)

	nrMeasured := 0
	sort.Slice(d.eView, func(i, j int) bool {
		return EuclideanDist(d.eView[i].Coordinates, d.self.Coordinates) < EuclideanDist(d.eView[j].Coordinates, d.self.Coordinates)
	})

	toPrint = ""
	for _, peer := range d.eView {
		toPrint = toPrint + ";" + fmt.Sprintf("%s:%+v:%f", peer.String(), peer.Coordinates, EuclideanDist(peer.Coordinates, d.self.Coordinates))
	}
	d.logger.Infof("d.eView sorted: %s", toPrint)

	for i := 0; i < len(d.eView) && nrMeasured < d.config.NrPeersToMeasure; i++ {
		p := d.eView[i]
		if _, isMeasuring := d.measuringPeers[p.String()]; isMeasuring {
			continue
		}

		alreadyMeasured := false
		for _, curr := range d.measuredPeers {
			if peer.PeersEqual(curr, p) {
				alreadyMeasured = true
				break
			}
		}

		if alreadyMeasured {
			continue
		}

		d.logger.Infof("measuring peer: %s", p.String())
		pkg.GetNodeWatcher().Watch(p, d.ID())
		c := pkg.Condition{
			Repeatable:                false,
			CondFunc:                  func(*pkg.NodeInfo) bool { return true },
			EvalConditionTickDuration: 1000 * time.Millisecond,
			Notification:              peerMeasuredNotification{peerMeasured: p},
			Peer:                      p,
			EnableGracePeriod:         false,
			ProtoId:                   d.ID(),
		}
		// d.logger.Infof("Doing NotifyOnCondition for node %s...", p.String())
		pkg.GetNodeWatcher().NotifyOnCondition(c)
		d.measuringPeers[p.String()] = true
		nrMeasured++
	}
}

// VERSION where nodes with highest latency node gets kicked towards its lowest latency peer
func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
	d.absorbTimerId = pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))

	// d.logger.Info("handleCheckChildrenSize timer trigger")

	if d.self.NrChildren() == 0 || d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil {
		// d.logger.Info("d.self.NrChildren() == 0, returning...")
		return
	}

	if uint16(len(d.myChildren)) <= d.config.MaxGrpSize {
		// d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
		return
	}

	childrenAsMeasuredPeers := make(MeasuredPeersByLat, 0, d.self.NrChildren())
	for _, children := range d.myChildren {
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(children)
		if err != nil {
			d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", children.String())
			return
		}
		currLat := nodeStats.LatencyCalc.CurrValue()
		childrenAsMeasuredPeers = append(childrenAsMeasuredPeers, NewMeasuredPeer(children, currLat))
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

			lowestLatPairLat := time.Duration(math.MaxInt64)
			var bestCandidateToKick *PeerWithIdChain
			var bestCandidateToAbsorb *PeerWithIdChain

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
						bestCandidateToKick = candidateToKick.PeerWithIdChain
						lowestLatPairLat = candidateToKick.MeasuredLatency
						bestCandidateToAbsorb = peerAbsorber.PeerWithIdChain
					}
				}
			}

			if bestCandidateToKick == nil {
				return
			}

			peersAlreadyKicked[bestCandidateToKick.String()] = true
			peersAbsorbedPerAbsorber[bestCandidateToAbsorb.String()]++

			d.logger.Infof("Sending absorb message with peerToAbsorb: %s, peerAbsorber: %s", bestCandidateToKick.StringWithFields(), bestCandidateToAbsorb.StringWithFields())
			toSend := NewAbsorbMessage(bestCandidateToKick, bestCandidateToAbsorb)
			d.sendMessage(toSend, bestCandidateToKick)
			d.resetImproveTimer()
		}
	}
}

// message handlers

func (d *DemmonTree) handleRandomWalkMessage(sender peer.Peer, m message.Message) {
	randWalkMsg := m.(randomWalkMessage)
	d.logger.Infof("Got randomWalkMessage: %+v from %s", randWalkMsg, sender.String())
	// toPrint := ""
	// for _, peer := range randWalkMsg.Sample {
	// 	toPrint = toPrint + "; " + peer.String()
	// }
	// d.logger.Infof("randomWalkMessage peers: %s", toPrint)

	hopsTaken := d.config.RandomWalkTTL - int(randWalkMsg.TTL)
	if hopsTaken < d.config.NrHopsToIgnoreWalk {
		randWalkMsg.TTL--
		neighbours := d.getNeighborsAsPeerWithIdChainArray()
		neighboursWithoutSenderDescendants := getExcludingDescendantsOf(neighbours, randWalkMsg.Sender.PeerIDChain)
		p := getRandomExcluding(neighboursWithoutSenderDescendants, randWalkMsg.Sender, d.self, sender)
		if p == nil {
			sampleToSend, _ := d.mergeEViewWith(randWalkMsg.Sample, randWalkMsg.Sender, d.config.NrPeersToMergeInWalkSample)
			walkReply := NewWalkReplyMessage(sampleToSend)
			d.logger.Infof("have no peers to forward message... merging and sending random walk reply to %s", randWalkMsg.Sender.String())
			d.sendMessageTmpTCPChan(walkReply, randWalkMsg.Sender)
			return
		}
		d.logger.Infof("hopsTaken < d.config.NrHopsToIgnoreWalk, simply forwarding random walk message %+v to: %s", randWalkMsg, p.String())
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
		sampleToSend, neighboursWithoutSenderDescendants := d.mergeEViewWith(randWalkMsg.Sample, randWalkMsg.Sender, d.config.NrPeersToMergeInWalkSample)
		randWalkMsg.TTL--
		p := getRandomExcluding(neighboursWithoutSenderDescendants, randWalkMsg.Sender, d.self, sender)
		if p == nil {
			d.logger.Infof("have no peers to forward message... merging and sending random walk reply to %s", randWalkMsg.Sender.String())
			walkReply := NewWalkReplyMessage(sampleToSend)
			d.sendMessageTmpTCPChan(walkReply, randWalkMsg.Sender)
			return
		}
		d.logger.Infof("hopsTaken >= d.config.NrHopsToIgnoreWalk, merging and forwarding random walk message %+v to: %s", randWalkMsg, p.String())
		randWalkMsg.Sample = sampleToSend
		d.sendMessage(randWalkMsg, p)
		return
	}

	// TTL == 0
	if randWalkMsg.TTL == 0 {
		sampleToSend, _ := d.mergeEViewWith(randWalkMsg.Sample, randWalkMsg.Sender, d.config.NrPeersToMergeInWalkSample)
		d.logger.Infof("random walk TTL is 0. Sending random walk reply to %s", randWalkMsg.Sender.String())
		d.sendMessageTmpTCPChan(NewWalkReplyMessage(sampleToSend), randWalkMsg.Sender)
		return
	}
}

func (d *DemmonTree) handleWalkReplyMessage(sender peer.Peer, m message.Message) {
	walkReply := m.(walkReplyMessage)
	d.logger.Infof("Got walkReplyMessage: %+v from %s", walkReply, sender.String())
	sample := walkReply.Sample
	if len(d.self.Chain()) > 0 {
	outer:
		for i := 0; i < len(sample); i++ {
			currPeer := sample[i]

			if peer.PeersEqual(currPeer, pkg.SelfPeer()) {
				continue outer
			}

			if d.isNeighbour(currPeer) {
				continue
			}

			if d.updateAndComparePeers(d.myParent, currPeer) {
				continue outer
			}

			if sibling, ok := d.mySiblings[currPeer.String()]; ok {
				d.updateAndComparePeers(sibling, currPeer)
				continue outer
			}

			if children, isChildren := d.myChildren[currPeer.String()]; isChildren {
				d.updateAndComparePeers(children, currPeer)
				continue outer
			}

			for idx, eViewPeer := range d.eView {
				if d.updateAndComparePeers(eViewPeer, currPeer) {
					if eViewPeer.IsDescendentOf(d.self.Chain()) {
						d.removeFromMeasuredPeers(eViewPeer)
						d.eView[len(d.eView)-1] = nil
						copy(d.eView[idx:], d.eView[idx+1:])
						d.eView = d.eView[:len(d.eView)-1]
					}
					continue outer
				}
			}

			if currPeer.IsDescendentOf(d.self.Chain()) {
				continue outer
			}

			if len(d.eView) >= d.config.MaxPeersInEView { // eView is full
				toRemoveIdx := rand.Intn(len(d.eView))
				d.eView[toRemoveIdx] = currPeer
			} else {
				d.eView = append(d.eView, currPeer)
			}
		}
	}

}

func (d *DemmonTree) handleAbsorbMessage(sender peer.Peer, m message.Message) {
	absorbMessage := m.(absorbMessage)

	d.logger.Infof("Got absorbMessage: %+v from %s", m, sender.String())

	if !peer.PeersEqual(d.myParent, sender) {
		d.logger.Warnf("Got absorbMessage: %+v from not my parent %s (my parent: %s)", m, sender.String(), d.myParent.String())
		return
	}

	if d.myParent == nil || d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil {
		d.logger.Errorf("Got absorbMessage but d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil...")
		return
	}

	if sibling, ok := d.mySiblings[absorbMessage.PeerAbsorber.String()]; ok {
		d.removeSibling(sibling, false, false)
		var peerLat time.Duration
		if nodeInfo, err := pkg.GetNodeWatcher().GetNodeInfo(sibling); err == nil {
			peerLat = nodeInfo.LatencyCalc.CurrValue()
		}
		d.myPendingParentInAbsorb = absorbMessage.PeerAbsorber
		toSend := NewJoinAsChildMessage(d.self, peerLat, absorbMessage.PeerAbsorber.PeerIDChain, false)
		d.sendMessage(toSend, absorbMessage.PeerAbsorber)
	} else {
		d.myPendingParentInAbsorb = absorbMessage.PeerAbsorber
		toSend := NewJoinAsChildMessage(d.self, 0, absorbMessage.PeerAbsorber.PeerIDChain, false)
		d.sendMessageTmpTCPChan(toSend, absorbMessage.PeerAbsorber)
	}
}

func (d *DemmonTree) handleDisconnectAsChildMsg(sender peer.Peer, m message.Message) {
	dacMsg := m.(disconnectAsChildMessage)
	d.logger.Infof("got DisconnectAsChildMsg %+v from %s", dacMsg, sender.String())
	d.removeChild(sender, true, true)
}

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {
	toSend := NewJoinReplyMessage(d.getChildrenAsPeerWithIdChainArray(), d.self)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinMessageResponseTimeout(timer timer.Timer) {
	peer := timer.(*peerJoinMessageResponseTimeout).Peer
	d.logger.Warnf("Join message timeout from %s", peer.String())

	delete(d.currLevelPeers[d.joinLevel], peer.String())
	delete(d.currLevelPeersDone[d.joinLevel], peer.String())

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

	d.logger.Infof("Got joinReply: %+v from %s", replyMsg, sender.String())

	if d.joinLevel == math.MaxUint16 {
		d.logger.Errorf("Got joinReply: %+v but already joined... %s", replyMsg, sender.String())
		return
	}

	if _, ok := d.currLevelPeers[d.joinLevel][sender.String()]; ok {
		pkg.CancelTimer(d.currLevelResponseTimeuts[sender.String()])
	} else {
		d.logger.Errorf("Got joinReply: %+v from timed out peer... %s", replyMsg, sender.String())
		return
	}

	if (d.joinLevel != replyMsg.Sender.Level()) || (d.joinLevel > 1 && !d.parents[sender.String()].IsParentOf(replyMsg.Sender)) {

		if d.joinLevel != replyMsg.Sender.Level() {
			d.logger.Warnf("Discarding joinReply %+v from %s because joinLevel is not mine: %d", replyMsg, sender.String(), d.joinLevel)
		}

		if d.joinLevel > 1 && !d.parents[sender.String()].IsParentOf(replyMsg.Sender) {
			d.logger.Warnf("Discarding joinReply %+v from %s because node does not have the same parent... should be: %s", replyMsg, sender.String(), d.parents[sender.String()].String())
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

	d.currLevelPeersDone[d.joinLevel][sender.String()] = replyMsg.Sender
	d.children[sender.String()] = make(map[string]*PeerWithIdChain, len(replyMsg.Children))
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
	jacMsg := m.(joinAsChildMessage)
	d.logger.Infof("got JoinAsChildMessage %+v from %s", jacMsg, sender.String())

	if len(d.self.Chain()) == 0 || d.self.IsDescendentOf(jacMsg.Sender.PeerIDChain) || d.myPendingParentInImprovement != nil || d.myPendingParentInRecovery != nil {
		toSend := NewJoinAsChildMessageReply(false, PeerID{}, 0, nil, nil, nil)
		if len(d.self.Chain()) == 0 {
			d.logger.Info("denying joinAsChildReply from because my chain is nil")
		}

		if d.myPendingParentInImprovement != nil {
			d.logger.Info("denying joinAsChildReply from because d.myPendingParentInImprovement != nil ")
		}

		if d.self.IsDescendentOf(jacMsg.Sender.PeerIDChain) {
			d.logger.Infof("denying joinAsChildReply from because im descendent of sender, sender chain: %+v", jacMsg.Sender.Chain())
		}

		d.sendMessageTmpTCPChan(toSend, sender)
		return
	}

	if !d.landmark {
		if !jacMsg.Urgent {
			if !jacMsg.ExpectedId.Equal(d.self.Chain()) {
				d.logger.Info("denying joinAsChildReply because expected id does not match my id")
				toSend := NewJoinAsChildMessageReply(false, PeerID{}, 0, nil, nil, nil)
				d.sendMessageTmpTCPChan(toSend, sender)
				return
			}
		}
	}

	newChildId := d.addChild(jacMsg.Sender, true, true, jacMsg.MeasuredLatency)
	childrenToSend := make([]*PeerWithIdChain, 0, d.self.NrChildren())
	for _, child := range d.myChildren {
		childrenToSend = append(childrenToSend, child)
	}
	toSend := NewJoinAsChildMessageReply(true, newChildId, d.self.Level(), d.self, childrenToSend, d.myGrandParent)
	d.sendMessageTmpTCPChan(toSend, sender)
}

func (d *DemmonTree) handleJoinAsChildMessageReply(sender peer.Peer, m message.Message) {
	japrMsg := m.(joinAsChildMessageReply)
	d.logger.Infof("got JoinAsChildMessageReply %+v from %s", japrMsg, sender.String())

	if !japrMsg.Accepted {
		// special case for parent in recovery
		if d.myPendingParentInRecovery != nil && peer.PeersEqual(sender, d.myPendingParentInRecovery) {
			d.removeFromMeasuredPeers(d.myPendingParentInRecovery)
			d.fallbackToMeasuredPeers()
			return
		}

		if d.myPendingParentInImprovement != nil && peer.PeersEqual(sender, d.myPendingParentInImprovement) {
			d.logger.Warnf("Pending parent in recovery denied request")
			d.myPendingParentInImprovement = nil
			return
		}

		if d.myPendingParentInAbsorb != nil && peer.PeersEqual(sender, d.myPendingParentInAbsorb) {
			if d.myGrandParent != nil {
				d.logger.Warnf("Pending parent in absorb denied request.. falling back to grandparent")
				d.myPendingParentInRecovery = d.myGrandParent
				d.sendJoinAsChildMsg(d.myGrandParent, 0, d.self.NrChildren(), true)
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

	myNewId := append(japrMsg.Parent.PeerIDChain, japrMsg.ProposedId)
	if d.myPendingParentInImprovement != nil && peer.PeersEqual(sender, d.myPendingParentInImprovement) {
		d.addToMeasuredPeers(d.myParent)
		d.addParent(japrMsg.Parent, japrMsg.GrandParent, myNewId, true, true, d.myPendingParentInImprovement.MeasuredLatency, true, true, true)
		d.myPendingParentInImprovement = nil
		return
	}

	if d.myPendingParentInAbsorb != nil && peer.PeersEqual(sender, d.myPendingParentInAbsorb) {
		d.addToMeasuredPeers(d.myParent)
		d.addParent(japrMsg.Parent, japrMsg.GrandParent, myNewId, true, true, 0, true, true, true)
		d.myPendingParentInAbsorb = nil
		return
	}

	if d.myPendingParentInRecovery != nil && peer.PeersEqual(sender, d.myPendingParentInRecovery) {
		d.addParent(japrMsg.Parent, japrMsg.GrandParent, myNewId, true, true, 0, false, false, true)
		d.myPendingParentInRecovery = nil
		return
	}

	if d.myPendingParentInJoin != nil && peer.PeersEqual(sender, d.myPendingParentInJoin) {
		d.addParent(japrMsg.Parent, japrMsg.GrandParent, myNewId, true, true, d.myPendingParentInJoin.MeasuredLatency, false, false, true)
		d.myPendingParentInJoin = nil
		return
	}

	d.logger.Panicf("got join as child reply but cause for it is not known")

}

func (d *DemmonTree) handleUpdateParentMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(updateParentMessage)
	d.logger.Infof("got UpdateParentMessage %+v from %s", upMsg, sender.String())
	if d.myParent == nil || !peer.PeersEqual(sender, d.myParent) {
		if d.myParent != nil {
			d.logger.Errorf("Received UpdateParentMessage from not my parent (parent:%s sender:%s)", d.myParent.StringWithFields(), upMsg.Parent.StringWithFields())
			return
		} else {
			d.logger.Errorf("Received UpdateParentMessage from not my parent (parent:%+v sender:%s)", d.myParent, upMsg.Parent.StringWithFields())
			return
		}
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
				d.logger.Warnf("My grandparent changed : (%s -> %s)", d.myGrandParent.StringWithFields(), upMsg.GrandParent.StringWithFields())
				d.myGrandParent = upMsg.GrandParent
			}
		}
	}

	d.myGrandParent = upMsg.GrandParent
	d.myParent = upMsg.Parent
	myNewChain := append(upMsg.Parent.Chain(), upMsg.ProposedId)
	if !myNewChain.Equal(d.self.Chain()) {
		d.logger.Warnf("My chain changed: (%+v -> %+v)", d.self.Chain(), myNewChain)
		d.logger.Warnf("My level changed: (%d -> %d)", d.self.Level(), upMsg.Parent.Level()+1) // IMPORTANT FOR VISUALIZER
		d.self.SetChain(myNewChain)
		d.self.SetVersion(d.self.Version() + 1)
		for _, child := range d.myChildren {
			childId := child.Chain()[len(child.Chain())-1]
			child.SetChain(append(myNewChain, childId))
		}
	}
	d.logger.Warnf("My level changed: (%d -> %d)", d.self.Level(), upMsg.Parent.Level()+1) // IMPORTANT FOR VISUALIZER
	d.mergeSiblingsWith(upMsg.Siblings)
}

func (d *DemmonTree) handleUpdateChildMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(updateChildMessage)
	d.logger.Infof("got updateChildMessage %+v from %s", m, sender.String())
	child, ok := d.myChildren[sender.String()]
	if !ok {
		d.logger.Errorf("got updateChildMessage %+v from not my children, or my pending children: %s", m, sender.String())
		return
	}
	// toPrint := ""
	// for _, measuredPeer := range upMsg.Siblings {
	// 	toPrint = toPrint + "; " + measuredPeer.StringWithFields()
	// }
	// d.logger.Infof("SiblingLatencies in updateChildMessage: %s:", toPrint)

	child.SetChildrenNr(upMsg.Child.nChildren)
	child.SetCoords(upMsg.Child.Coordinates)
	child.SetVersion(upMsg.Child.version)

	d.myChildrenLatencies[child.String()] = upMsg.Siblings
}

func (d *DemmonTree) handleSwitchMessage(sender peer.Peer, m message.Message) {
	swMsg := m.(switchMessage)
	d.logger.Infof("got switchMessage %+v from %s", m, sender.String())

	if peer.PeersEqual(swMsg.Parent, pkg.SelfPeer()) {

		if d.myParent != nil && !peer.PeersEqual(sender, d.myParent) {
			// accepting children, but not adding new parent
			d.logger.Infof("got switchMessage %+v from not my parent %s (my parent: %s), accepting children either way...", m, sender.String(), d.myParent.StringWithFields())
			for _, sibling := range swMsg.Children {
				d.addChild(sibling, true, true, 0)
			}
			return
		}

		d.addParent(swMsg.GrandParent, nil, swMsg.Parent.Chain(), true, true, 0, false, false, true)
		for _, sibling := range swMsg.Children {
			d.addChild(sibling, true, true, 0)
		}
		return
	}

	if peer.PeersEqual(swMsg.GrandParent, pkg.SelfPeer()) {
		d.addChild(swMsg.Parent, true, true, 0)
		d.removeChild(sender, true, true)
		return
	}

	// only case left is where i am a child, which is ignored if switch is sent by the wrong parent
	if !peer.PeersEqual(sender, d.myParent) {
		d.logger.Infof("got switchMessage %+v from not my parent %s (my parent: %s), returning", m, sender.String(), d.myParent.StringWithFields())
		return
	}

	for _, p := range swMsg.Children {
		if peer.PeersEqual(p, pkg.SelfPeer()) {
			d.logger.Info("I am children of switch message parent...")
			d.addParent(swMsg.Parent, d.myGrandParent, p.Chain(), true, true, 0, false, false, true)
			for _, sibling := range swMsg.Children {
				if !peer.PeersEqual(sibling, pkg.SelfPeer()) {
					d.addSibling(sibling, true, true)
				}
			}

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

	d.logger.Warnf("Conn requested by unkown peer: %s", p.String())
	return true
}

func (d *DemmonTree) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {

	if sourceProto != d.ID() {
		d.logger.Infof("Not accepting dial from other proto: %d", sourceProto)
		return false
	}

	d.logger.Infof("Dialed peer with success: %s", p.String())
	if d.myParent != nil && peer.PeersEqual(d.myParent, p) {
		d.logger.Infof("Dialed parent with success, parent: %s", d.myParent.StringWithFields())
		d.sendUpdateChildMessage(d.myParent)
		return true
	}

	child, isChildren := d.myChildren[p.String()]
	if isChildren {
		d.logger.Infof("Dialed children with success: %s", child.StringWithFields())
		toSend := NewUpdateParentMessage(d.myParent, d.self, child.Chain()[len(child.Chain())-1], d.getChildrenAsPeerWithIdChainArray(child))
		d.sendMessage(toSend, child)
		return true
	}

	sibling, isSibling := d.mySiblings[p.String()]
	if isSibling {
		d.logger.Infof("Dialed sibling with success: %s", sibling.StringWithFields())
		return true
	}

	d.logger.Infof("d.myParent: %s", d.myParent)
	d.logger.Infof("d.myChildren: %+v", d.myChildren)
	d.logger.Errorf("Dialed unknown peer: %s", p.String())
	return false
}

func (d *DemmonTree) DialFailed(p peer.Peer) {
	d.logger.Errorf("Failed to dial %s", p.String())

	if d.landmark {
		for _, landmark := range d.config.Landmarks {
			if peer.PeersEqual(landmark, p) {
				pkg.RegisterTimer(d.ID(), NewLandmarkRedialTimer(d.config.LandmarkRedialTimer, landmark))
				pkg.GetNodeWatcher().Unwatch(p, d.ID())
				return
			}
		}
	}

	if peer.PeersEqual(p, d.myParent) {
		d.logger.Warnf("failed to dial parent... %s", p.String())
		d.myParent = nil
		pkg.GetNodeWatcher().Unwatch(p, d.ID())
		if d.myGrandParent != nil {
			d.logger.Warnf("Falling back to grandparent %s", d.myGrandParent.String())
			d.myPendingParentInRecovery = d.myGrandParent
			d.sendJoinAsChildMsg(d.myGrandParent, 0, d.self.NrChildren(), true)
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
		d.removeSibling(sibling, true, true)
		return
	}

	pkg.GetNodeWatcher().Unwatch(p, d.ID())
	d.logger.Errorf("Failed to dial unknown peer %s", p.String())
}

func (d *DemmonTree) OutConnDown(p peer.Peer) {
	d.handlePeerDown(p)
}

func (d *DemmonTree) handlePeerDownNotification(notification notification.Notification) {
	p := notification.(peerDownNotification).peerDown
	d.logger.Errorf("peer down %s (PHI >= %f)", p.String(), d.config.PhiLevelForNodeDown)
	d.handlePeerDown(p.Peer)
}

func (d *DemmonTree) handlePeerDown(p peer.Peer) {
	if peer.PeersEqual(p, d.myParent) {
		d.logger.Warnf("Parent down %s", p.String())
		d.myParent = nil
		pkg.GetNodeWatcher().Unwatch(p, d.ID())
		if d.myGrandParent != nil {
			d.logger.Warnf("Falling back to grandparent %s", d.myGrandParent.String())
			d.myPendingParentInRecovery = d.myGrandParent
			d.sendJoinAsChildMsg(d.myGrandParent, 0, d.self.NrChildren(), true)
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
		d.removeSibling(sibling, true, true)
		return
	}

	pkg.GetNodeWatcher().Unwatch(p, d.ID())
	d.logger.Errorf("Unknown peer down %s", p.String())
}

func (d *DemmonTree) MessageDelivered(message message.Message, peer peer.Peer) {
	// d.logger.Infof("Message %+v delivered to: %s", message, peer.String())
	d.metrics.nrMessagesSentCounter.Add(1)
}

func (d *DemmonTree) MessageDeliveryErr(message message.Message, p peer.Peer, error errors.Error) {
	d.logger.Errorf("Message %+v failed to deliver to: %s because: %s", message, p.String(), error.Reason())
	switch message := message.(type) {
	case joinMessage:
		_, ok := d.retries[p.String()]
		if !ok {
			d.retries[p.String()] = 0
		}
		if d.joinLevel != math.MaxUint16 {
			d.retries[p.String()]++
			if d.retries[p.String()] >= d.config.MaxRetriesJoinMsg {
				d.logger.Warnf("Deleting peer %s from currLevelPeers because it exceeded max retries (%d)", p.String(), d.config.MaxRetriesJoinMsg)
				delete(d.currLevelPeers[d.joinLevel], p.String())
				delete(d.retries, p.String())
				if d.canProgressToNextStep() {
					d.progressToNextStep()
				}
				return
			}
			d.sendMessageAndMeasureLatency(message, p)
		}
	case joinAsChildMessage:
		if peer.PeersEqual(p, d.myGrandParent) { // message was a fault-tolerance message to grandparent
			d.myGrandParent = nil
			d.fallbackToMeasuredPeers()
			return
		}

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
	// 		pkg.Dial(d.myParent, d.ID(), stream.NewTCPDialer())
	// 	}

	// case updateParentMessage:
	// 	_, isChild := d.myChildren[p.String()]
	// 	if error.Reason() == "stream not found" && isChild {
	// 		pkg.Dial(p, d.ID(), stream.NewTCPDialer())
	// 	}

	case randomWalkMessage:
		d.sendMessageTmpUDPChan(message, p)

	case disconnectAsChildMessage:
		pkg.Disconnect(d.ID(), p)

	}
}

func (d *DemmonTree) joinOverlay() {
	nrLandmarks := len(d.config.Landmarks)
	d.currLevelPeersDone = []map[string]*PeerWithIdChain{make(map[string]*PeerWithIdChain, nrLandmarks)} // start level 1
	d.currLevelPeers = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)}                   // start level 1
	d.joinLevel = 0
	d.logger.Infof("Landmarks:")
	for i, landmark := range d.config.Landmarks {
		d.logger.Infof("%d :%s", i, landmark.String())
		d.lastLevelProgress = time.Now()
		d.currLevelPeers[d.joinLevel][landmark.String()] = landmark
		joinMsg := joinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark)
	}
}

func (d *DemmonTree) sendJoinAsChildMsg(newParent *PeerWithIdChain, newParentLat time.Duration, nrChildren uint16, urgent bool) {
	d.logger.Infof("Pending parent: %s", newParent.String())
	d.logger.Infof("Joining level %d", uint16(len(newParent.Chain())))
	toSend := NewJoinAsChildMessage(d.self, newParentLat, newParent.PeerIDChain, urgent)
	d.sendMessageTmpTCPChan(toSend, newParent)
}

func (d *DemmonTree) unwatchPeersInLevelDone(level uint16, exclusions ...*PeerWithIdChain) {
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
	currLevelPeersDone := d.GetPeersInLevelByLat(d.joinLevel, d.lastLevelProgress.Add(d.config.MaxTimeToProgressToNextLevel))
	if len(currLevelPeersDone) == 0 {
		if d.joinLevel > 0 {
			currLevelPeersDone := d.GetPeersInLevelByLat(d.joinLevel-1, d.lastLevelProgress.Add(d.config.MaxTimeToProgressToNextLevel))
			lowestLatencyPeer := currLevelPeersDone[0]
			d.myPendingParentInJoin = lowestLatencyPeer
			d.sendJoinAsChildMsg(lowestLatencyPeer.PeerWithIdChain, lowestLatencyPeer.MeasuredLatency, 0, true)
			return
		} else {
			d.logger.Panic("Do not have enough measurements for landmarks")
		}
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
		d.self.SetVersion(d.self.Version() + 1)
		d.logger.Infof("My Coordinates: %+v", d.self.Coordinates)
	}

	lowestLatencyPeer := currLevelPeersDone[0]
	d.lastLevelProgress = time.Now()

	d.logger.Infof("Lowest Latency Peer: %s , Latency: %d", lowestLatencyPeer.String(), lowestLatencyPeer.MeasuredLatency)
	toPrint := ""
	for _, peerDone := range currLevelPeersDone {
		toPrint = toPrint + "; " + peerDone.String()
	}
	d.logger.Infof("d.currLevelPeersDone (level %d): %s:", d.joinLevel, toPrint)

	if d.joinLevel == 0 {
		if lowestLatencyPeer.NrChildren() < d.config.MinGrpSize {
			d.myPendingParentInJoin = lowestLatencyPeer
			d.sendJoinAsChildMsg(lowestLatencyPeer.PeerWithIdChain, lowestLatencyPeer.MeasuredLatency, 0, false)
			return
		} else {
			d.unwatchPeersInLevelDone(d.joinLevel, lowestLatencyPeer.PeerWithIdChain)
			d.progressToNextLevel(lowestLatencyPeer)
			return
		}
	}

	lowestLatencyPeerParent := d.parents[lowestLatencyPeer.String()]
	d.logger.Infof("Joining level %d because nodes in this level have not enough members", d.joinLevel)
	info, err := pkg.GetNodeWatcher().GetNodeInfo(lowestLatencyPeerParent)
	if err != nil {
		d.logger.Panic(err.Reason())
	}
	parentLatency := info.LatencyCalc.CurrValue()
	if parentLatency < lowestLatencyPeer.MeasuredLatency {
		d.unwatchPeersInLevelDone(d.joinLevel-1, lowestLatencyPeerParent)
		d.myPendingParentInJoin = NewMeasuredPeer(lowestLatencyPeerParent, parentLatency)
		d.sendJoinAsChildMsg(lowestLatencyPeerParent, info.LatencyCalc.CurrValue(), 0, false)
		return
	} else {
		if lowestLatencyPeer.NrChildren() >= d.config.MinGrpSize {
			d.unwatchPeersInLevelDone(d.joinLevel - 1)
			d.unwatchPeersInLevelDone(d.joinLevel, lowestLatencyPeer.PeerWithIdChain)
			d.progressToNextLevel(lowestLatencyPeer)
			return
		} else {
			if lowestLatencyPeerParent.NrChildren() >= d.config.MaxGrpSize {
				d.unwatchPeersInLevelDone(d.joinLevel-1, lowestLatencyPeerParent)
				d.myPendingParentInJoin = lowestLatencyPeer
				d.sendJoinAsChildMsg(lowestLatencyPeer.PeerWithIdChain, lowestLatencyPeer.MeasuredLatency, 0, false)
				return
			}
			d.myPendingParentInJoin = NewMeasuredPeer(lowestLatencyPeerParent, parentLatency)
			d.sendJoinAsChildMsg(lowestLatencyPeerParent, parentLatency, 0, false)
			return
		}
	}
}

func (d *DemmonTree) progressToNextLevel(lowestLatencyPeer peer.Peer) {
	d.logger.Infof("Progressing to next level (currLevel=%d), (nextLevel=%d) ", d.joinLevel, d.joinLevel+1)
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
		d.currLevelPeersDone[d.joinLevel] = make(map[string]*PeerWithIdChain)
	} else {
		d.currLevelPeersDone = append(d.currLevelPeersDone, make(map[string]*PeerWithIdChain))
	}

	d.currLevelResponseTimeuts = make(map[string]int)
	for _, p := range d.currLevelPeers[d.joinLevel] {
		d.currLevelResponseTimeuts[p.String()] = pkg.RegisterTimer(d.ID(), NewJoinMessageResponseTimeout(d.config.JoinMessageTimeout, p))
		d.sendMessageAndMeasureLatency(NewJoinMessage(), p)
	}
}

// aux functions

func (d *DemmonTree) sendMessageAndMeasureLatency(toSend message.Message, destPeer peer.Peer) {
	d.sendMessageTmpTCPChan(toSend, destPeer)
	pkg.GetNodeWatcher().Watch(destPeer, d.ID())
}

func (d *DemmonTree) sendMessageTmpTCPChan(toSend message.Message, destPeer peer.Peer) {
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	pkg.SendMessageSideStream(toSend, destPeer, destPeer.ToTCPAddr(), d.ID(), []protocol.ID{d.ID()})
}

func (d *DemmonTree) sendMessageTmpUDPChan(toSend message.Message, destPeer peer.Peer) {
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	pkg.SendMessageSideStream(toSend, destPeer, destPeer.ToUDPAddr(), d.ID(), []protocol.ID{d.ID()})
}

func (d *DemmonTree) sendMessage(toSend message.Message, destPeer peer.Peer) {
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	pkg.SendMessage(toSend, destPeer, d.ID(), []protocol.ID{d.ID()})
}

func (d *DemmonTree) sendMessageAndDisconnect(toSend message.Message, destPeer peer.Peer) {
	// d.logger.Infof("Sending message type %s : %+v to: %s", reflect.TypeOf(toSend), toSend, destPeer.String())
	pkg.SendMessageAndDisconnect(toSend, destPeer, d.ID(), []protocol.ID{d.ID()})
}

func (d *DemmonTree) GetPeersInLevelByLat(level uint16, deadline time.Time) MeasuredPeersByLat {
	if len(d.currLevelPeersDone[level]) == 0 {
		return MeasuredPeersByLat{}
	}

	measuredPeersInLvl := make(MeasuredPeersByLat, 0, len(d.currLevelPeersDone[level]))
	for _, currPeerWithChain := range d.currLevelPeersDone[level] {
		currPeer := currPeerWithChain
		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfoWithDeadline(currPeer.Peer, deadline)
		if err != nil {
			d.logger.Warnf("Do not have latency measurement for %s", currPeer.String())
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
		childId := child.Chain()[len(child.Chain())-1]
		occupiedIds[childId] = true
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
	peerParent, ok := d.parents[node.String()]
	if !ok {
		d.joinOverlay()
		return
	}
	info, err := pkg.GetNodeWatcher().GetNodeInfo(peerParent.Peer)
	var peerLat time.Duration
	if err != nil {
		d.logger.Errorf("Peer %s has no latency measurement", node.String())
		peerLat = 0
	} else {
		peerLat = info.LatencyCalc.CurrValue()
	}

	d.unwatchPeersInLevelDone(d.joinLevel)
	d.joinLevel--
	d.myPendingParentInJoin = NewMeasuredPeer(peerParent, 0)
	d.sendJoinAsChildMsg(peerParent, peerLat, 0, true)
}

func (d *DemmonTree) fallbackToMeasuredPeers() {
	var bestPeer *MeasuredPeer
	for _, measuredPeer := range d.measuredPeers {
		if !d.isNeighbour(measuredPeer.Peer) && !measuredPeer.IsDescendentOf(d.self.Chain()) && !peer.PeersEqual(measuredPeer, pkg.SelfPeer()) {
			d.logger.Infof("%s : %s", measuredPeer.String(), measuredPeer.MeasuredLatency)
			bestPeer = measuredPeer
			break
		}
	}

	if bestPeer == nil {
		d.joinOverlay()
		return
	}

	d.myPendingParentInRecovery = bestPeer.PeerWithIdChain
	d.sendJoinAsChildMsg(bestPeer.PeerWithIdChain, bestPeer.MeasuredLatency, uint16(len(d.myChildren)), true)
}

func (d *DemmonTree) updateAndComparePeers(p1 *PeerWithIdChain, p2 *PeerWithIdChain) bool {
	if peer.PeersEqual(p2, p1) {
		if p1.Version() > p2.Version() {
			// message holds the updated version
			p2.SetChain(p1.Chain())
			p2.SetChildrenNr(p1.NrChildren())
			p2.SetVersion(p1.Version())
			p2.SetCoords(p1.Coordinates)
		}
		if p1.Version() < p2.Version() {
			// i hold the updated version
			p1.SetChain(p2.Chain())
			p1.SetChildrenNr(p2.NrChildren())
			p1.SetVersion(p2.Version())
			p1.SetCoords(p2.Coordinates)
		}
		return true
	}
	return false
}

func (d *DemmonTree) mergeEViewWith(sample []*PeerWithIdChain, sender *PeerWithIdChain, nrPeersToMerge int) (sampleToSend, neighboursWithoutSenderDescendants []*PeerWithIdChain) {
	selfInSample := false
	nrPeersMerged := 0
	if len(d.self.Chain()) > 0 {
	outer:
		for i := len(sample) - 1; i >= 0; i-- {
			currPeer := sample[i]

			if currPeer.IsDescendentOf(d.self.Chain()) {
				continue outer
			}

			if peer.PeersEqual(currPeer, pkg.SelfPeer()) {
				selfInSample = true
				currPeer.SetChain(d.self.Chain())
				currPeer.SetChildrenNr(d.self.NrChildren())
				currPeer.SetVersion(d.self.Version())
				currPeer.SetCoords(d.self.Coordinates)
				continue
			}

			if d.isNeighbour(currPeer) {
				continue
			}

			if d.updateAndComparePeers(d.myParent, currPeer) {
				continue outer
			}

			if sibling, ok := d.mySiblings[currPeer.String()]; ok {
				d.updateAndComparePeers(sibling, currPeer)
				continue outer
			}

			if children, isChildren := d.myChildren[currPeer.String()]; isChildren {
				d.updateAndComparePeers(children, currPeer)
				continue outer
			}

			for idx, eViewPeer := range d.eView {
				if d.updateAndComparePeers(eViewPeer, currPeer) {
					if eViewPeer.IsDescendentOf(d.self.Chain()) {
						d.removeFromMeasuredPeers(eViewPeer)
						copy(d.eView[idx:], d.eView[idx+1:])
						d.eView[len(d.eView)-1] = nil
						d.eView = d.eView[:len(d.eView)-1]
					}
					continue outer
				}
			}

			if nrPeersMerged < nrPeersToMerge {
				if len(d.eView) >= d.config.MaxPeersInEView { // eView is full
					toRemoveIdx := rand.Intn(len(d.eView))
					d.eView[toRemoveIdx] = currPeer
				} else {
					d.eView = append(d.eView, currPeer)
				}
				nrPeersMerged++
			} else {
				break
			}
		}
	}

	neighbours := d.getNeighborsAsPeerWithIdChainArray()
	neighboursWithoutSenderDescendants = getExcludingDescendantsOf(neighbours, sender.PeerIDChain)
	sampleAsPeerArr := []peer.Peer{}
	for _, p := range sample {
		sampleAsPeerArr = append(sampleAsPeerArr, p)
	}
	neighboursWithoutSenderDescendantsAndNotInSample := getPeersExcluding(neighboursWithoutSenderDescendants, sampleAsPeerArr...)

	if !selfInSample {
		sampleToSend = getRandSample(nrPeersToMerge-1, append(neighboursWithoutSenderDescendantsAndNotInSample, d.eView...)...)
		if len(d.self.Chain()) > 0 && !d.self.IsDescendentOf(sender.PeerIDChain) {
			sampleToSend = append(sampleToSend, d.self)
		}
	} else {
		sampleToSend = getRandSample(nrPeersToMerge, append(neighboursWithoutSenderDescendantsAndNotInSample, d.eView...)...)
	}

	sampleToSend = append(sampleToSend, sample...)

	if len(sampleToSend) > d.config.NrPeersInWalkMessage {
		for i := d.config.NrPeersInWalkMessage; i < len(sampleToSend); i++ {
			sampleToSend[i] = nil
		}
		sampleToSend = sampleToSend[:d.config.NrPeersInWalkMessage]
	}

	return sampleToSend, neighboursWithoutSenderDescendants
}

func (d *DemmonTree) mergeSiblingsWith(newSiblings []*PeerWithIdChain) {
	for _, msgSibling := range newSiblings {
		if peer.PeersEqual(pkg.SelfPeer(), msgSibling) {
			continue
		}
		sibling, ok := d.mySiblings[msgSibling.String()]
		if !ok {
			d.addSibling(msgSibling, true, true)
			continue
		}
		if sibling.Version() > msgSibling.version {
			sibling.SetChain(msgSibling.Chain())
			sibling.SetChildrenNr(msgSibling.NrChildren())
			sibling.SetVersion(msgSibling.Version())
			sibling.SetCoords(msgSibling.Coordinates)
		}
	}

	for siblingId, mySibling := range d.mySiblings {
		found := false
		for _, msgSibling := range newSiblings {
			if peer.PeersEqual(mySibling, msgSibling) {
				found = true
				break
			}
		}
		if !found {

			_, isChildren := d.myChildren[siblingId]
			isParent := peer.PeersEqual(d.myParent, mySibling)
			isPendingParent := peer.PeersEqual(d.myPendingParentInAbsorb, mySibling)

			if isChildren || isParent || isPendingParent {
				d.removeSibling(mySibling, false, false)
			} else {
				d.removeSibling(mySibling, true, true)
			}

		}
	}
}

func (d *DemmonTree) getChildrenAsPeerWithIdChainArray(exclusions ...*PeerWithIdChain) []*PeerWithIdChain {
	toReturn := make([]*PeerWithIdChain, 0, d.self.NrChildren()-1)
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

func (d *DemmonTree) getNeighborsAsPeerWithIdChainArray() []*PeerWithIdChain {
	possibilitiesToSend := make([]*PeerWithIdChain, 0) // parent and me
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

func (d *DemmonTree) isNeighbour(toTest peer.Peer) bool {
	if peer.PeersEqual(toTest, pkg.SelfPeer()) {
		panic("is self")
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

func (d *DemmonTree) isNodeDown(n *pkg.NodeInfo) bool {
	// d.logger.Infof("Node %s phi: %f", n.Peer, n.Detector.Phi())
	return !n.Detector.IsAvailable()
}

func (d *DemmonTree) removeFromMeasuredPeers(p peer.Peer) {
	for i := 0; i < len(d.measuredPeers); i++ {
		curr := d.measuredPeers[i]
		if peer.PeersEqual(curr, p) {
			copy(d.measuredPeers[i:], d.measuredPeers[i+1:])
			d.measuredPeers[len(d.measuredPeers)-1] = nil
			d.measuredPeers = d.measuredPeers[:len(d.measuredPeers)-1]
			return
		}
	}
}

func (d *DemmonTree) addToMeasuredPeers(p *PeerWithIdChain) {
	nodeInfo, err := pkg.GetNodeWatcher().GetNodeInfo(p)
	if err != nil {
		return
	}
	nodeLat := nodeInfo.LatencyCalc.CurrValue()
	measuredPeer := NewMeasuredPeer(p, nodeLat)
	d.measuredPeers = append(d.measuredPeers, measuredPeer)
	sort.Sort(d.measuredPeers)
	if len(d.measuredPeers) > d.config.MeasuredPeersSize {
		for i := d.config.MeasuredPeersSize; i < len(d.measuredPeers); i++ {
			d.measuredPeers[i] = nil
		}
		d.measuredPeers = d.measuredPeers[:d.config.MeasuredPeersSize]
	}
}

func (d *DemmonTree) addParent(newParent *PeerWithIdChain, newGrandParent *PeerWithIdChain, myNewChain PeerIDChain, watchNewParent, setupFaultDetector bool, parentLatency time.Duration, disconnectFromParent, sendDisconnectMsg, dialParent bool) {
	d.logger.Warnf("My level changed: (%d -> %d)", d.self.Level(), newParent.PeerIDChain.Level()+1) // IMPORTANT FOR VISUALIZER
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
			if sendDisconnectMsg {
				toSend := NewDisconnectAsChildMessage()
				d.sendMessageAndDisconnect(toSend, d.myParent)
			} else {
				pkg.Disconnect(d.ID(), d.myParent)
			}
		}
	}

	d.myGrandParent = newGrandParent
	d.myParent = newParent
	d.self.SetChain(myNewChain)
	d.self.SetVersion(d.self.Version() + 1)
	d.joinLevel = math.MaxUint16

	if watchNewParent {
		if parentLatency != 0 {
			pkg.GetNodeWatcher().WatchWithInitialLatencyValue(newParent, d.ID(), parentLatency)
		} else {
			pkg.GetNodeWatcher().Watch(newParent, d.ID())
		}
	}

	if setupFaultDetector {
		c := pkg.Condition{
			Repeatable:                false,
			CondFunc:                  d.isNodeDown,
			EvalConditionTickDuration: 1000 * time.Millisecond,
			Notification:              peerDownNotification{peerDown: newParent},
			Peer:                      newParent,
			EnableGracePeriod:         false,
			ProtoId:                   d.ID(),
		}
		pkg.GetNodeWatcher().NotifyOnCondition(c)
	}

	if dialParent {
		pkg.Dial(d.ID(), newParent, newParent.ToTCPAddr())
	} else {
		d.logger.Infof("Dialed parent with success, parent: %s", d.myParent.String()) // just here to visualize
	}
	d.removeFromMeasuredPeers(newParent)
	// d.resetSwitchTimer()
	d.resetImproveTimer()

	for _, child := range d.myChildren {
		childId := child.Chain()[len(child.Chain())-1]
		child.SetChain(append(myNewChain, childId))
	}
}

func (d *DemmonTree) addChild(newChild *PeerWithIdChain, dialChild bool, watchChild bool, childrenLatency time.Duration) PeerID {
	proposedId := d.generateChildId()
	if watchChild {
		if childrenLatency != 0 {
			pkg.GetNodeWatcher().WatchWithInitialLatencyValue(newChild, d.ID(), childrenLatency)
		} else {
			pkg.GetNodeWatcher().Watch(newChild, d.ID())
		}
		c := pkg.Condition{
			Repeatable:                false,
			CondFunc:                  d.isNodeDown,
			EvalConditionTickDuration: 1000 * time.Millisecond,
			Notification:              peerDownNotification{peerDown: newChild},
			Peer:                      newChild,
			EnableGracePeriod:         false,
			ProtoId:                   d.ID(),
		}
		pkg.GetNodeWatcher().NotifyOnCondition(c)
	}

	d.myChildren[newChild.String()] = newChild
	if dialChild {
		pkg.Dial(d.ID(), newChild, newChild.ToTCPAddr())
	}
	newChild.SetChain(append(d.self.PeerIDChain, proposedId))
	d.self.SetChildrenNr(uint16(len(d.myChildren)))
	d.self.SetVersion(d.self.Version() + 1)
	for _, child := range d.myChildren {
		toSend := NewUpdateParentMessage(d.myParent, d.self, child.Chain()[len(child.Chain())-1], d.getChildrenAsPeerWithIdChainArray(child))
		d.sendMessage(toSend, child.Peer)
	}
	d.removeFromMeasuredPeers(newChild)
	// d.resetSwitchTimer()
	d.resetImproveTimer()
	return proposedId
}

func (d *DemmonTree) removeChild(toRemove peer.Peer, disconnect bool, unwatch bool) {
	if disconnect {
		pkg.Disconnect(d.ID(), toRemove)
	}
	if unwatch {
		pkg.GetNodeWatcher().Unwatch(toRemove, d.ID())
	}
	delete(d.myChildrenLatencies, toRemove.String())
	delete(d.myChildren, toRemove.String())
	d.self.SetChildrenNr(uint16(len(d.myChildren)))
	d.self.SetVersion(d.self.Version() + 1)
}

func (d *DemmonTree) addSibling(newSibling *PeerWithIdChain, watch bool, dial bool) {
	d.mySiblings[newSibling.String()] = newSibling
	if watch {
		pkg.GetNodeWatcher().Watch(newSibling.Peer, d.ID())
		c := pkg.Condition{
			Repeatable:                false,
			CondFunc:                  d.isNodeDown,
			EvalConditionTickDuration: 1000 * time.Millisecond,
			Notification:              peerDownNotification{peerDown: newSibling},
			Peer:                      newSibling,
			EnableGracePeriod:         false,
			ProtoId:                   d.ID(),
		}
		pkg.GetNodeWatcher().NotifyOnCondition(c)
	}
	if dial {
		pkg.Dial(d.ID(), newSibling.Peer, newSibling.Peer.ToTCPAddr())
	}
	d.removeFromMeasuredPeers(newSibling)
}

func (d *DemmonTree) removeSibling(toRemove peer.Peer, unwatch, disconnect bool) {
	delete(d.mySiblings, toRemove.String())
	if unwatch {
		pkg.GetNodeWatcher().Unwatch(toRemove, d.ID())
	}
	if disconnect {
		pkg.Disconnect(d.ID(), toRemove)
	}
}

// func (d *DemmonTree) resetSwitchTimer() {
// 	if d.switchTimerId != -1 {
// 		err := pkg.CancelTimer(d.switchTimerId)
// 		if err != nil {
// 			d.switchTimerId = -1
// 			return
// 		}
// 		d.switchTimerId = pkg.RegisterTimer(d.ID(), NewSwitchTimer(d.config.CheckSwitchOportunityTimeout))
// 	}
// }

func (d *DemmonTree) resetImproveTimer() {
	if d.improveTimerId != -1 {
		err := pkg.CancelTimer(d.improveTimerId)
		if err != nil {
			d.improveTimerId = -1
			return
		}
		d.improveTimerId = pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
	}
}

func (d *DemmonTree) handleSwitchTimer(timer timer.Timer) {
	if !d.config.EnableSwitch {
		return
	}

	d.logger.Info("SwitchTimer trigger")
	if d.switchTimerId == -1 {
		d.switchTimerId = pkg.RegisterTimer(d.ID(), NewSwitchTimer(d.config.CheckSwitchOportunityTimeout))
		return
	}
	d.switchTimerId = pkg.RegisterTimer(d.ID(), NewSwitchTimer(d.config.CheckSwitchOportunityTimeout))

	if d.myParent == nil || len(d.self.Chain()) == 0 || d.myPendingParentInImprovement != nil || d.myPendingParentInRecovery != nil || uint16(len(d.myChildren)) < d.config.MinGrpSize {
		return
	}

	r := rand.Float32()
	if r < d.config.SwitchProbability {
		return
	}

	myLatTotal := &big.Int{}
	for childId, child := range d.myChildren {
		currNodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(child.Peer)
		if err != nil {
			d.logger.Errorf("Do not yet have latency measurement for children %s", childId)
			return
		}
		myLatTotal.Add(myLatTotal, big.NewInt(int64(currNodeStats.LatencyCalc.CurrValue())))
	}

	var bestCandidateToSwitch *PeerWithIdChain
	var bestCandidateLatTotal *big.Int

	for childId, candidateToSwitch := range d.myChildren {
		candidateToSwitchSiblingLatencies := d.myChildrenLatencies[candidateToSwitch.String()]

		if len(candidateToSwitchSiblingLatencies) < len(d.myChildren)-1 {
			d.logger.Warnf("Skipping children %s because it does not have enough measured siblings", candidateToSwitch.String())
			continue
		}

		latTotal := &big.Int{}

		currNodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(candidateToSwitch.Peer)
		if err != nil {
			d.logger.Errorf("Do not have latency measurement for children %s", childId)
			continue
		}
		latTotal.Add(latTotal, big.NewInt(int64(currNodeStats.LatencyCalc.CurrValue())))

		d.logger.Infof("candidateToSwitchSiblingLatencies: %+v:", candidateToSwitchSiblingLatencies)

		for i := 0; i < len(candidateToSwitchSiblingLatencies); i++ {
			currCandidateAbsorbtion := candidateToSwitchSiblingLatencies[i]
			latTotal.Add(latTotal, big.NewInt(int64(currCandidateAbsorbtion.MeasuredLatency)))
		}

		if bestCandidateLatTotal == nil {
			bestCandidateToSwitch = candidateToSwitch
			bestCandidateLatTotal = latTotal
			continue
		}

		if latTotal.Cmp(bestCandidateLatTotal) == -1 {
			bestCandidateToSwitch = candidateToSwitch
			bestCandidateLatTotal = latTotal
			continue
		}
	}

	if bestCandidateToSwitch == nil {
		return
	}

	if bestCandidateLatTotal.Cmp(myLatTotal) == -1 { // bestCandidateLatTotal < myLatTotal
		bestCandidateLatImprovement := &big.Int{}
		bestCandidateLatImprovement = bestCandidateLatImprovement.Sub(myLatTotal, bestCandidateLatTotal)
		minLatencyImprovementForSwitch := big.NewInt(int64(d.config.MinLatencyImprovementPerPeerForSwitch) * int64(len(d.myChildren)-1))
		if bestCandidateLatImprovement.Cmp(minLatencyImprovementForSwitch) == 1 {
			// bestCandidateLatImprovement > minLatencyImprovementForSwitch
			bestCandidateChain := bestCandidateToSwitch.Chain()
			bestCandidateToSwitch.SetChain(d.self.PeerIDChain)
			for _, child := range d.myChildren {
				childrenToSend := d.getChildrenAsPeerWithIdChainArray(bestCandidateToSwitch, child)
				toSend := NewSwitchMessage(bestCandidateToSwitch, d.myParent, childrenToSend, true, false)
				d.sendMessage(toSend, child.Peer)
			}
			childrenToSend := d.getChildrenAsPeerWithIdChainArray(bestCandidateToSwitch)
			toSend := NewSwitchMessage(bestCandidateToSwitch, d.myParent, childrenToSend, false, true)
			for _, child := range d.myChildren {
				d.addSibling(child, false, false)
				d.removeChild(child, false, false)
			}
			d.sendMessageAndDisconnect(toSend, d.myParent)
			d.addParent(bestCandidateToSwitch, d.myGrandParent, bestCandidateChain, false, true, 0, false, false, false)
		}
	}
}

// // VERSION where nodes n nodes  with lowest latency are picked, and the one with least overall latency to its siblings get picked
// func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
// 	d.absorbTimerId = pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
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
// 		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(children)
// 		if err != nil {
// 			d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", children.String())
// 			return
// 		}
// 		currLat := nodeStats.LatencyCalc.CurrValue()
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
// 	toSend := NewAbsorbMessage(bestCandidateChildrenToAbsorb, bestCandidate.PeerWithIdChain)

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
// 	pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))
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
// 		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(children)
// 		if err != nil {
// 			d.logger.Warnf("Do not have latency measurement for %s", children.String())
// 			continue
// 		}
// 		currLat := nodeStats.LatencyCalc.CurrValue()
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

// 		toSend := NewAbsorbMessage(toAbsorb, candidateToAbsorb.PeerWithIdChain)
// 		d.logger.Infof("Sending absorb message %+v to %s", toSend, candidateToAbsorb.String())

// 		for _, child := range d.myChildren {
// 			d.sendMessage(toSend, child)
// 		}

// 		for _, newGrandChildren := range toAbsorb {
// 			d.addToMeasuredPeers(newGrandChildren.PeerWithIdChain)
// 			d.removeChild(newGrandChildren.PeerWithIdChain, true, true)
// 		}
// 		return
// 	}
// }

// VERSION where nodes with lowest latency pair are picked, the new children is the member of the pair with lowest latency to the parent
// func (d *DemmonTree) handleCheckChildrenSizeTimer(checkChildrenTimer timer.Timer) {
// 	d.absorbTimerId = pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))

// 	// d.logger.Info("handleCheckChildrenSize timer trigger")

// 	if d.self.NrChildren() == 0 || d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil {
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
// 	var peerAbsorber *PeerWithIdChain
// 	var peerToKick *PeerWithIdChain

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

// 			// nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(candidateToBeAbsorbed)
// 			// if err != nil {
// 			// 	d.logger.Errorf("Cannot send absorbMessage to %s as i have no latency measurement", candidateToBeAbsorbed.String())
// 			// 	continue
// 			// }
// 			// myLatToPeerToBeAbsorbed := nodeStats.LatencyCalc.CurrValue()

// 			peer1NodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(peer1)
// 			if err != nil {
// 				d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", peer1.String())
// 				continue
// 			}
// 			latToPeer1 := peer1NodeStats.LatencyCalc.CurrValue()

// 			peer2NodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(peer2)
// 			if err != nil {
// 				d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", peer2.String())
// 				continue
// 			}
// 			latToPeer2 := peer2NodeStats.LatencyCalc.CurrValue()

// 			if peer2.MeasuredLatency < lowestLatPairLat {
// 				if latToPeer1 < latToPeer2 && latToPeer1 < lowestLatPairLat {
// 					peerAbsorber = peer1
// 					peerToKick = peer2.PeerWithIdChain
// 					lowestLatPairLat = peer2.MeasuredLatency
// 				} else if latToPeer2 < lowestLatPairLat {
// 					peerAbsorber = peer2.PeerWithIdChain
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
// 		d.improveTimerId = pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
// 		return
// 	}
// 	d.improveTimerId = pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))

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
// 	parentStats, err := pkg.GetNodeWatcher().GetNodeInfo(d.myParent)
// 	if err != nil {
// 		d.logger.Error(err.Reason())
// 		return
// 	}

// 	parentLatency := parentStats.LatencyCalc.CurrValue()
// 	sort.Sort(d.measuredPeers)
// 	for _, measuredPeer := range d.measuredPeers {
// 		d.logger.Infof("Least latency peer with children: %s", measuredPeer.String())
// 		// parent latency is higher than latency to peer
// 		latencyImprovement := parentLatency - measuredPeer.MeasuredLatency
// 		if latencyImprovement < 0 {
// 			return
// 		}

// 		if latencyImprovement > d.config.MinLatencyImprovementToImprovePosition {
// 			if measuredPeer.Level() >= d.self.Level()-1 {
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
// 			d.sendJoinAsChildMsg(measuredPeer.PeerWithIdChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
// 			return
// 		}

// 		if measuredPeer.Level() < d.myParent.Level() {
// 			if measuredPeer.NrChildren() < d.config.MaxGrpSize { // must go UP in levels
// 				aux := *measuredPeer
// 				d.myPendingParentInImprovement = &aux
// 				d.sendJoinAsChildMsg(measuredPeer.PeerWithIdChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
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

// 	// if peer.PeersEqual(pkg.SelfPeer(), absorbMessage.PeerAbsorber) {
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
// 		if peer.PeersEqual(pkg.SelfPeer(), p) {
// 			if sibling, ok := d.mySiblings[absorbMessage.PeerAbsorber.String()]; ok {
// 				d.removeSibling(sibling, false, false)
// 				var peerLat time.Duration
// 				if nodeInfo, err := pkg.GetNodeWatcher().GetNodeInfo(sibling); err == nil {
// 					peerLat = nodeInfo.LatencyCalc.CurrValue()
// 				}
// 				d.myPendingParentInAbsorb = absorbMessage.PeerAbsorber
// 				toSend := NewJoinAsChildMessage(d.self, peerLat, absorbMessage.PeerAbsorber.PeerIDChain, false)
// 				d.sendMessage(toSend, absorbMessage.PeerAbsorber)
// 			} else {
// 				toSend := NewJoinAsChildMessage(d.self, 0, absorbMessage.PeerAbsorber.PeerIDChain, false)
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

// 	// if peer.PeersEqual(pkg.SelfPeer(), absorbMessage.PeerAbsorber) {
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
// 		if peer.PeersEqual(pkg.SelfPeer(), p) {
// 			if sibling, ok := d.mySiblings[absorbMessage.PeerAbsorber.String()]; ok {
// 				d.removeSibling(sibling, false, false)
// 				var peerLat time.Duration
// 				if nodeInfo, err := pkg.GetNodeWatcher().GetNodeInfo(sibling); err == nil {
// 					peerLat = nodeInfo.LatencyCalc.CurrValue()
// 				}
// 				d.myPendingParentInAbsorb = absorbMessage.PeerAbsorber
// 				toSend := NewJoinAsChildMessage(d.self, peerLat, absorbMessage.PeerAbsorber.PeerIDChain, false)
// 				d.sendMessage(toSend, absorbMessage.PeerAbsorber)
// 			} else {
// 				toSend := NewJoinAsChildMessage(d.self, 0, absorbMessage.PeerAbsorber.PeerIDChain, false)
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
// 		d.improveTimerId = pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
// 		return
// 	}

// 	d.improveTimerId = pkg.RegisterTimer(d.ID(), NewEvalMeasuredPeersTimer(d.config.EvalMeasuredPeersRefreshTickDuration))
// 	d.logger.Info("EvalMeasuredPeersTimer trigger...")

// 	if len(d.self.Chain()) == 0 || d.myParent == nil || d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil {
// 		d.logger.Info("EvalMeasuredPeersTimer returning because len(d.self.Chain()) == 0 || d.myParent == nil || d.myPendingParentInImprovement != nil...")
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
// 	parentStats, err := pkg.GetNodeWatcher().GetNodeInfo(d.myParent)
// 	if err != nil {
// 		d.logger.Error("returning due to  not having latency measurement for parent")
// 		d.logger.Error(err.Reason())
// 		return
// 	}

// 	parentLatency := parentStats.LatencyCalc.CurrValue()
// 	sort.Sort(d.measuredPeers)
// 	for _, measuredPeer := range d.measuredPeers {
// 		// parent latency is higher than latency to peer
// 		latencyImprovement := parentLatency - measuredPeer.MeasuredLatency
// 		if latencyImprovement < 0 {
// 			d.logger.Infof("returning due to latencyImprovement < 0")
// 			return
// 		}

// 		if latencyImprovement > d.config.MinLatencyImprovementToImprovePosition {
// 			if measuredPeer.Level() > d.self.Level()-1 {
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
// 			d.sendJoinAsChildMsg(measuredPeer.PeerWithIdChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
// 			return
// 		}

// 		// if measuredPeer.Level() < d.self.Level()-1 {
// 		// 	// if measuredPeer.NrChildren() < d.config.MaxGrpSize { // must go UP in levels
// 		// 	aux := *measuredPeer
// 		// 	d.myPendingParentInImprovement = &aux
// 		// 	d.sendJoinAsChildMsg(measuredPeer.PeerWithIdChain, measuredPeer.MeasuredLatency, uint16(d.self.NrChildren()), false)
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
// 	d.absorbTimerId = pkg.RegisterTimer(d.ID(), NewCheckChidrenSizeTimer(d.config.CheckChildenSizeTimerDuration))

// 	// d.logger.Info("handleCheckChildrenSize timer trigger")

// 	if d.self.NrChildren() == 0 || d.myPendingParentInImprovement != nil || d.myPendingParentInAbsorb != nil || d.myPendingParentInJoin != nil {
// 		// d.logger.Info("d.self.NrChildren() == 0, returning...")
// 		return
// 	}

// 	if uint16(len(d.myChildren)) <= d.config.MaxGrpSize {
// 		// d.logger.Info("d.self.NrChildren() < d.config.MaxGrpSize, returning...")
// 		return
// 	}

// 	childrenAsMeasuredPeers := make(MeasuredPeersByLat, 0, d.self.NrChildren())
// 	for _, children := range d.myChildren {
// 		nodeStats, err := pkg.GetNodeWatcher().GetNodeInfo(children)
// 		if err != nil {
// 			d.logger.Errorf("Do not have latency measurement for my children %s in absorb procedure", children.String())
// 			return
// 		}
// 		currLat := nodeStats.LatencyCalc.CurrValue()
// 		childrenAsMeasuredPeers = append(childrenAsMeasuredPeers, NewMeasuredPeer(children, currLat))
// 	}

// 	var peerAbsorber *PeerWithIdChain
// 	var peerToKick *PeerWithIdChain
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

// 			peerAbsorber = candidateToAbsorb.PeerWithIdChain
// 			peerToKick = candidateToBeAbsorbed.PeerWithIdChain
// 			d.logger.Infof("Sending absorb message with peerToAbsorb: %s, peerAbsorber: %s", peerToKick.String(), peerAbsorber.String())
// 			toSend := NewAbsorbMessage(peerToKick, peerAbsorber)
// 			d.sendMessage(toSend, peerToKick)
// 			d.resetImproveTimer()
// 			return
// 		}
// 	}
// }
