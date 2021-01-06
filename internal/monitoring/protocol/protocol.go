package protocol

import (
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	membershipProtocol "github.com/nm-morais/demmon/internal/membership/protocol"
	"github.com/nm-morais/demmon/internal/monitoring/engine"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const (
	MonitorProtoID = 6000
	name           = "monitor_proto"

	CleanupInterestSetTimerDuration           = 5 * time.Second
	RebroadcastNeighInterestSetsTimerDuration = 5 * time.Second
	RebroadcastTreeInterestSetsTimerDuration  = 5 * time.Second
	ExpireNeighInterestSetTimeout             = 3 * RebroadcastNeighInterestSetsTimerDuration
)

type subWithTTL struct {
	ttl         int
	p           peer.Peer
	lastRefresh time.Time
}

type neighInterestSet struct {
	nrRetries   int
	subscribers map[string]subWithTTL
	TTL         int
	IS          body_types.InterestSet
}

type treeAggSet struct {
	nrRetries   int
	AggSet      body_types.TreeAggregationSet
	childValues map[string]map[string]interface{}
	local       bool
	parent      bool
}

type Monitor struct {
	currID            membershipProtocol.PeerIDChain
	neighInterestSets map[int64]*neighInterestSet
	treeAggFuncs      map[int64]*treeAggSet

	currView membershipProtocol.InView
	logger   *logrus.Logger
	babel    protocolManager.ProtocolManager
	me       *engine.MetricsEngine
	tsdb     *tsdb.TSDB
}

func New(babel protocolManager.ProtocolManager, db *tsdb.TSDB, me *engine.MetricsEngine) *Monitor {
	return &Monitor{
		tsdb:              db,
		me:                me,
		currID:            make(membershipProtocol.PeerIDChain, 0),
		treeAggFuncs:      make(map[int64]*treeAggSet),
		neighInterestSets: make(map[int64]*neighInterestSet),
		currView:          membershipProtocol.InView{},
		babel:             babel,
		logger:            logs.NewLogger(name),
	}
}

// BOILERPLATE

func (m *Monitor) MessageDelivered(msg message.Message, p peer.Peer) {
}

func (m *Monitor) MessageDeliveryErr(msg message.Message, p peer.Peer, err errors.Error) {
	m.logger.Errorf("Message %+v failed to deliver to: %s", msg, p)
}

func (m *Monitor) ID() protocol.ID {
	return MonitorProtoID
}

func (m *Monitor) Name() string {
	return name
}

func (m *Monitor) Logger() *logrus.Logger {
	return m.logger
}

func (m *Monitor) Init() { // REPLY HANDLERS
	m.babel.RegisterNotificationHandler(m.ID(), membershipProtocol.NodeUpNotification{}, m.handleNodeUp)
	m.babel.RegisterNotificationHandler(m.ID(), membershipProtocol.NodeDownNotification{}, m.handleNodeDown)
	m.babel.RegisterNotificationHandler(m.ID(), membershipProtocol.IDChangeNotification{}, m.handlePeerIDChange)

	// REQUEST HANDLERS
	m.babel.RegisterRequestHandler(m.ID(), AddNeighborhoodInterestSetReqID, m.handleAddNeighInterestSetRequest)
	m.babel.RegisterRequestHandler(m.ID(), RemoveNeighborhoodInterestSetReqID, m.handleRemoveNeighInterestSetRequest)

	m.babel.RegisterRequestHandler(m.ID(), AddTreeAggregationFuncReqID, m.handleAddTreeAggregationFuncRequest)

	// MESSAGE HANDLERS
	m.babel.RegisterMessageHandler(
		m.ID(),
		NewInstallNeighInterestSetMessage(nil),
		m.handleInstallNeighInterestSetMessage,
	)
	m.babel.RegisterMessageHandler(
		m.ID(),
		NewPropagateNeighInterestSetMetricsMessage(0, nil, 0),
		m.handlePropagateNeighInterestSetMetricsMessage,
	)

	m.babel.RegisterMessageHandler(
		m.ID(),
		NewInstallTreeAggFuncMessage(nil),
		m.handleInstallTreeAggFuncMetricsMessage,
	)
	m.babel.RegisterMessageHandler(
		m.ID(),
		NewPropagateTreeAggFuncMetricsMessage(0, nil),
		m.handlePropagateTreeAggFuncMetricsMessage,
	)

	// TIMER HANDLERS
	m.babel.RegisterTimerHandler(
		m.ID(),
		ExportTreeAggregationFuncTimerID,
		m.handleExportTreeAggregationFuncTimer,
	)
	m.babel.RegisterTimerHandler(
		m.ID(),
		ExportNeighInterestSetMetricsTimerID,
		m.handleExportNeighInterestSetMetricsTimer,
	)
	m.babel.RegisterTimerHandler(m.ID(), RebroadcastTreeAggregationFuncsTimerID, m.handleRebroadcastTreeInterestSetsTimer)
	m.babel.RegisterTimerHandler(m.ID(), RebroadcastInterestSetTimerID, m.handleRebroadcastInterestSetsTimer)
	m.babel.RegisterTimerHandler(m.ID(), CleanupInsterestSetsTimerID, m.handleCleanupInterestSetsTimer)
}

func (m *Monitor) Start() {
	m.babel.RegisterTimer(
		m.ID(),
		NewRebroadcastInterestSetsTimer(RebroadcastNeighInterestSetsTimerDuration),
	)

	m.babel.RegisterTimer(
		m.ID(),
		NewBroadcastTreeAggregationFuncsTimer(RebroadcastTreeInterestSetsTimerDuration),
	)

	m.babel.RegisterTimer(
		m.ID(),
		NewCleanupInterestSetsTimer(CleanupInterestSetTimerDuration),
	)
}

// TIMER HANDLERS

func (m *Monitor) handleCleanupInterestSetsTimer(t timer.Timer) {
	m.babel.RegisterTimer(m.ID(), NewCleanupInterestSetsTimer(CleanupInterestSetTimerDuration))
	m.cleanupNeighInterestSets()
	m.cleanupTreeInterestSets()
}

// NOTIFICATION HANDLERS

func (m *Monitor) handlePeerIDChange(n notification.Notification) {
	idChangeNotification := n.(membershipProtocol.IDChangeNotification)
	m.currID = idChangeNotification.NewID
}

func (m *Monitor) handleNodeUp(n notification.Notification) {
	nodeUpNotification := n.(membershipProtocol.NodeUpNotification)
	m.currView = nodeUpNotification.InView
	m.logger.Infof("Node up: %s", nodeUpNotification.PeerUp.String())
	m.logger.Infof("Curr View: %+v", m.currView)
}

func (m *Monitor) handleNodeDown(n notification.Notification) {
	nodeDownNotification := n.(membershipProtocol.NodeDownNotification)
	nodeDown := nodeDownNotification.PeerDown
	m.currView = nodeDownNotification.InView
	m.logger.Infof("Node down: %s", nodeDownNotification.PeerDown.String())
	m.logger.Infof("Curr View: %+v", m.currView)

	// isSibling, isChildren, isParent := m.getPeerRelationshipType(nodeDown)
	for intSetID, intSet := range m.neighInterestSets {
		if _, ok := intSet.subscribers[nodeDown.String()]; ok {
			// m.tsdb.DeleteBucket(intSet.interestSet.OutputBucketOpts.Name, map[string]string{"host": m.babel.SelfPeer().IP().String()})
			delete(intSet.subscribers, nodeDown.String())
			if len(intSet.subscribers) == 0 {
				delete(m.neighInterestSets, intSetID)
				m.logger.Infof("Deleting neigh int set: %d", intSetID)
			}
		}
	}
}

// UTILS

func (m *Monitor) isPeerInView(p peer.Peer) bool {
	for _, s := range m.currView.Siblings {
		if peer.PeersEqual(s, p) {
			return true
		}
	}

	for _, c := range m.currView.Children {
		if peer.PeersEqual(c, p) {
			return true
		}
	}

	return peer.PeersEqual(p, m.currView.Parent)
}

func (m *Monitor) getPeerRelationshipType(p peer.Peer) (isSibling, isChildren, isParent bool) {
	for _, sibling := range m.currView.Siblings {
		if peer.PeersEqual(sibling, p) {
			return true, false, false
		}
	}

	for _, children := range m.currView.Children {
		if peer.PeersEqual(children, p) {
			return false, true, false
		}
	}

	if peer.PeersEqual(p, m.currView.Parent) {
		return false, false, true
	}
	return false, false, false
}

// BOILERPLATE

func (m *Monitor) DialFailed(p peer.Peer) {
}

func (m *Monitor) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {
	return false
}

func (m *Monitor) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	return false
}

func (m *Monitor) OutConnDown(p peer.Peer) {}
