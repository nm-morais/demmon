package protocol

import (
	"reflect"
	"time"

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

	CleanupInterestSetTimerDuration = 5 * time.Second
	// neigh sets
	RebroadcastNeighInterestSetsTimerDuration = 5 * time.Second
	ExpireNeighInterestSetTimeout             = 3 * RebroadcastNeighInterestSetsTimerDuration

	// tree agg funcs
	RebroadcastTreeAggFuncTimerDuration   = 5 * time.Second
	RebroadcastGlobalAggFuncTimerDuration = 5 * time.Second

	// global agg funcs
	ExpireGlobalAggFuncTimeout = 3 * RebroadcastNeighInterestSetsTimerDuration
	ExpireTreeAggFuncTimeout   = 3 * RebroadcastTreeAggFuncTimerDuration
)

type Monitor struct {
	currID            membershipProtocol.PeerIDChain
	neighInterestSets map[int64]*neighInterestSet
	treeAggFuncs      map[int64]*treeAggSet
	globalAggFuncs    map[int64]*globalAggFunc

	currView membershipProtocol.InView
	logger   *logrus.Logger
	babel    protocolManager.ProtocolManager
	me       *engine.MetricsEngine
	tsdb     *tsdb.TSDB
}

func New(babel protocolManager.ProtocolManager, db *tsdb.TSDB, me *engine.MetricsEngine) *Monitor {
	return &Monitor{
		currID:            make(membershipProtocol.PeerIDChain, 0),
		neighInterestSets: make(map[int64]*neighInterestSet),
		treeAggFuncs:      make(map[int64]*treeAggSet),
		globalAggFuncs:    make(map[int64]*globalAggFunc),
		currView:          membershipProtocol.InView{},
		logger:            logs.NewLogger(name),
		babel:             babel,
		me:                me,
		tsdb:              db,
	}
}

// BOILERPLATE

func (m *Monitor) MessageDelivered(msg message.Message, p peer.Peer) {
	m.logger.Infof("Message of type %s delivered to: %s", reflect.TypeOf(msg), p)
}

func (m *Monitor) MessageDeliveryErr(msg message.Message, p peer.Peer, err errors.Error) {
	m.logger.Errorf("Message of type %s : %+v failed to deliver to: %s", reflect.TypeOf(msg), msg, p)
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

func (m *Monitor) SendMessage(msg message.Message, p peer.Peer) {
	// m.logger.Infof("Sending message of type %s to %s", reflect.TypeOf(msg), p.String())
	m.babel.SendMessage(msg, p, m.ID(), m.ID(), true)
}

func (m *Monitor) Init() { // REPLY HANDLERS
	m.babel.RegisterNotificationHandler(m.ID(), membershipProtocol.NodeUpNotification{}, m.handleNodeUp)
	m.babel.RegisterNotificationHandler(m.ID(), membershipProtocol.NodeDownNotification{}, m.handleNodeDown)
	m.babel.RegisterNotificationHandler(m.ID(), membershipProtocol.IDChangeNotification{}, m.handlePeerIDChange)

	// NEIGH INT SETS

	m.babel.RegisterRequestHandler(m.ID(), AddNeighborhoodInterestSetReqID, m.handleAddNeighInterestSetRequest)
	m.babel.RegisterRequestHandler(m.ID(), RemoveNeighborhoodInterestSetReqID, m.handleRemoveNeighInterestSetRequest)

	m.babel.RegisterTimerHandler(m.ID(), RebroadcastInterestSetTimerID, m.handleRebroadcastInterestSetsTimer)

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
	m.babel.RegisterTimerHandler(
		m.ID(),
		ExportNeighInterestSetMetricsTimerID,
		m.handleExportNeighInterestSetMetricsTimer,
	)

	// TREE AGG FUNCS

	m.babel.RegisterRequestHandler(m.ID(), AddTreeAggregationFuncReqID, m.handleAddTreeAggregationFuncRequest)

	m.babel.RegisterTimerHandler(m.ID(), RebroadcastTreeAggregationFuncsTimerID, m.handleRebroadcastTreeInterestSetsTimer)

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
	m.babel.RegisterTimerHandler(
		m.ID(),
		ExportTreeAggregationFuncTimerID,
		m.handleExportTreeAggregationFuncTimer,
	)

	// GLOBAL AGG FUNCS

	m.babel.RegisterRequestHandler(m.ID(), AddGlobalAggregationFuncReqID, m.handleAddGlobalAggFuncRequest)

	m.babel.RegisterTimerHandler(m.ID(), RebroadcastGlobalAggregationFuncsTimerID, m.handleRebroadcastGlobalInterestSetsTimer)

	m.babel.RegisterTimerHandler(
		m.ID(),
		ExportGlobalAggregationFuncTimerID,
		m.handleExportGlobalAggFuncFuncTimer,
	)

	m.babel.RegisterMessageHandler(
		m.ID(),
		NewInstallGlobalAggFuncMessage(nil),
		m.handleInstallGlobalAggFuncMessage,
	)

	m.babel.RegisterMessageHandler(
		m.ID(),
		NewPropagateGlobalAggFuncMetricsMessage(0, nil),
		m.handlePropagateGlobalAggFuncMetricsMessage,
	)

	// CLEANUP (SAME FOR ALL)

	m.babel.RegisterTimerHandler(m.ID(), CleanupInsterestSetsTimerID, m.handleCleanupInterestSetsTimer)
}

func (m *Monitor) Start() {
	m.babel.RegisterPeriodicTimer(
		m.ID(),
		NewRebroadcastInterestSetsTimer(RebroadcastNeighInterestSetsTimerDuration),
		false,
	)

	m.babel.RegisterPeriodicTimer(
		m.ID(),
		NewBroadcastTreeAggregationFuncsTimer(RebroadcastTreeAggFuncTimerDuration),
		false,
	)

	m.babel.RegisterPeriodicTimer(
		m.ID(),
		NewBroadcastGlobalAggregationFuncsTimer(RebroadcastGlobalAggFuncTimerDuration),
		false,
	)

	m.babel.RegisterPeriodicTimer(
		m.ID(),
		NewCleanupInterestSetsTimer(CleanupInterestSetTimerDuration),
		false,
	)
}

// TIMER HANDLERS

func (m *Monitor) handleCleanupInterestSetsTimer(t timer.Timer) {
	m.cleanupNeighInterestSets()
	m.cleanupTreeInterestSets()
	m.cleanupGlobalAggFuncs()
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
	m.handleNodeUpTreeAggFunc(nodeUpNotification.PeerUp)

}

func (m *Monitor) handleNodeDown(n notification.Notification) {
	nodeDownNotification := n.(membershipProtocol.NodeDownNotification)
	nodeDown := nodeDownNotification.PeerDown
	m.currView = nodeDownNotification.InView
	m.logger.Infof("Node down: %s", nodeDownNotification.PeerDown.String())
	m.logger.Infof("Curr View: %+v", m.currView)

	m.handleNodeDownNeighInterestSet(nodeDown)
	m.handleNodeDownTreeAggFunc(nodeDown)
	m.handleNodeDownGlobalAggFunc(nodeDown)
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
