package monitoring_proto

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon/internal/membership/membership_protocol"
	"github.com/nm-morais/demmon/internal/monitoring/metrics_engine"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const (
	MonitorProtoID = 6000
	name           = "Monitor"
)

type localNeighInterestSet struct {
	nrRetries   int
	interestSet body_types.NeighborhoodInterestSet
}

type remoteNeighInterestSet struct {
	nrRetries   int
	sender      peer.Peer
	interestSet body_types.NeighborhoodInterestSet
}

type Monitor struct {
	currId                  membership_protocol.PeerIDChain
	remoteNeighInterestSets map[uint64]remoteNeighInterestSet
	localNeighInterestSets  map[uint64]localNeighInterestSet
	interestSetTimerIds     map[uint64]timer.ID
	currView                membership_protocol.InView
	logger                  *logrus.Logger
	babel                   protocolManager.ProtocolManager
	me                      *metrics_engine.MetricsEngine
	tsdb                    *tsdb.TSDB
}

func New(babel protocolManager.ProtocolManager, db *tsdb.TSDB, me *metrics_engine.MetricsEngine) *Monitor {
	return &Monitor{
		tsdb:                    db,
		me:                      me,
		currId:                  make(membership_protocol.PeerIDChain, 0),
		interestSetTimerIds:     make(map[uint64]uint16),
		currView:                membership_protocol.InView{},
		localNeighInterestSets:  make(map[uint64]localNeighInterestSet),
		remoteNeighInterestSets: make(map[uint64]remoteNeighInterestSet),
		babel:                   babel,
		logger:                  logs.NewLogger(name),
	}
}

// BOILERPLATE

func (m *Monitor) MessageDelivered(message message.Message, peer peer.Peer) {
}

func (m *Monitor) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
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

func (m *Monitor) Init() {

	// REPLY HANDLERS
	m.babel.RegisterNotificationHandler(m.ID(), membership_protocol.NodeUpNotification{}, m.handleNodeUp)
	m.babel.RegisterNotificationHandler(m.ID(), membership_protocol.NodeDownNotification{}, m.handleNodeDown)
	m.babel.RegisterNotificationHandler(m.ID(), membership_protocol.IDChangeNotification{}, m.handlePeerIdChange)

	// REQUEST HANDLERS
	m.babel.RegisterRequestHandler(m.ID(), AddNeighborhoodInterestSetReqId, m.handleAddNeighInterestSet)
	m.babel.RegisterRequestHandler(m.ID(), RemoveNeighborhoodInterestSetReqId, m.handleRemoveNeighInterestSet)

	// MESSAGE HANDLERS
	m.babel.RegisterMessageHandler(m.ID(), NewInstallNeighInterestSetMessage(nil), m.handleInstallNeighInterestSetMessage)
	m.babel.RegisterMessageHandler(m.ID(), NewPropagateInterestSetMetricsMessage(0, nil), m.handlePropagateNeighInterestSetMetricsMessage)

	// TIMER HANDLERS
	m.babel.RegisterTimerHandler(m.ID(), ExportNeighInterestSetMetricsTimerID, m.handleExportNeighInterestSetMetricsTimer)
	m.babel.RegisterTimerHandler(m.ID(), RebroadcastInterestSetTimerID, m.handleRebroadcastInterestSetTimer)
}

func (m *Monitor) Start() {

}

// TIMER HANDLERS

func (m *Monitor) handleExportNeighInterestSetMetricsTimer(t timer.Timer) {
	tConverted := t.(*exportNeighInterestSetMetricsTimer)
	interestSetId := tConverted.InterestSetId
	interestSet, ok := m.localNeighInterestSets[interestSetId]
	if ok {
		query := interestSet.interestSet.Query
		result, err := m.me.MakeQuery(query.Expression, query.Timeout)
		if err != nil {
			interestSet.nrRetries++
			m.localNeighInterestSets[interestSetId] = interestSet
			m.logger.Errorf("Local neigh interest set query failed to process with err %s (%d/%d)", err, interestSet.nrRetries, interestSet.interestSet.MaxRetries)
			if interestSet.nrRetries >= interestSet.interestSet.MaxRetries {
				return // abort timer
			}
		}
		for _, ts := range result {
			tags := ts.Tags()
			if tags == nil {
				tags = make(map[string]string)
			}
			tags["host"] = m.babel.SelfPeer().IP().String()
			lastPt := ts.Last()
			m.tsdb.AddMetric(interestSet.interestSet.OutputBucketOpts.Name, tags, lastPt.Fields, lastPt.TS)
		}
		m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(interestSet.interestSet.Query.Timeout, interestSetId))
		return
	}

	remoteInterestSet, ok := m.remoteNeighInterestSets[interestSetId]
	if ok {
		query := remoteInterestSet.interestSet.Query
		result, err := m.me.MakeQuery(query.Expression, query.Timeout)
		if err != nil {
			interestSet.nrRetries++
			m.localNeighInterestSets[interestSetId] = interestSet
			m.logger.Errorf("Local neigh interest set query failed to process with err %s (%d/%d)", err, interestSet.nrRetries, interestSet.interestSet.MaxRetries)
			if remoteInterestSet.nrRetries >= remoteInterestSet.interestSet.MaxRetries {
				return // abort timer
			}
		}
		target := remoteInterestSet.sender
		if !m.isPeerInView(target) {
			panic("peer to export remote neigh interest set metrics is not in view")
		}
		for _, ts := range result {
			tags := ts.Tags()
			if tags == nil {
				tags = make(map[string]string)
			}
			tags["host"] = m.babel.SelfPeer().IP().String()
		}
		toSendMsg := NewPropagateInterestSetMetricsMessage(interestSetId, result)
		m.babel.SendMessage(toSendMsg, remoteInterestSet.sender, m.ID(), m.ID())
		m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(interestSet.interestSet.Query.Timeout, interestSetId))
		return
	}
}

func (m *Monitor) handleRebroadcastInterestSetTimer(t timer.Timer) {
	tConverted := t.(*exportNeighInterestSetMetricsTimer)
	interestSetId := tConverted.InterestSetId
	interestSet, ok := m.localNeighInterestSets[interestSetId]
	if !ok {
		return
	}
	toSend := NewInstallNeighInterestSetMessage(map[uint64]body_types.NeighborhoodInterestSet{interestSetId: interestSet.interestSet})
	m.broadcastToAllNeighbors(toSend)
}

// MESSAGE HANDLERS

func (m *Monitor) handleInstallNeighInterestSetMessage(sender peer.Peer, msg message.Message) {
	installNeighIntSetMsg := msg.(installNeighInterestSetMsg)
	if !m.isPeerInView(sender) {
		panic("received install neigh interest set from peer not in my view")
	}
	for interestSetId, interestSet := range installNeighIntSetMsg.InterestSets {
		// TODO check if already present???
		if interestSet.TTL > 0 {
			interestSet.TTL = interestSet.TTL - 1
			m.broadcastMessage(installNeighIntSetMsg, sender)
		}
		interestSetId := interestSetId
		interestSet := interestSet
		m.remoteNeighInterestSets[interestSetId] = remoteNeighInterestSet{
			sender:      sender,
			interestSet: interestSet,
		}
	}
}

func (m *Monitor) handlePropagateNeighInterestSetMetricsMessage(sender peer.Peer, msg message.Message) {
	msgConverted := msg.(propagateInterestSetMetricsMsg)
	interestSetId := msgConverted.InterestSetId
	remoteInterestSet, ok := m.remoteNeighInterestSets[interestSetId]
	if ok {
		if !m.isPeerInView(remoteInterestSet.sender) {
			panic("received interest set propagation message but target is not in view")
		}
		m.babel.SendMessage(msgConverted, remoteInterestSet.sender, m.ID(), m.ID())
		return
	}
	localInterestSet, ok := m.localNeighInterestSets[interestSetId]
	if ok {
		toAdd := msgConverted.Metrics
		for _, ts := range toAdd {
			m.tsdb.AddMetric(localInterestSet.interestSet.OutputBucketOpts.Name, ts.Tags(), ts.Last().Fields, ts.Last().TS)
		}
	}
}

// REQUEST HANDLERS

func (m *Monitor) handleAddNeighInterestSet(req request.Request) request.Reply {
	addNeighInterestSetReq := req.(AddNeighborhoodInterestSetReq)
	interestSetId := addNeighInterestSetReq.InterestSetId
	interestSet := addNeighInterestSetReq.InterestSet
	m.localNeighInterestSets[interestSetId] = localNeighInterestSet{
		nrRetries:   0,
		interestSet: interestSet,
	}
	frequency := addNeighInterestSetReq.InterestSet.OutputBucketOpts.Granularity.Granularity
	m.broadcastToAllNeighbors(NewInstallNeighInterestSetMessage(map[uint64]body_types.NeighborhoodInterestSet{interestSetId: interestSet}))
	m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(frequency, interestSetId))
	return nil
}

func (m *Monitor) handleRemoveNeighInterestSet(req request.Request) request.Reply {
	remNeighInterestSetReq := req.(RemoveNeighborhoodInterestSetReq)
	delete(m.localNeighInterestSets, remNeighInterestSetReq.InterestSetId)
	return nil
}

// NOTIFICATION HANDLERS

func (m *Monitor) handlePeerIdChange(n notification.Notification) {
	nodeUpNotification := n.(membership_protocol.IDChangeNotification)
	m.currId = nodeUpNotification.NewId
}

func (m *Monitor) handleNodeUp(n notification.Notification) {
	nodeUpNotification := n.(membership_protocol.NodeUpNotification)
	nodeUp := nodeUpNotification.PeerUp
	_, isChildren, _ := m.getPeerRelationshipType(nodeUp)

	neighIntSetstoSend := make(map[uint64]body_types.NeighborhoodInterestSet, len(m.localNeighInterestSets)+len(m.remoteNeighInterestSets))
	for isd, is := range m.localNeighInterestSets {
		neighIntSetstoSend[isd] = is.interestSet
	}
	toSend := NewInstallNeighInterestSetMessage(neighIntSetstoSend)

	if isChildren { // add remote interest sets to send to children
		for interestSetId, remoteIntSet := range m.remoteNeighInterestSets {
			if remoteIntSet.interestSet.TTL > 0 {
				toSend.InterestSets[interestSetId] = body_types.NeighborhoodInterestSet{
					Query:            remoteIntSet.interestSet.Query,
					OutputBucketOpts: remoteIntSet.interestSet.OutputBucketOpts,
					TTL:              remoteIntSet.interestSet.TTL - 1,
				}
			}
		}
	}

	m.babel.SendMessage(toSend, nodeUp, m.ID(), m.ID())
	m.currView = nodeUpNotification.InView
}

func (m *Monitor) handleNodeDown(n notification.Notification) {
	nodeDownNotification := n.(membership_protocol.NodeDownNotification)
	nodeDown := nodeDownNotification.PeerDown
	m.currView = nodeDownNotification.InView

	isSibling, isChildren, isParent := m.getPeerRelationshipType(nodeDown)

	if isSibling {
		m.currView = nodeDownNotification.InView
		return
	}

	if isChildren {
		m.currView = nodeDownNotification.InView
		return
	}

	if isParent {
		m.currView = nodeDownNotification.InView
		return
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

func (m *Monitor) broadcastToAllNeighbors(msg message.Message) {
	for _, s := range m.currView.Siblings {
		m.babel.SendMessage(msg, s, m.ID(), m.ID())
	}

	for _, s := range m.currView.Children {
		m.babel.SendMessage(msg, s, m.ID(), m.ID())
	}
}

func (m *Monitor) getPeerRelationshipType(p peer.Peer) (isSibling, isChildren, isParent bool) {
	for _, sibling := range m.currView.Siblings {
		if peer.PeersEqual(sibling, p) {
			isSibling = true
		}
	}

	for _, children := range m.currView.Children {
		if peer.PeersEqual(children, p) {
			isChildren = true
		}
	}

	if peer.PeersEqual(p, m.currView.Parent) {
		isParent = true
	}
	return
}

func (m *Monitor) broadcastMessage(msg message.Message, sender peer.Peer) {
	isSibling, isChildren, isParent := m.getPeerRelationshipType(sender)

	if isSibling {
		for _, c := range m.currView.Children {
			m.babel.SendMessage(msg, c, m.ID(), m.ID())
		}
		return
	}

	if isChildren {
		for _, s := range m.currView.Siblings {
			m.babel.SendMessage(msg, s, m.ID(), m.ID())
		}

		if m.currView.Parent != nil {
			m.babel.SendMessage(msg, m.currView.Parent, m.ID(), m.ID())
		}
		return
	}

	if isParent {
		for _, c := range m.currView.Children {
			m.babel.SendMessage(msg, c, m.ID(), m.ID())
		}
		return
	}
}

func (m *Monitor) DialFailed(p peer.Peer) {
}

func (m *Monitor) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (m *Monitor) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (m *Monitor) OutConnDown(peer peer.Peer) {}

func (m *Monitor) AddNeighborhoodInterestSetReq(key uint64, interestSet body_types.NeighborhoodInterestSet) {
	m.babel.SendRequest(NewAddNeighborhoodInterestSetReq(key, interestSet), m.ID(), m.ID())
}
