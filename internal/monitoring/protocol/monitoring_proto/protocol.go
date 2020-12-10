package monitoring_proto

import (
	"time"

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
	name           = "monitor_proto"
)

type localNeighInterestSet struct {
	nrRetries   int
	queryHash   []byte
	interestSet body_types.NeighborhoodInterestSet
}

type remoteNeighInterestSet struct {
	nrRetries   int
	sender      peer.Peer
	queryHash   []byte
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
	m.logger.Errorf("Message %+v failed to deliver to: %s", message, peer)
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
	m.babel.RegisterRequestHandler(m.ID(), AddNeighborhoodInterestSetReqId, m.handleAddNeighInterestSetRequest)
	m.babel.RegisterRequestHandler(m.ID(), RemoveNeighborhoodInterestSetReqId, m.handleRemoveNeighInterestSetRequest)

	// MESSAGE HANDLERS
	m.babel.RegisterMessageHandler(m.ID(), NewInstallNeighInterestSetMessage(nil), m.handleInstallNeighInterestSetMessage)
	m.babel.RegisterMessageHandler(m.ID(), NewPropagateInterestSetMetricsMessage(0, nil), m.handlePropagateNeighInterestSetMetricsMessage)

	// TIMER HANDLERS
	m.babel.RegisterTimerHandler(m.ID(), ExportNeighInterestSetMetricsTimerID, m.handleExportNeighInterestSetMetricsTimer)
	m.babel.RegisterTimerHandler(m.ID(), RebroadcastInterestSetTimerID, m.handleRebroadcastInterestSetTimer)
	m.babel.RegisterTimerHandler(m.ID(), CheckInterestSetPeerInViewTimerID, m.handleCheckInterestSetPeerInViewTimer)

}

func (m *Monitor) Start() {

}

// REQUEST CREATORS

func (m *Monitor) AddNeighborhoodInterestSetReq(key uint64, interestSet body_types.NeighborhoodInterestSet) {
	m.babel.SendRequest(NewAddNeighborhoodInterestSetReq(key, interestSet), m.ID(), m.ID())
}

// TIMER HANDLERS

func (m *Monitor) handleExportNeighInterestSetMetricsTimer(t timer.Timer) {
	tConverted := t.(*exportNeighInterestSetMetricsTimer)
	interestSetId := tConverted.InterestSetId
	localInterestSet, ok := m.localNeighInterestSets[interestSetId]
	if ok {
		m.logger.Infof("Exporting metrics for local interest set %d: %s", interestSetId, localInterestSet.interestSet.OutputBucketOpts.Name)
		query := localInterestSet.interestSet.Query
		result, err := m.me.MakeQuery(query.Expression, query.Timeout)

		if err != nil {
			localInterestSet.nrRetries++
			m.localNeighInterestSets[interestSetId] = localInterestSet
			m.logger.Errorf("Local neigh interest set query failed to process with err %s (%d/%d)", err, localInterestSet.nrRetries, localInterestSet.interestSet.MaxRetries)
			if localInterestSet.nrRetries >= localInterestSet.interestSet.MaxRetries {
				m.logger.Errorf("Aborting export timer for interest set %d", interestSetId)
				return // abort timer
			}
		}

		for _, ts := range result {
			ts.SetTag("host", m.babel.SelfPeer().IP().String())
			allPts := ts.All()
			if len(allPts) == 0 {
				m.logger.Error("Timeseries result is empty")
			}
			for _, pt := range allPts {
				err := m.tsdb.AddMetric(localInterestSet.interestSet.OutputBucketOpts.Name, ts.Tags(), pt.Value(), pt.TS())
				if err != nil {
					m.logger.Panic(err)
				}
			}
		}

		if localInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity == 0 {
			panic("granularity is 0")
		}

		m.logger.Infof("Setting timer for interest set %d to %+v from now", interestSetId, localInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity)
		m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(localInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity, interestSetId))
		return
	}

	remoteInterestSet, ok := m.remoteNeighInterestSets[interestSetId]
	if ok {
		m.logger.Infof("Exporting metrics for remote interest set %d: %s", interestSetId, remoteInterestSet.interestSet.OutputBucketOpts.Name)
		query := remoteInterestSet.interestSet.Query
		result, err := m.me.MakeQuery(query.Expression, query.Timeout)
		if err != nil {
			remoteInterestSet.nrRetries++
			m.remoteNeighInterestSets[interestSetId] = remoteInterestSet
			m.logger.Errorf("Remote neigh interest set query failed to process with err %s (%d/%d)", err, remoteInterestSet.nrRetries, remoteInterestSet.interestSet.MaxRetries)
			if remoteInterestSet.nrRetries >= remoteInterestSet.interestSet.MaxRetries {
				m.logger.Errorf("Aborting export timer for remote interest set %d", interestSetId)
				return // abort timer
			}
		}
		target := remoteInterestSet.sender

		if !m.isPeerInView(target) {
			return
		}

		for _, ts := range result {
			ts.SetTag("host", m.babel.SelfPeer().IP().String())
		}

		if remoteInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity == 0 {
			panic("granularity is 0")
		}

		toSendMsg := NewPropagateInterestSetMetricsMessage(interestSetId, result)
		m.babel.SendMessage(toSendMsg, remoteInterestSet.sender, m.ID(), m.ID())
		m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(remoteInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity, interestSetId))
		return
	}
	m.logger.Warn("ExportNeighInterestSetMetricsTimer not rescheduled")
}

func (m *Monitor) handleRebroadcastInterestSetTimer(t timer.Timer) {
	tConverted := t.(*rebroadcastInterestSetTimer)
	interestSetId := tConverted.InterestSetId
	interestSet, ok := m.localNeighInterestSets[interestSetId]
	if !ok {
		return
	}
	toSend := NewInstallNeighInterestSetMessage(map[uint64]body_types.NeighborhoodInterestSet{interestSetId: interestSet.interestSet})
	m.broadcastToAllNeighbors(toSend)
	m.babel.RegisterTimer(m.ID(), NewRebroadcastInterestSetTimer(5*time.Second, interestSetId)) // TODO export to variable
}

func (m *Monitor) handleCheckInterestSetPeerInViewTimer(t timer.Timer) {
	tConverted := t.(*checkInterestSetPeerInViewTimer)
	interestSetId := tConverted.InterestSetId
	interestSet, ok := m.remoteNeighInterestSets[interestSetId]
	if !ok {
		return
	}

	if !m.isPeerInView(interestSet.sender) {
		delete(m.remoteNeighInterestSets, interestSetId)
		m.logger.Errorf("Removing interest set %d because peer %s is not in view", interestSetId, interestSet.sender.String())
	}
}

// MESSAGE HANDLERS

func (m *Monitor) handleInstallNeighInterestSetMessage(sender peer.Peer, msg message.Message) {
	installNeighIntSetMsg := msg.(installNeighInterestSetMsg)
	m.logger.Infof("received message to install neigh interest sets from %s (%+v)", sender.String(), installNeighIntSetMsg)
	for interestSetId, interestSet := range installNeighIntSetMsg.InterestSets {
		m.logger.Infof("installing neigh interest set %d: %+v", interestSetId, interestSet)
		// TODO check if already present???
		_, alreadyExists := m.remoteNeighInterestSets[interestSetId]
		if alreadyExists {
			m.logger.Warn("Neigh interest set already present")
			continue
		}

		if interestSet.TTL > 0 {
			interestSet.TTL = interestSet.TTL - 1
			m.broadcastMessage(installNeighIntSetMsg, sender)
		}

		if interestSet.OutputBucketOpts.Granularity.Granularity == 0 {
			panic("Cannot install neigh interest set with granularity 0")
		}

		m.remoteNeighInterestSets[interestSetId] = remoteNeighInterestSet{
			nrRetries:   0,
			sender:      sender,
			interestSet: interestSet,
		}

		if !m.isPeerInView(sender) {
			m.logger.Warn("received install neigh interest set from peer not in my view")
			m.babel.RegisterTimer(m.ID(), NewCheckInterestSetPeerInViewTimer(3*time.Second, interestSetId)) // TODO export config var
			return
		}
		m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(1, interestSetId))
	}
}

func (m *Monitor) handlePropagateNeighInterestSetMetricsMessage(sender peer.Peer, msg message.Message) {
	msgConverted := msg.(propagateInterestSetMetricsMsg)
	interestSetId := msgConverted.InterestSetId
	localInterestSet, ok := m.localNeighInterestSets[interestSetId]
	if ok {
		m.logger.Infof("received propagation of metric values for local interest set %d: %s from %s", interestSetId, localInterestSet.interestSet.OutputBucketOpts.Name, sender.String())
		// m.logger.Infof("adding metrics: %+v", msgConverted.Metrics)
		toAdd := msgConverted.Metrics
		for _, ts := range toAdd {
			for _, pt := range ts.Points {
				err := m.tsdb.AddMetric(localInterestSet.interestSet.OutputBucketOpts.Name, ts.Tags, pt.Fields, pt.TS)
				if err != nil {
					m.logger.Error(err)
				}
			}
		}
		return
	}

	remoteInterestSet, ok := m.remoteNeighInterestSets[interestSetId]
	if ok {
		m.logger.Infof("received propagation of metric values for neigh interest set %d: %s from %s", interestSetId, remoteInterestSet.interestSet.OutputBucketOpts.Name, sender.String())
		m.logger.Infof("values: %+v", msgConverted.Metrics)

		if !m.isPeerInView(remoteInterestSet.sender) {
			panic("received interest set propagation message but target is not in view")
		}
		m.babel.SendMessage(msgConverted, remoteInterestSet.sender, m.ID(), m.ID())
		return
	}

}

// REQUEST HANDLERS

func (m *Monitor) handleAddNeighInterestSetRequest(req request.Request) request.Reply {
	addNeighInterestSetReq := req.(AddNeighborhoodInterestSetReq)
	interestSetId := addNeighInterestSetReq.InterestSetId
	interestSet := addNeighInterestSetReq.InterestSet
	m.localNeighInterestSets[interestSetId] = localNeighInterestSet{
		nrRetries:   0,
		interestSet: interestSet,
	}
	frequency := interestSet.OutputBucketOpts.Granularity.Granularity
	toSend := map[uint64]body_types.NeighborhoodInterestSet{
		interestSetId: interestSet,
	}
	m.broadcastToAllNeighbors(NewInstallNeighInterestSetMessage(toSend))
	m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(frequency, interestSetId))
	m.babel.RegisterTimer(m.ID(), NewRebroadcastInterestSetTimer(5*time.Second, interestSetId)) // TODO export to variable
	return nil
}

func (m *Monitor) handleRemoveNeighInterestSetRequest(req request.Request) request.Reply {
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
	m.currView = nodeUpNotification.InView
	m.logger.Infof("Node up: %s", nodeUpNotification.PeerUp.String())
	m.logger.Infof("Curr View: %+v", m.currView)
	_, isChildren, _ := m.getPeerRelationshipType(nodeUp)

	neighIntSetstoSend := make(map[uint64]body_types.NeighborhoodInterestSet)
	for isd, is := range m.localNeighInterestSets {
		if is.interestSet.TTL > 0 {
			neighIntSetstoSend[isd] = is.interestSet
		}
	}
	if isChildren { // add remote interest sets to send to children
		for interestSetId, remoteIntSet := range m.remoteNeighInterestSets {
			if remoteIntSet.interestSet.TTL > 0 {
				neighIntSetstoSend[interestSetId] = body_types.NeighborhoodInterestSet{
					MaxRetries:       remoteIntSet.interestSet.MaxRetries,
					Query:            remoteIntSet.interestSet.Query,
					OutputBucketOpts: remoteIntSet.interestSet.OutputBucketOpts,
					TTL:              remoteIntSet.interestSet.TTL - 1,
				}
			}
		}
	}
	m.logger.Infof("Sending installNeighRequestMessage with sets: %+v", neighIntSetstoSend)
	toSend := NewInstallNeighInterestSetMessage(neighIntSetstoSend)
	m.babel.SendMessage(toSend, nodeUp, m.ID(), m.ID())
	for intSetId, intSet := range m.remoteNeighInterestSets {
		if peer.PeersEqual(intSet.sender, nodeUp) {
			m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(1, intSetId))
			m.logger.Infof("setting export timer for neigh interest set %d: %s from %s", intSetId, intSet.interestSet.OutputBucketOpts.Name, intSet.sender.String())
		}
	}
}

func (m *Monitor) handleNodeDown(n notification.Notification) {
	nodeDownNotification := n.(membership_protocol.NodeDownNotification)
	nodeDown := nodeDownNotification.PeerDown
	m.currView = nodeDownNotification.InView
	m.logger.Infof("Node down: %s", nodeDownNotification.PeerDown.String())
	m.logger.Infof("Curr View: %+v", m.currView)

	// isSibling, isChildren, isParent := m.getPeerRelationshipType(nodeDown)
	for intSetId, intSet := range m.remoteNeighInterestSets {
		if peer.PeersEqual(intSet.sender, nodeDown) {
			// m.tsdb.DeleteBucket(intSet.interestSet.OutputBucketOpts.Name, map[string]string{"host": m.babel.SelfPeer().IP().String()})
			delete(m.remoteNeighInterestSets, intSetId)
			break
		}
	}

	// if isSibling {
	// 	m.currView = nodeDownNotification.InView
	// 	return
	// }

	// if isChildren {
	// 	m.currView = nodeDownNotification.InView
	// 	return
	// }

	// if isParent {
	// 	m.currView = nodeDownNotification.InView
	// 	return
	// }
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
