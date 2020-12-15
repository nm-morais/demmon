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
	currID                  membershipProtocol.PeerIDChain
	remoteNeighInterestSets map[uint64]remoteNeighInterestSet
	localNeighInterestSets  map[uint64]localNeighInterestSet
	interestSetTimerIds     map[uint64]timer.ID
	currView                membershipProtocol.InView
	logger                  *logrus.Logger
	babel                   protocolManager.ProtocolManager
	me                      *engine.MetricsEngine
	tsdb                    *tsdb.TSDB
}

const (
	CleanupInterestSetTimerDuration    = 5 * time.Second
	BroadcastInterestSetsTimerDuration = 5 * time.Second
)

func New(babel protocolManager.ProtocolManager, db *tsdb.TSDB, me *engine.MetricsEngine) *Monitor {
	return &Monitor{
		tsdb:                    db,
		me:                      me,
		currID:                  make(membershipProtocol.PeerIDChain, 0),
		interestSetTimerIds:     make(map[uint64]uint16),
		currView:                membershipProtocol.InView{},
		localNeighInterestSets:  make(map[uint64]localNeighInterestSet),
		remoteNeighInterestSets: make(map[uint64]remoteNeighInterestSet),
		babel:                   babel,
		logger:                  logs.NewLogger(name),
	}
}

// BOILERPLATE

func (m *Monitor) MessageDelivered(msg message.Message, p peer.Peer) {
}

func (m *Monitor) MessageDeliveryErr(msg message.Message, peer peer.Peer, err errors.Error) {
	m.logger.Errorf("Message %+v failed to deliver to: %s", msg, peer)
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

	// MESSAGE HANDLERS
	m.babel.RegisterMessageHandler(
		m.ID(),
		NewInstallNeighInterestSetMessage(nil),
		m.handleInstallNeighInterestSetMessage,
	)
	m.babel.RegisterMessageHandler(
		m.ID(),
		NewPropagateInterestSetMetricsMessage(0, nil),
		m.handlePropagateNeighInterestSetMetricsMessage,
	)

	// TIMER HANDLERS
	m.babel.RegisterTimerHandler(
		m.ID(),
		ExportNeighInterestSetMetricsTimerID,
		m.handleExportNeighInterestSetMetricsTimer,
	)
	m.babel.RegisterTimerHandler(
		m.ID(),
		ExportLocalNeighInterestSetMetricsTimerID,
		m.handleExportLocalNeighInterestSetMetricsTimer,
	)
	m.babel.RegisterTimerHandler(m.ID(), RebroadcastInterestSetTimerID, m.handleBroadcastInterestSetsTimer)
	m.babel.RegisterTimerHandler(m.ID(), CleanupInsterestSetsTimerID, m.handleCleanupInterestSetsTimer)
}

func (m *Monitor) Start() {
	m.babel.RegisterTimer(
		m.ID(),
		NewRebroadcastInterestSetsTimer(BroadcastInterestSetsTimerDuration),
	)

	m.babel.RegisterTimer(
		m.ID(),
		NewCleanupInterestSetsTimer(CleanupInterestSetTimerDuration),
	)
}

// REQUEST CREATORS

func (m *Monitor) AddNeighborhoodInterestSetReq(key uint64, interestSet body_types.NeighborhoodInterestSet) {
	m.babel.SendRequest(NewAddNeighborhoodInterestSetReq(key, interestSet), m.ID(), m.ID())
}

// TIMER HANDLERS

func (m *Monitor) handleCleanupInterestSetsTimer(t timer.Timer) {
	m.babel.RegisterTimer(m.ID(), NewCleanupInterestSetsTimer(CleanupInterestSetTimerDuration))
	for isID, is := range m.remoteNeighInterestSets {
		if !m.isPeerInView(is.sender) {
			delete(m.remoteNeighInterestSets, isID)
			m.logger.Errorf(
				"Removing interest set %d because peer %s is not in view",
				isID,
				is.sender.String(),
			)
		}
	}
}

func (m *Monitor) handleBroadcastInterestSetsTimer(t timer.Timer) {
	m.babel.RegisterTimer(
		m.ID(),
		NewRebroadcastInterestSetsTimer(BroadcastInterestSetsTimerDuration),
	)
	neighIntSetstoSend := make(map[uint64]body_types.NeighborhoodInterestSet)

	for isd, is := range m.localNeighInterestSets {
		if is.interestSet.TTL > 0 {
			neighIntSetstoSend[isd] = body_types.NeighborhoodInterestSet{
				MaxRetries:       is.interestSet.MaxRetries,
				Query:            is.interestSet.Query,
				OutputBucketOpts: is.interestSet.OutputBucketOpts,
				TTL:              is.interestSet.TTL - 1,
			}
		}
	}
	for interestSetID, remoteIntSet := range m.remoteNeighInterestSets {
		if remoteIntSet.interestSet.TTL > 0 {
			neighIntSetstoSend[interestSetID] = body_types.NeighborhoodInterestSet{
				MaxRetries:       remoteIntSet.interestSet.MaxRetries,
				Query:            remoteIntSet.interestSet.Query,
				OutputBucketOpts: remoteIntSet.interestSet.OutputBucketOpts,
				TTL:              remoteIntSet.interestSet.TTL - 1,
			}
		}
	}
	toSend := NewInstallNeighInterestSetMessage(neighIntSetstoSend)
	m.broadcastToAllNeighbors(toSend)
}

func (m *Monitor) handleExportNeighInterestSetMetricsTimer(t timer.Timer) {
	tConverted := t.(*exportNeighInterestSetMetricsTimer)
	interestSetID := tConverted.InterestSetID
	remoteInterestSet, ok := m.remoteNeighInterestSets[interestSetID]

	if !ok {
		m.logger.Warnf("Canceling export timer for remote interest set %d", interestSetID)
		return
	}

	m.logger.Infof(
		"Exporting metrics for remote interest set %d: %s",
		interestSetID,
		remoteInterestSet.interestSet.OutputBucketOpts.Name,
	)

	query := remoteInterestSet.interestSet.Query
	result, err := m.me.MakeQuery(query.Expression, query.Timeout)

	if err != nil {
		remoteInterestSet.nrRetries++
		m.remoteNeighInterestSets[interestSetID] = remoteInterestSet
		m.logger.Errorf(
			"Remote neigh interest set query failed to process with err %s (%d/%d)",
			err,
			remoteInterestSet.nrRetries,
			remoteInterestSet.interestSet.MaxRetries,
		)

		if remoteInterestSet.nrRetries >= remoteInterestSet.interestSet.MaxRetries {
			m.logger.Errorf("Aborting export timer for remote interest set %d", interestSetID)
			return // abort timer
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportNeighInterestSetMetricsTimer(
				remoteInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
		return
	}

	if !m.isPeerInView(remoteInterestSet.sender) {
		m.logger.Errorf(
			"Returning because peer is not in view: %s",
			remoteInterestSet.sender.String(),
		)
		return
	}

	m.logger.Infof(
		"Remote neigh interest set query result: (%+v)",
		result,
	)

	for _, ts := range result {
		ts.SetTag("host", m.babel.SelfPeer().IP().String())
	}

	if remoteInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity == 0 {
		panic("granularity is 0")
	}

	toSendMsg := NewPropagateInterestSetMetricsMessage(interestSetID, result)
	m.babel.SendMessage(toSendMsg, remoteInterestSet.sender, m.ID(), m.ID())
	m.babel.RegisterTimer(
		m.ID(),
		NewExportNeighInterestSetMetricsTimer(
			remoteInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity,
			interestSetID,
		),
	)
}

func (m *Monitor) handleExportLocalNeighInterestSetMetricsTimer(t timer.Timer) {
	tConverted := t.(*exportLocalNeighInterestSetMetricsTimer)
	interestSetID := tConverted.InterestSetID
	localInterestSet, ok := m.localNeighInterestSets[interestSetID]
	if !ok {
		m.logger.Warnf("Canceling export timer for local interest set %d", interestSetID)
		return
	}

	m.logger.Infof(
		"Exporting metrics for local interest set %d: %s",
		interestSetID,
		localInterestSet.interestSet.OutputBucketOpts.Name,
	)
	query := localInterestSet.interestSet.Query
	result, err := m.me.MakeQuery(query.Expression, query.Timeout)

	if err != nil {
		localInterestSet.nrRetries++
		m.localNeighInterestSets[interestSetID] = localInterestSet
		m.logger.Errorf(
			"Local neigh interest set query failed to process with err %s (%d/%d)",
			err,
			localInterestSet.nrRetries,
			localInterestSet.interestSet.MaxRetries,
		)

		if localInterestSet.nrRetries >= localInterestSet.interestSet.MaxRetries {
			m.logger.Errorf("Aborting export timer for interest set %d", interestSetID)
			return // abort timer
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportLocalNeighInterestSetMetricsTimer(
				localInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
		return
	}

	m.logger.Infof(
		"Local interest set query result: (%+v)",
		result,
	)

	for _, ts := range result {
		ts.SetTag("host", m.babel.SelfPeer().IP().String())
		allPts := ts.All()
		if len(allPts) == 0 {
			m.logger.Error("Timeseries result is empty")
		}
		for _, pt := range allPts {
			err := m.tsdb.AddMetric(
				localInterestSet.interestSet.OutputBucketOpts.Name,
				ts.Tags(),
				pt.Value(),
				pt.TS(),
			)
			if err != nil {
				m.logger.Panic(err)
			}
		}
	}

	if localInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity == 0 {
		panic("granularity is 0")
	}

	m.logger.Infof(
		"Setting timer for interest set %d to %+v from now",
		interestSetID,
		localInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity,
	)
	m.babel.RegisterTimer(
		m.ID(),
		NewExportLocalNeighInterestSetMetricsTimer(
			localInterestSet.interestSet.OutputBucketOpts.Granularity.Granularity,
			interestSetID,
		),
	)
	return
}

// MESSAGE HANDLERS

func (m *Monitor) handleInstallNeighInterestSetMessage(sender peer.Peer, msg message.Message) {
	installNeighIntSetMsg := msg.(InstallNeighInterestSetMsg)

	if !m.isPeerInView(sender) {
		m.logger.Warn("received install neigh interest set from peer not in my view")
		return
	}

	m.logger.Infof(
		"received message to install neigh interest sets from %s (%+v)",
		sender.String(),
		installNeighIntSetMsg,
	)

	for interestSetID, interestSet := range installNeighIntSetMsg.InterestSets {
		m.logger.Infof("installing neigh interest set %d: %+v", interestSetID, interestSet)
		_, alreadyExists := m.remoteNeighInterestSets[interestSetID]

		if alreadyExists {
			m.logger.Info("Neigh interest set already present")
			continue
		}
		_, alreadyExists = m.localNeighInterestSets[interestSetID]

		if alreadyExists {
			m.logger.Info("Neigh interest set already present (is local)")
			continue
		}

		m.remoteNeighInterestSets[interestSetID] = remoteNeighInterestSet{
			nrRetries:   0,
			sender:      sender,
			interestSet: interestSet,
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportNeighInterestSetMetricsTimer(
				interestSet.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
	}
}

func (m *Monitor) handlePropagateNeighInterestSetMetricsMessage(sender peer.Peer, msg message.Message) {
	msgConverted := msg.(PropagateInterestSetMetricsMsg)
	interestSetID := msgConverted.InterestSetID
	localInterestSet, ok := m.localNeighInterestSets[interestSetID]
	if ok {
		m.logger.Infof(
			"received propagation of metric values for local interest set %d: %s from %s",
			interestSetID,
			localInterestSet.interestSet.OutputBucketOpts.Name,
			sender.String(),
		)
		m.logger.Infof("metrics in propagation message: %+v", msgConverted.Metrics)
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

	remoteInterestSet, ok := m.remoteNeighInterestSets[interestSetID]
	if ok {
		m.logger.Infof(
			"received propagation of metric values for neigh interest set %d: %s from %s",
			interestSetID,
			remoteInterestSet.interestSet.OutputBucketOpts.Name,
			sender.String(),
		)
		m.logger.Infof("metrics in propagation message: %+v", msgConverted.Metrics)

		if !m.isPeerInView(remoteInterestSet.sender) {
			m.logger.Warnf("received interest set propagation message but target (%s) is not in view", remoteInterestSet.sender)
		}

		m.babel.SendMessage(msgConverted, remoteInterestSet.sender, m.ID(), m.ID())
		return
	}
}

// REQUEST HANDLERS

func (m *Monitor) handleAddNeighInterestSetRequest(req request.Request) request.Reply {
	addNeighInterestSetReq := req.(AddNeighborhoodInterestSetReq)
	interestSetID := addNeighInterestSetReq.InterestSetID
	interestSet := addNeighInterestSetReq.InterestSet
	m.localNeighInterestSets[interestSetID] = localNeighInterestSet{
		nrRetries:   0,
		interestSet: interestSet,
	}
	frequency := interestSet.OutputBucketOpts.Granularity.Granularity
	m.logger.Infof("Installing local insterest set: %+v", interestSet)
	m.babel.RegisterTimer(m.ID(), NewExportLocalNeighInterestSetMetricsTimer(frequency, interestSetID))
	return nil
}

func (m *Monitor) handleRemoveNeighInterestSetRequest(req request.Request) request.Reply {
	remNeighInterestSetReq := req.(RemoveNeighborhoodInterestSetReq)
	delete(m.localNeighInterestSets, remNeighInterestSetReq.InterestSetID)
	return nil
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
	for intSetID, intSet := range m.remoteNeighInterestSets {
		if peer.PeersEqual(intSet.sender, nodeDown) {
			// m.tsdb.DeleteBucket(intSet.interestSet.OutputBucketOpts.Name, map[string]string{"host": m.babel.SelfPeer().IP().String()})
			delete(m.remoteNeighInterestSets, intSetID)
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

func (m *Monitor) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {
	return false
}

func (m *Monitor) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	return false
}

func (m *Monitor) OutConnDown(p peer.Peer) {}
