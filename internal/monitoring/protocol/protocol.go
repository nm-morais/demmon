package protocol

import (
	"math"
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

	CleanupInterestSetTimerDuration    = 5 * time.Second
	BroadcastInterestSetsTimerDuration = 5 * time.Second
	ExpireNeighInterestSetTimeout      = 3 * BroadcastInterestSetsTimerDuration
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
	InterestSet body_types.InterestSet
}

type Monitor struct {
	currID              membershipProtocol.PeerIDChain
	interestSets        map[uint64]*neighInterestSet
	interestSetTimerIds map[uint64]timer.ID
	currView            membershipProtocol.InView
	logger              *logrus.Logger
	babel               protocolManager.ProtocolManager
	me                  *engine.MetricsEngine
	tsdb                *tsdb.TSDB
}

func New(babel protocolManager.ProtocolManager, db *tsdb.TSDB, me *engine.MetricsEngine) *Monitor {
	return &Monitor{
		tsdb:                db,
		me:                  me,
		currID:              make(membershipProtocol.PeerIDChain, 0),
		interestSetTimerIds: make(map[uint64]uint16),
		interestSets:        make(map[uint64]*neighInterestSet),
		currView:            membershipProtocol.InView{},
		babel:               babel,
		logger:              logs.NewLogger(name),
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

	// MESSAGE HANDLERS
	m.babel.RegisterMessageHandler(
		m.ID(),
		NewInstallNeighInterestSetMessage(nil),
		m.handleInstallNeighInterestSetMessage,
	)
	m.babel.RegisterMessageHandler(
		m.ID(),
		NewPropagateInterestSetMetricsMessage(0, nil, 0),
		m.handlePropagateNeighInterestSetMetricsMessage,
	)

	// TIMER HANDLERS
	m.babel.RegisterTimerHandler(
		m.ID(),
		ExportNeighInterestSetMetricsTimerID,
		m.handleExportNeighInterestSetMetricsTimer,
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
	for isID, is := range m.interestSets {

		for k, sub := range is.subscribers {

			if peer.PeersEqual(sub.p, m.babel.SelfPeer()) {
				continue
			}

			if !m.isPeerInView(sub.p) {
				m.logger.Errorf(
					"Removing peer %s from interest set %d because peer is not in view",
					sub.p.String(),
					isID,
				)
				delete(is.subscribers, k)
				continue
			}

			if time.Since(sub.lastRefresh) > ExpireNeighInterestSetTimeout {
				m.logger.Errorf(
					"Removing peer %s from interest set %d because entry expired",
					sub.p.String(),
					isID,
				)
				delete(is.subscribers, k)
				continue
			}
		}

		if len(is.subscribers) == 0 {
			delete(m.interestSets, isID)
		}
	}
}

func (m *Monitor) handleBroadcastInterestSetsTimer(t timer.Timer) {
	m.babel.RegisterTimer(
		m.ID(),
		NewRebroadcastInterestSetsTimer(BroadcastInterestSetsTimerDuration),
	)
	m.broadcastNeighInterestSetsToSiblings()
	m.broadcastNeighInterestSetsToChildren()
	m.broadcastNeighInterestSetsToParent()
}

func (m *Monitor) broadcastNeighInterestSetsToSiblings() {
	for _, sibling := range m.currView.Siblings {
		neighIntSetstoSend := make(map[uint64]neighInterestSet)

		for isID, is := range m.interestSets {
			maxTTL := -math.MaxInt64

			for _, sub := range is.subscribers {
				if sub.ttl == 0 {
					continue
				}

				isSibling, _, isParent := m.getPeerRelationshipType(sub.p)
				if isSibling || isParent {
					continue
				}

				if maxTTL < sub.ttl {
					maxTTL = sub.ttl
				}
			}

			if maxTTL != -math.MaxInt64 {
				neighIntSetstoSend[isID] = neighInterestSet{
					InterestSet: is.InterestSet,
					TTL:         maxTTL - 1,
				}
			}
		}

		if len(neighIntSetstoSend) > 0 {
			toSend := NewInstallNeighInterestSetMessage(neighIntSetstoSend)
			m.babel.SendMessage(toSend, sibling, m.ID(), m.ID())
		}
	}
}

func (m *Monitor) broadcastNeighInterestSetsToChildren() {
	for _, children := range m.currView.Children {
		neighIntSetstoSend := make(map[uint64]neighInterestSet)

		for isID, is := range m.interestSets {
			maxTTL := -math.MaxInt64
			for _, sub := range is.subscribers {

				if sub.ttl == 0 {
					continue
				}

				_, isChildren, _ := m.getPeerRelationshipType(sub.p)
				if isChildren {
					continue
				}

				if maxTTL < sub.ttl {
					maxTTL = sub.ttl
				}
			}
			if maxTTL != -math.MaxInt64 {
				neighIntSetstoSend[isID] = neighInterestSet{
					InterestSet: is.InterestSet,
					TTL:         maxTTL - 1,
				}
			}
		}
		if len(neighIntSetstoSend) > 0 {
			toSend := NewInstallNeighInterestSetMessage(neighIntSetstoSend)
			m.babel.SendMessage(toSend, children, m.ID(), m.ID())
		}
	}
}

func (m *Monitor) broadcastNeighInterestSetsToParent() {
	neighIntSetstoSend := make(map[uint64]neighInterestSet)

	if m.currView.Parent == nil {
		return
	}

	for isID, is := range m.interestSets {
		maxTTL := -math.MaxInt64

		for _, sub := range is.subscribers {

			if sub.ttl == 0 {
				continue
			}

			isSibling, _, isParent := m.getPeerRelationshipType(sub.p)
			if isSibling || isParent {
				continue
			}

			if maxTTL < sub.ttl {
				maxTTL = sub.ttl
			}
		}

		if maxTTL != -math.MaxInt64 {
			neighIntSetstoSend[isID] = neighInterestSet{
				InterestSet: is.InterestSet,
				TTL:         maxTTL - 1,
			}
		}
	}

	if len(neighIntSetstoSend) > 0 {
		toSend := NewInstallNeighInterestSetMessage(neighIntSetstoSend)
		m.babel.SendMessage(toSend, m.currView.Parent, m.ID(), m.ID())
	}
}

func (m *Monitor) handleExportNeighInterestSetMetricsTimer(t timer.Timer) {
	tConverted := t.(*exportNeighInterestSetMetricsTimer)
	interestSetID := tConverted.InterestSetID
	remoteInterestSet, ok := m.interestSets[interestSetID]
	m.logger.Infof("Export timer for neigh interest set %d triggered", interestSetID)

	if !ok {
		m.logger.Warnf("Canceling export timer for remote interest set %d", interestSetID)
		return
	}

	m.logger.Infof(
		"Exporting metric values for remote interest set %d: %s",
		interestSetID,
		remoteInterestSet.InterestSet.OutputBucketOpts.Name,
	)

	query := remoteInterestSet.InterestSet.Query
	result, err := m.me.MakeQuery(query.Expression, query.Timeout)

	if err != nil {
		remoteInterestSet.nrRetries++
		m.logger.Errorf(
			"Remote neigh interest set query failed to process with err %s (%d/%d)",
			err,
			remoteInterestSet.nrRetries,
			remoteInterestSet.InterestSet.MaxRetries,
		)

		if remoteInterestSet.nrRetries >= remoteInterestSet.InterestSet.MaxRetries {
			m.logger.Errorf("Aborting export timer for remote interest set %d", interestSetID)
			return // abort timer
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportNeighInterestSetMetricsTimer(
				remoteInterestSet.InterestSet.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
		return
	}

	m.logger.Infof(
		"Remote neigh interest set query result: (%+v)",
		result,
	)

	timeseriesDTOs := make([]body_types.TimeseriesDTO, 0, len(result))
	for _, ts := range result {
		ts.(*tsdb.StaticTimeseries).SetName(remoteInterestSet.InterestSet.OutputBucketOpts.Name)
		ts.(*tsdb.StaticTimeseries).SetTag("host", m.babel.SelfPeer().IP().String())
		tsDTO := ts.ToDTO()
		timeseriesDTOs = append(timeseriesDTOs, tsDTO)
	}

	toSendMsg := NewPropagateInterestSetMetricsMessage(interestSetID, timeseriesDTOs, 1)
	for _, sub := range remoteInterestSet.subscribers {
		if peer.PeersEqual(sub.p, m.babel.SelfPeer()) {
			for _, ts := range result {
				allPts := ts.All()
				if len(allPts) == 0 {
					m.logger.Error("Timeseries result is empty")
					continue
				}
				for _, pt := range allPts {
					err := m.tsdb.AddMetric(
						remoteInterestSet.InterestSet.OutputBucketOpts.Name,
						ts.Tags(),
						pt.Value(),
						pt.TS(),
					)
					if err != nil {
						m.logger.Panic(err)
					}
				}
			}
			continue
		}

		if !m.isPeerInView(sub.p) {
			m.logger.Errorf(
				"Continuing because peer is not in view: %s",
				sub.p.String(),
			)

			continue
		}
		m.babel.SendMessage(toSendMsg, sub.p, m.ID(), m.ID())
	}

	m.babel.RegisterTimer(
		m.ID(),
		NewExportNeighInterestSetMetricsTimer(
			remoteInterestSet.InterestSet.OutputBucketOpts.Granularity.Granularity,
			interestSetID,
		),
	)
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

		is, alreadyExists := m.interestSets[interestSetID]

		if alreadyExists {
			is.subscribers[sender.String()] = subWithTTL{
				p:           sender,
				ttl:         interestSet.TTL,
				lastRefresh: time.Now(),
			}

			m.logger.Info("Neigh interest set already present")
			continue
		}

		m.interestSets[interestSetID] = &neighInterestSet{
			nrRetries: 0,
			subscribers: map[string]subWithTTL{
				sender.String(): {ttl: interestSet.TTL, p: sender},
			},
			InterestSet: interestSet.InterestSet,
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportNeighInterestSetMetricsTimer(
				interestSet.InterestSet.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
	}
}

func (m *Monitor) handlePropagateNeighInterestSetMetricsMessage(sender peer.Peer, msg message.Message) {

	msgConverted := msg.(PropagateInterestSetMetricsMsg)
	interestSetID := msgConverted.InterestSetID

	interestSet, ok := m.interestSets[interestSetID]
	if !ok {
		m.logger.Errorf(
			"received propagation of metric values for missing interest set %d: %s from %s",
			interestSetID,
			interestSet.InterestSet.OutputBucketOpts.Name,
			sender.String(),
		)
		return
	}

	if !m.isPeerInView(sender) {
		m.logger.Errorf("received interest set propagation message but target (%s) is not in view", sender)
		return
	}

	m.logger.WithFields(logrus.Fields{"metric_values": msgConverted.Metrics}).Infof(
		"received propagation of metric values for interest set %d: %s from %s",
		interestSetID,
		interestSet.InterestSet.OutputBucketOpts.Name,
		sender.String(),
	)

	for _, sub := range interestSet.subscribers {

		if peer.PeersEqual(sub.p, sender) {
			continue
		}

		if peer.PeersEqual(m.babel.SelfPeer(), sub.p) {
			toAdd := make([]tsdb.ReadOnlyTimeSeries, 0, len(msgConverted.Metrics))
			for _, ts := range msgConverted.Metrics {
				toAdd = append(toAdd, tsdb.StaticTimeseriesFromDTO(ts))
			}
			err := m.tsdb.AddAll(toAdd)
			if err != nil {
				m.logger.Error(err)
			}
			continue
		}

		if !m.isPeerInView(sub.p) {
			m.logger.Errorf("received interest set propagation message but cannot propagate to target (%s) because it is not in view", sub.p.String())
			continue
		}

		if msgConverted.TTL > sub.ttl {
			m.logger.WithFields(logrus.Fields{"metric_values": msgConverted.Metrics, "msg_ttl": msgConverted.TTL, "sub_ttl": sub.ttl}).Warnf(
				"not relaying metric values for interest set %d: %s from %s to %s because msgConverted.TTL <= sub.ttl",
				interestSetID,
				interestSet.InterestSet.OutputBucketOpts.Name,
				sender.String(),
				sub.p.String(),
			)
			continue
		}
		m.logger.WithFields(logrus.Fields{"metric_values": msgConverted.Metrics, "msg_ttl": msgConverted.TTL, "sub_ttl": sub.ttl}).Infof(
			"relaying metric values for interest set %d: %s from %s to %s",
			interestSetID,
			interestSet.InterestSet.OutputBucketOpts.Name,
			sender.String(),
			sub.p.String(),
		)
		msgConverted.TTL++
		m.babel.SendMessage(msgConverted, sub.p, m.ID(), m.ID())
	}
}

// REQUEST HANDLERS

func (m *Monitor) handleAddNeighInterestSetRequest(req request.Request) request.Reply {
	addNeighInterestSetReq := req.(AddNeighborhoodInterestSetReq)
	interestSet := addNeighInterestSetReq.InterestSet
	interestSetID := addNeighInterestSetReq.Id
	m.interestSets[addNeighInterestSetReq.Id] = &neighInterestSet{
		InterestSet: addNeighInterestSetReq.InterestSet.IS,
		nrRetries:   0,
		subscribers: map[string]subWithTTL{
			m.babel.SelfPeer().String(): {
				ttl: addNeighInterestSetReq.InterestSet.TTL,
				p:   m.babel.SelfPeer(),
			},
		},
	}
	frequency := interestSet.IS.OutputBucketOpts.Granularity.Granularity
	m.logger.Infof("Installing local insterest set %d: %+v", interestSetID, interestSet)
	m.babel.RegisterTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(frequency, interestSetID))
	return nil
}

func (m *Monitor) handleRemoveNeighInterestSetRequest(req request.Request) request.Reply {
	remNeighInterestSetReq := req.(RemoveNeighborhoodInterestSetReq)
	toRemoveID := remNeighInterestSetReq.InterestSetID
	if is, ok := m.interestSets[toRemoveID]; ok {
		delete(is.subscribers, m.babel.SelfPeer().String())
		if len(is.subscribers) == 0 {
			delete(m.interestSets, toRemoveID)
		}
	}
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
	for intSetID, intSet := range m.interestSets {
		if _, ok := intSet.subscribers[nodeDown.String()]; ok {
			// m.tsdb.DeleteBucket(intSet.interestSet.OutputBucketOpts.Name, map[string]string{"host": m.babel.SelfPeer().IP().String()})
			delete(intSet.subscribers, nodeDown.String())
			if len(intSet.subscribers) == 0 {
				delete(m.interestSets, intSetID)
				m.logger.Infof("Deleting neigh int set: %d", intSetID)
			}
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

func (m *Monitor) broadcastMessage(msg message.Message, sendToSiblings, sendToChildren, sendToParent bool) {
	if sendToSiblings {
		for _, s := range m.currView.Siblings {
			m.babel.SendMessage(msg, s, m.ID(), m.ID())
		}
	}

	if sendToChildren {
		for _, s := range m.currView.Children {
			m.babel.SendMessage(msg, s, m.ID(), m.ID())
		}
	}

	if sendToParent {
		if m.currView.Parent != nil {
			m.babel.SendMessage(msg, m.currView.Parent, m.ID(), m.ID())
		}
	}
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

// func (m *Monitor) broadcastInterestSet(msg message.Message, subscribers map[string]peer.Peer) {
// 	isSibling, isChildren, isParent := m.getPeerRelationshipType(sender)

// 	if isSibling {
// 		for _, c := range m.currView.Children {
// 			if _, ok := exlusions[c.String()]; !ok {
// 				m.babel.SendMessage(msg, c, m.ID(), m.ID())
// 			}
// 		}
// 		return
// 	}

// 	if isChildren {
// 		for _, s := range m.currView.Siblings {
// 			if _, ok := exlusions[s.String()]; !ok {
// 				m.babel.SendMessage(msg, s, m.ID(), m.ID())
// 			}
// 		}

// 		if m.currView.Parent != nil {
// 			if _, ok := exlusions[m.currView.Parent.String()]; !ok {
// 				m.babel.SendMessage(msg, m.currView.Parent, m.ID(), m.ID())
// 			}
// 		}
// 		return
// 	}

// 	if isParent {
// 		for _, c := range m.currView.Children {
// 			if _, ok := exlusions[m.currView.Parent.String()]; !ok {
// 				m.babel.SendMessage(msg, c, m.ID(), m.ID())
// 			}
// 		}
// 		return
// 	}
// }

func (m *Monitor) DialFailed(p peer.Peer) {
}

func (m *Monitor) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {
	return false
}

func (m *Monitor) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	return false
}

func (m *Monitor) OutConnDown(p peer.Peer) {}

func arrContains(pArr []peer.Peer, toFind peer.Peer) bool {
	for _, p := range pArr {
		if peer.PeersEqual(p, toFind) {
			return true
		}
	}
	return false
}
