package protocol

import (
	"math"
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

// REQUEST CREATOR

func (m *Monitor) AddNeighborhoodInterestSetReq(key int64, interestSet body_types.NeighborhoodInterestSet) {
	m.babel.SendRequest(NewAddNeighborhoodInterestSetReq(key, interestSet), m.ID(), m.ID())
}

// ADD REQUEST HANDLER

func (m *Monitor) handleAddNeighInterestSetRequest(req request.Request) request.Reply {
	addNeighInterestSetReq := req.(AddNeighborhoodInterestSetReq)
	interestSet := addNeighInterestSetReq.InterestSet
	interestSetID := addNeighInterestSetReq.Id

	existing, ok := m.neighInterestSets[addNeighInterestSetReq.Id]
	if ok {
		existing.subscribers[m.babel.SelfPeer().String()] = subWithTTL{
			ttl: addNeighInterestSetReq.InterestSet.TTL,
			p:   m.babel.SelfPeer(),
		}
		return nil
	}

	m.neighInterestSets[addNeighInterestSetReq.Id] = &neighInterestSet{
		IS:        addNeighInterestSetReq.InterestSet.IS,
		nrRetries: 0,
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

// REMOVE REQUEST HANDLER

func (m *Monitor) handleRemoveNeighInterestSetRequest(req request.Request) request.Reply {
	remNeighInterestSetReq := req.(RemoveNeighborhoodInterestSetReq)
	toRemoveID := remNeighInterestSetReq.InterestSetID

	if is, ok := m.neighInterestSets[toRemoveID]; ok {
		delete(is.subscribers, m.babel.SelfPeer().String())
		if len(is.subscribers) == 0 {
			delete(m.neighInterestSets, toRemoveID)
		}
	}
	return nil
}

// EXPORT TIMER HANDLER

func (m *Monitor) handleExportNeighInterestSetMetricsTimer(t timer.Timer) {
	tConverted := t.(*exportNeighInterestSetMetricsTimer)
	interestSetID := tConverted.InterestSetID
	remoteInterestSet, ok := m.neighInterestSets[interestSetID]
	m.logger.Infof("Export timer for neigh interest set %d triggered", interestSetID)

	if !ok {
		m.logger.Warnf("Canceling export timer for remote interest set %d", interestSetID)
		return
	}

	m.logger.Infof(
		"Exporting metric values for remote interest set %d: %s",
		interestSetID,
		remoteInterestSet.IS.OutputBucketOpts.Name,
	)

	query := remoteInterestSet.IS.Query
	result, err := m.me.MakeQuery(query.Expression, query.Timeout)

	if err != nil {
		remoteInterestSet.nrRetries++
		m.logger.Errorf(
			"Remote neigh interest set query failed to process with err %s (%d/%d)",
			err,
			remoteInterestSet.nrRetries,
			remoteInterestSet.IS.MaxRetries,
		)

		if remoteInterestSet.nrRetries >= remoteInterestSet.IS.MaxRetries {
			m.logger.Errorf("Aborting export timer for remote interest set %d", interestSetID)
			return // abort timer
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportNeighInterestSetMetricsTimer(
				remoteInterestSet.IS.OutputBucketOpts.Granularity.Granularity,
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
		ts.(*tsdb.StaticTimeseries).SetName(remoteInterestSet.IS.OutputBucketOpts.Name)
		ts.(*tsdb.StaticTimeseries).SetTag("host", m.babel.SelfPeer().IP().String())
		tsDTO := ts.ToDTO()
		timeseriesDTOs = append(timeseriesDTOs, tsDTO)
	}

	toSendMsg := NewPropagateNeighInterestSetMetricsMessage(interestSetID, timeseriesDTOs, 1)
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
						remoteInterestSet.IS.OutputBucketOpts.Name,
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
			remoteInterestSet.IS.OutputBucketOpts.Granularity.Granularity,
			interestSetID,
		),
	)
}

// BROADCAST TIMER

func (m *Monitor) handleBroadcastInterestSetsTimer(t timer.Timer) {
	m.babel.RegisterTimer(
		m.ID(),
		NewRebroadcastInterestSetsTimer(RebroadcastNeighInterestSetsTimerDuration),
	)
	m.broadcastNeighInterestSetsToSiblings()
	m.broadcastNeighInterestSetsToChildren()
	m.broadcastNeighInterestSetsToParent()
}

// INSTALL MESSAGE HANDLER

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

		is, alreadyExists := m.neighInterestSets[interestSetID]

		if alreadyExists {
			is.subscribers[sender.String()] = subWithTTL{
				p:           sender,
				ttl:         interestSet.TTL,
				lastRefresh: time.Now(),
			}

			m.logger.Info("Neigh interest set already present")
			continue
		}

		m.neighInterestSets[interestSetID] = &neighInterestSet{
			nrRetries: 0,
			subscribers: map[string]subWithTTL{
				sender.String(): {ttl: interestSet.TTL, p: sender},
			},
			IS: interestSet.IS,
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportNeighInterestSetMetricsTimer(
				interestSet.IS.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
	}
}

// PROPAGATE MESSAGE HANDLER

func (m *Monitor) handlePropagateNeighInterestSetMetricsMessage(sender peer.Peer, msg message.Message) {

	msgConverted := msg.(PropagateNeighInterestSetMetricsMsg)
	interestSetID := msgConverted.InterestSetID

	interestSet, ok := m.neighInterestSets[interestSetID]
	if !ok {
		m.logger.Errorf(
			"received propagation of metric values for missing interest set %d: %s from %s",
			interestSetID,
			interestSet.IS.OutputBucketOpts.Name,
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
		interestSet.IS.OutputBucketOpts.Name,
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
				interestSet.IS.OutputBucketOpts.Name,
				sender.String(),
				sub.p.String(),
			)
			continue
		}
		m.logger.WithFields(logrus.Fields{"metric_values": msgConverted.Metrics, "msg_ttl": msgConverted.TTL, "sub_ttl": sub.ttl}).Infof(
			"relaying metric values for interest set %d: %s from %s to %s",
			interestSetID,
			interestSet.IS.OutputBucketOpts.Name,
			sender.String(),
			sub.p.String(),
		)
		msgConverted.TTL++
		m.babel.SendMessage(msgConverted, sub.p, m.ID(), m.ID())
	}
}

// AUS FUNCTIONS

func (m *Monitor) broadcastNeighInterestSetsToSiblings() {
	for _, sibling := range m.currView.Siblings {
		neighIntSetstoSend := make(map[int64]neighInterestSet)

		for isID, is := range m.neighInterestSets {
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
					IS:  is.IS,
					TTL: maxTTL - 1,
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
		neighIntSetstoSend := make(map[int64]neighInterestSet)

		for isID, is := range m.neighInterestSets {
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
					IS:  is.IS,
					TTL: maxTTL - 1,
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
	neighIntSetstoSend := make(map[int64]neighInterestSet)

	if m.currView.Parent == nil {
		return
	}

	for isID, is := range m.neighInterestSets {
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
				IS:  is.IS,
				TTL: maxTTL - 1,
			}
		}
	}

	if len(neighIntSetstoSend) > 0 {
		toSend := NewInstallNeighInterestSetMessage(neighIntSetstoSend)
		m.babel.SendMessage(toSend, m.currView.Parent, m.ID(), m.ID())
	}
}

func (m *Monitor) cleanupNeighInterestSets() {
	for isID, is := range m.neighInterestSets {
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
			delete(m.neighInterestSets, isID)
		}
	}
}
