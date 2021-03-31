package protocol

import (
	"fmt"
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

type subWithTTL struct {
	ttl         int
	p           peer.Peer
	lastRefresh time.Time
}

type neighInterestSet struct {
	exportTimerID int
	storeHopCount bool
	nrRetries     int
	subscribers   map[string]subWithTTL
	TTL           int
	IS            body_types.InterestSet
}

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
		existing.storeHopCount = addNeighInterestSetReq.InterestSet.StoreHopCountAsTag
		existing.subscribers[m.babel.SelfPeer().String()] = subWithTTL{
			ttl: addNeighInterestSetReq.InterestSet.TTL,
			p:   m.babel.SelfPeer(),
		}
		m.neighInterestSets[addNeighInterestSetReq.Id] = existing
		return nil
	}

	frequency := interestSet.IS.OutputBucketOpts.Granularity.Granularity
	tID := m.babel.RegisterPeriodicTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(frequency, interestSetID), true)
	m.neighInterestSets[addNeighInterestSetReq.Id] = &neighInterestSet{
		exportTimerID: tID,
		storeHopCount: addNeighInterestSetReq.InterestSet.StoreHopCountAsTag,
		nrRetries:     addNeighInterestSetReq.InterestSet.IS.MaxRetries,
		subscribers:   map[string]subWithTTL{m.babel.SelfPeer().String(): {ttl: addNeighInterestSetReq.InterestSet.TTL, p: m.babel.SelfPeer()}},
		TTL:           0,
		IS:            addNeighInterestSetReq.InterestSet.IS,
	}
	m.logger.Infof("Installing local insterest set %d: %+v", interestSetID, interestSet)
	return nil
}

// REMOVE REQUEST HANDLER

func (m *Monitor) handleRemoveNeighInterestSetRequest(req request.Request) request.Reply {
	remNeighInterestSetReq := req.(RemoveNeighborhoodInterestSetReq)
	toRemoveID := remNeighInterestSetReq.InterestSetID

	if is, ok := m.neighInterestSets[toRemoveID]; ok {
		delete(is.subscribers, m.babel.SelfPeer().String())
		if len(is.subscribers) == 0 {
			m.babel.CancelTimer(is.exportTimerID)
			delete(m.neighInterestSets, toRemoveID)
		}
	}
	return nil
}

// EXPORT TIMER HANDLER

func (m *Monitor) handleExportNeighInterestSetMetricsTimer(t timer.Timer) {
	tConverted := t.(*exportNeighInterestSetMetricsTimer)
	interestSetID := tConverted.InterestSetID
	interestSet, ok := m.neighInterestSets[interestSetID]
	// m.logger.Infof("Export timer for neigh interest set %d triggered", interestSetID)

	if !ok {
		m.logger.Warnf("Canceling export timer for remote interest set %d", interestSetID)
		return
	}

	// m.logger.Infof(
	// 	"Exporting metric values for remote interest set %d: %s",
	// 	interestSetID,
	// 	remoteInterestSet.IS.OutputBucketOpts.Name,
	// )

	query := interestSet.IS.Query
	result, err := m.me.MakeQuery(query.Expression, query.Timeout)

	if err != nil {
		interestSet.nrRetries++
		m.logger.Errorf(
			"Remote neigh interest set query failed to process with err %s (%d/%d)",
			err,
			interestSet.nrRetries,
			interestSet.IS.MaxRetries,
		)

		if interestSet.nrRetries >= interestSet.IS.MaxRetries {
			m.logger.Errorf("Aborting export timer for remote interest set %d", interestSetID)
			m.babel.CancelTimer(interestSet.exportTimerID)
			return // abort timer
		}
		return
	}

	// m.logger.Infof(
	// 	"Remote neigh interest set query result: (%+v)",
	// 	result,
	// )

	timeseriesDTOs := make([]body_types.TimeseriesDTO, 0, len(result))
	for _, tsGeneric := range result {

		ts := tsGeneric.(*tsdb.StaticTimeseries)
		ts.SetName(interestSet.IS.OutputBucketOpts.Name)
		if _, ok := ts.Tag("host"); !ok {
			ts.SetTag("host", m.babel.SelfPeer().IP().String())
		}
		tsDTO := ts.ToDTO()
		timeseriesDTOs = append(timeseriesDTOs, tsDTO)
	}

	toSendMsg := NewPropagateNeighInterestSetMetricsMessage(interestSetID, timeseriesDTOs, 1)
	for _, sub := range interestSet.subscribers {
		if peer.PeersEqual(sub.p, m.babel.SelfPeer()) {
			for _, ts := range result {
				allPts := ts.All()
				if len(allPts) == 0 {
					m.logger.Error("Timeseries result is empty")
					continue
				}

				tmpTags := map[string]string{}
				for k, v := range ts.Tags() {
					tmpTags[k] = v
				}
				if interestSet.storeHopCount {
					if _, ok := tmpTags["hop_nr"]; !ok {
						tmpTags["hop_nr"] = "0"
					}
				}

				for _, pt := range allPts {
					err := m.tsdb.AddMetric(
						interestSet.IS.OutputBucketOpts.Name,
						tmpTags,
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
		defer m.SendMessage(toSendMsg, sub.p, false)
	}
}

// BROADCAST TIMER

func (m *Monitor) handleRebroadcastInterestSetsTimer(t timer.Timer) {
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

	// m.logger.Infof(
	// 	"received message to install neigh interest sets from %s (%+v)",
	// 	sender.String(),
	// 	installNeighIntSetMsg,
	// )

	for interestSetID, interestSet := range installNeighIntSetMsg.InterestSets {

		is, alreadyExists := m.neighInterestSets[interestSetID]

		if alreadyExists {
			is.subscribers[sender.String()] = subWithTTL{
				p:           sender,
				ttl:         interestSet.TTL,
				lastRefresh: time.Now(),
			}

			// m.logger.Info("Neigh interest set already present")
			continue
		}

		m.logger.Infof("installing neigh interest set from remote peer %d: %+v", interestSetID, interestSet)
		tID := m.babel.RegisterPeriodicTimer(m.ID(), NewExportNeighInterestSetMetricsTimer(interestSet.IS.OutputBucketOpts.Granularity.Granularity, interestSetID), true)
		m.neighInterestSets[interestSetID] = &neighInterestSet{
			exportTimerID: tID,
			storeHopCount: false,
			nrRetries:     interestSet.nrRetries,
			subscribers:   map[string]subWithTTL{sender.String(): {ttl: interestSet.TTL, p: sender}},
			TTL:           0,
			IS:            interestSet.IS,
		}

	}
}

// PROPAGATE MESSAGE HANDLER

func (m *Monitor) handlePropagateNeighInterestSetMetricsMessage(sender peer.Peer, msg message.Message) {

	msgConverted := msg.(PropagateNeighInterestSetMetricsMsg)
	interestSetID := msgConverted.InterestSetID

	interestSet, ok := m.neighInterestSets[interestSetID]
	if !ok {
		m.logger.Warnf(
			"received propagation of metric values for missing neigh interest set %d: %s from %s with hopNr: %d",
			interestSetID,
			interestSet.IS.OutputBucketOpts.Name,
			sender.String(),
			msgConverted.TTL,
		)
		return
	}

	if !m.isPeerInView(sender) {
		m.logger.Warnf("received interest set propagation message but target (%s) is not in view", sender)
		return
	}

	m.logger.WithFields(logrus.Fields{"metric_values": msgConverted.Metrics}).Infof(
		"received propagation of metric values with TTL=%d for interest set %d: %s from %s",
		msgConverted.TTL,
		interestSetID,
		interestSet.IS.OutputBucketOpts.Name,
		sender.String(),
	)

	for _, sub := range interestSet.subscribers {

		if peer.PeersEqual(sub.p, sender) {
			continue
		}

		if msgConverted.TTL > sub.ttl {
			// m.logger.WithFields(logrus.Fields{"metric_values": msgConverted.Metrics, "msg_ttl": msgConverted.TTL, "sub_ttl": sub.ttl}).Warnf(
			// 	"not relaying metric values for interest set %d: %s from %s to %s because msgConverted.TTL > sub.ttl (%d > %d)",
			// 	interestSetID,
			// 	interestSet.IS.OutputBucketOpts.Name,
			// 	sender.String(),
			// 	sub.p.String(),
			// 	msgConverted.TTL,
			// 	sub.ttl,
			// )
			continue
		}

		if peer.PeersEqual(m.babel.SelfPeer(), sub.p) {
			m.logger.Infof(
				"Inserting metric values locally for interest set %d: %s from %s",
				interestSetID,
				interestSet.IS.OutputBucketOpts.Name,
				sender.String(),
			)
			for _, ts := range msgConverted.Metrics {
				tmpTags := map[string]string{}

				for k, v := range ts.TSTags {
					tmpTags[k] = v
				}

				if _, ok := tmpTags["hop_nr"]; !ok {
					tmpTags["hop_nr"] = fmt.Sprintf("%d", msgConverted.TTL)
				}
				// m.logger.WithFields(logrus.Fields{"metric_values": msgConverted.Metrics, "msg_ttl": msgConverted.TTL, "sub_ttl": sub.ttl}).Info("Adding metrics locally")
				for _, pt := range ts.Values {
					err := m.tsdb.AddMetric(
						interestSet.IS.OutputBucketOpts.Name,
						tmpTags,
						pt.Fields,
						pt.TS,
					)
					if err != nil {
						m.logger.Panic(err)
					}
				}
			}
			continue
		}

		if !m.isPeerInView(sub.p) {
			m.logger.Errorf("received interest set propagation message but cannot propagate to target (%s) because it is not in view", sub.p.String())
			continue
		}

		if !m.shouldBroadcastTo(sender, sub.p) {
			continue
		}
		// m.logger.WithFields(logrus.Fields{"metric_values": msgConverted.Metrics, "msg_ttl": msgConverted.TTL, "sub_ttl": sub.ttl}).Infof(
		// 	"relaying metric values for interest set %d: %s from %s to %s",
		// 	interestSetID,
		// 	interestSet.IS.OutputBucketOpts.Name,
		// 	sender.String(),
		// 	sub.p.String(),
		// )
		m.SendMessage(NewPropagateNeighInterestSetMetricsMessage(interestSetID, msgConverted.Metrics, msgConverted.TTL+1), sub.p, false)
	}
}

func (m *Monitor) shouldBroadcastTo(from, to peer.Peer) bool {
	fromSibling, fromChildren, fromParent := m.getPeerRelationshipType(from)
	toSibling, toChildren, toParent := m.getPeerRelationshipType(to)

	if fromSibling {
		return toChildren
	}
	if fromParent {
		return toChildren
	}
	if fromChildren {
		return toParent || toSibling
	}
	return false
}

// AUX FUNCTIONS

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
			m.SendMessage(toSend, sibling, false)
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
			m.SendMessage(toSend, children, false)
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
		m.SendMessage(toSend, m.currView.Parent, false)
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
			m.babel.CancelTimer(is.exportTimerID)
			delete(m.neighInterestSets, isID)
		}
	}
}

// HANDLE NODE DOWN

func (m *Monitor) handleNodeDownNeighInterestSet(nodeDown peer.Peer) {
	// isSibling, isChildren, isParent := m.getPeerRelationshipType(nodeDown)
	for intSetID, intSet := range m.neighInterestSets {
		if _, ok := intSet.subscribers[nodeDown.String()]; ok {
			// m.tsdb.DeleteBucket(intSet.interestSet.OutputBucketOpts.Name, map[string]string{"host": m.babel.SelfPeer().IP().String()})
			delete(intSet.subscribers, nodeDown.String())
			if len(intSet.subscribers) == 0 {
				m.babel.CancelTimer(intSet.exportTimerID)
				delete(m.neighInterestSets, intSetID)
				m.logger.Infof("Deleting neigh int set: %d", intSetID)
			}
		}
	}
}
