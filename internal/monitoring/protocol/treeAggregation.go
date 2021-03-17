package protocol

import (
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

type treeAggSet struct {
	nrRetries   int
	AggSet      *body_types.TreeAggregationSet
	childValues map[string]map[string]interface{}
	local       bool
	sender      peer.Peer
	lastRefresh time.Time
	timerID     int
}

// REQUEST CREATORS

func (m *Monitor) AddTreeAggregationFuncReq(key int64, interestSet *body_types.TreeAggregationSet) {
	m.babel.SendRequest(NewAddTreeAggregationFuncReq(key, interestSet), m.ID(), m.ID())
}

// BROADCAST TIMER

func (m *Monitor) handleRebroadcastTreeInterestSetsTimer(t timer.Timer) {
	m.logger.Info("Export timer for tree aggregation set broadcasts")
	m.broadcastInterestSets()
}

func (m *Monitor) broadcastInterestSets(specificChildren ...peer.Peer) {
	treeAggFuncstoSend := make(map[int64]*body_types.TreeAggregationSet)

	for isID, is := range m.treeAggFuncs {
		if is.AggSet.Levels > 0 {
			treeAggFuncstoSend[isID] = &body_types.TreeAggregationSet{
				MaxRetries:       is.AggSet.MaxRetries,
				Query:            is.AggSet.Query,
				OutputBucketOpts: is.AggSet.OutputBucketOpts,
				MergeFunction:    is.AggSet.MergeFunction,
				Levels:           is.AggSet.Levels - 1,
			}
		}
	}

	if len(treeAggFuncstoSend) == 0 {
		return
	}

	toSend := NewInstallTreeAggFuncMessage(treeAggFuncstoSend)
	if len(specificChildren) > 0 {
		for _, child := range specificChildren {
			m.SendMessage(toSend, child)
		}
	} else {
		for _, child := range m.currView.Children {
			m.SendMessage(toSend, child)
		}
	}

}

// EXPORT TIMER

func (m *Monitor) handleExportTreeAggregationFuncTimer(t timer.Timer) {
	tConverted := t.(*exportTreeAggregationFuncTimer)
	interestSetID := tConverted.InterestSetID
	treeAggFunc, ok := m.treeAggFuncs[interestSetID]
	m.logger.Infof("Export timer for tree aggregation func %d triggered", interestSetID)

	if !ok {
		m.logger.Warnf("Canceling export timer for tree aggregation func %d", interestSetID)
		return
	}

	if !peer.PeersEqual(m.currView.Parent, treeAggFunc.sender) && !treeAggFunc.local {
		m.logger.Errorf(
			"tree aggregation function %d could not propagate to parent because sender is not same as current parent",
			interestSetID,
		)
	}

	m.logger.Infof(
		"Exporting metric values for tree aggregation func %d: %s",
		interestSetID,
		treeAggFunc.AggSet.OutputBucketOpts.Name,
	)
	m.propagateTreeIntSetMetrics(interestSetID, treeAggFunc)
}

func (m *Monitor) propagateTreeIntSetMetrics(interestSetID int64, treeAggFunc *treeAggSet) {

	query := treeAggFunc.AggSet.Query
	queryResult, err := m.me.MakeQuerySingleReturn(query.Expression, query.Timeout)

	if err != nil {
		treeAggFunc.nrRetries++
		m.logger.Errorf(
			"Remote tree aggregation func query failed to process with err %s (%d/%d)",
			err,
			treeAggFunc.nrRetries,
			treeAggFunc.AggSet.MaxRetries,
		)

		if treeAggFunc.nrRetries >= treeAggFunc.AggSet.MaxRetries {
			m.logger.Errorf("Aborting export timer for tree aggregation func %d", interestSetID)
			m.babel.CancelTimer(treeAggFunc.timerID) // abort timer
			return
		}
		return
	}

	m.logger.Infof(
		"tree aggregation function query result: (%+v)",
		queryResult,
	)

	var mergedVal map[string]interface{}
	mergedVal = queryResult
	if len(treeAggFunc.childValues) > 0 {
		valuesToMerge := []map[string]interface{}{}
		for _, childVal := range treeAggFunc.childValues {
			valuesToMerge = append(valuesToMerge, childVal)
		}

		valuesToMerge = append(valuesToMerge, queryResult)

		m.logger.Infof(
			"Merging values: (%+v)",
			valuesToMerge,
		)

		mergedVal, err = m.me.RunMergeFunc(treeAggFunc.AggSet.MergeFunction.Expression, treeAggFunc.AggSet.MergeFunction.Timeout, valuesToMerge)
		if err != nil {
			panic(err)
		}
	}
	m.logger.Infof(
		"Merged value: (%+v)",
		mergedVal,
	)
	if treeAggFunc.local {

		m.logger.Info("Adding merged values locally")
		err := m.tsdb.AddMetric(
			treeAggFunc.AggSet.OutputBucketOpts.Name,
			make(map[string]string),
			mergedVal,
			time.Now(),
		)
		if err != nil {
			m.logger.Panic(err)
		}
		return
	}

	if peer.PeersEqual(treeAggFunc.sender, m.babel.SelfPeer()) {
		return
	}

	toSendMsg := NewPropagateTreeAggFuncMetricsMessage(interestSetID, &body_types.ObservableDTO{TS: time.Now(), Fields: mergedVal})
	m.logger.Infof(
		"propagating metrics for tree aggregation function %d (%+v) to: %s",
		interestSetID,
		mergedVal,
		treeAggFunc.sender.String(),
	)
	m.SendMessage(toSendMsg, treeAggFunc.sender)
}

// MESSAGES

func (m *Monitor) handlePropagateTreeAggFuncMetricsMessage(sender peer.Peer, msg message.Message) {

	msgConverted := msg.(PropagateTreeAggFuncMetricsMsg)
	treeAggSetID := msgConverted.InterestSetID
	treeAggSet, ok := m.treeAggFuncs[treeAggSetID]

	if !ok {
		m.logger.Errorf(
			"received propagation of metric values for missing tree agg func %d from %s",
			treeAggSetID,
			sender.String(),
		)
		return
	}

	_, children, _ := m.getPeerRelationshipType(sender)
	if !children {
		m.logger.Errorf("received tree agg func propagation message from node (%s) not in my children", sender)
		return
	}

	m.logger.WithFields(logrus.Fields{"value": msgConverted.Value}).Infof(
		"received propagation of metric values for tree agg func %d: %s from %s",
		treeAggSetID,
		treeAggSet.AggSet.OutputBucketOpts.Name,
		sender.String(),
	)
	treeAggSet.childValues[sender.String()] = msgConverted.Value.Fields
}

func (m *Monitor) handleInstallTreeAggFuncMetricsMessage(sender peer.Peer, msg message.Message) {
	installTreeAggFuncMsg := msg.(InstallTreeAggFuncMsg)
	_, _, parent := m.getPeerRelationshipType(sender)
	if !parent {
		m.logger.Warnf("received install tree aggregation function from peer not in my view (%s)", sender.String())
		return
	}

	m.logger.Infof(
		"received message to install tree aggregation function from %s (%+v)",
		sender.String(),
		installTreeAggFuncMsg,
	)

	for treeAggFuncID, treeAggFunc := range installTreeAggFuncMsg.InterestSets {
		m.logger.Infof("installing tree aggregation function %d: %+v", treeAggFuncID, treeAggFunc)

		alreadyExisting, ok := m.treeAggFuncs[treeAggFuncID]
		if ok {
			alreadyExisting.lastRefresh = time.Now()
			alreadyExisting.sender = sender
			m.propagateTreeIntSetMetrics(treeAggFuncID, alreadyExisting)
			continue
		}

		timerID := m.babel.RegisterPeriodicTimer(
			m.ID(),
			NewExportTreeAggregationFuncTimer(
				treeAggFunc.OutputBucketOpts.Granularity.Granularity,
				treeAggFuncID,
			),
			true,
		)
		m.treeAggFuncs[treeAggFuncID] = &treeAggSet{
			nrRetries:   0,
			AggSet:      treeAggFunc,
			childValues: make(map[string]map[string]interface{}),
			local:       false,
			sender:      sender,
			lastRefresh: time.Now(),
			timerID:     timerID,
		}
	}
}

// CLEANUP

func (m *Monitor) cleanupTreeInterestSets() {
	for isID, is := range m.treeAggFuncs {
		if is.local {
			continue
		}
		if time.Since(is.lastRefresh) > ExpireTreeAggFuncTimeout {
			m.logger.Warnf(
				"Removing tree agg func %d from peer %s because entry expired",
				isID,
				is.sender,
			)
			delete(m.treeAggFuncs, isID)
			m.babel.CancelTimer(is.timerID)
			continue
		}
	}
}

// REQUEST HANDLERS

func (m *Monitor) handleAddTreeAggregationFuncRequest(req request.Request) request.Reply {
	addTreeAggFuncSetReq := req.(AddTreeAggregationFuncReq)
	interestSet := addTreeAggFuncSetReq.InterestSet
	interestSetID := addTreeAggFuncSetReq.Id

	existing, ok := m.treeAggFuncs[interestSetID]
	if ok {
		existing.local = true
		return nil
	}
	frequency := interestSet.OutputBucketOpts.Granularity.Granularity
	timerID := m.babel.RegisterPeriodicTimer(m.ID(), NewExportTreeAggregationFuncTimer(frequency, interestSetID), true)
	m.treeAggFuncs[addTreeAggFuncSetReq.Id] = &treeAggSet{
		nrRetries:   0,
		AggSet:      addTreeAggFuncSetReq.InterestSet,
		childValues: make(map[string]map[string]interface{}),
		local:       true,
		sender:      m.babel.SelfPeer(),
		lastRefresh: time.Now(),
		timerID:     timerID,
	}
	m.logger.Infof("Installing local tree aggregation function %d: %+v", interestSetID, interestSet)
	return nil
}

// HANDLE NODE DOWN

func (m *Monitor) handleNodeDownTreeAggFunc(nodeDown peer.Peer) {
	// remove all child values from tree agg func
	for _, treeAggFunc := range m.treeAggFuncs {
		delete(treeAggFunc.childValues, nodeDown.String())
	}
}

// HANDLE NODE UP

func (m *Monitor) handleNodeUpTreeAggFunc(nodeUp peer.Peer) {
	// remove all child values from tree agg func
	_, isChildren, _ := m.getPeerRelationshipType(nodeUp)
	if isChildren {
		m.broadcastInterestSets(nodeUp)
	}
}
