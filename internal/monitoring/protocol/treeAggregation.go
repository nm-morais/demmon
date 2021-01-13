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
	AggSet      body_types.TreeAggregationSet
	childValues map[string]map[string]interface{}
	local       bool
	sender      peer.Peer
	lastRefresh time.Time
}

// REQUEST CREATORS

func (m *Monitor) AddTreeAggregationFuncReq(key int64, interestSet body_types.TreeAggregationSet) {
	m.babel.SendRequest(NewAddTreeAggregationFuncReq(key, interestSet), m.ID(), m.ID())
}

// BROADCAST TIMER

func (m *Monitor) handleRebroadcastTreeInterestSetsTimer(t timer.Timer) {
	m.babel.RegisterTimer(
		m.ID(),
		NewBroadcastTreeAggregationFuncsTimer(RebroadcastTreeAggFuncTimerDuration),
	)

	treeAggFuncstoSend := make(map[int64]body_types.TreeAggregationSet)

	for isID, is := range m.treeAggFuncs {
		if is.AggSet.Levels > 0 {
			treeAggFuncstoSend[isID] = body_types.TreeAggregationSet{
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
	for _, child := range m.currView.Children {
		m.babel.SendMessage(toSend, child, m.ID(), m.ID(), false)
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

	m.logger.Infof(
		"Exporting metric values for tree aggregation func %d: %s",
		interestSetID,
		treeAggFunc.AggSet.OutputBucketOpts.Name,
	)

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
			return // abort timer
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportTreeAggregationFuncTimer(
				treeAggFunc.AggSet.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
		return
	}

	m.logger.Infof(
		"tree aggregation function query result: (%+v)",
		queryResult,
	)

	var mergedVal map[string]interface{}
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
	} else {
		m.logger.Warn("Not merging values due to not having children values")
		mergedVal = queryResult
	}

	if treeAggFunc.local {
		err := m.tsdb.AddMetric(
			treeAggFunc.AggSet.OutputBucketOpts.Name,
			make(map[string]string), // TODO is this correct? merged points will have no tags
			mergedVal,
			time.Now(),
		)
		if err != nil {
			m.logger.Panic(err)
		}
		return
	}

	toSendMsg := NewPropagateTreeAggFuncMetricsMessage(interestSetID, &body_types.ObservableDTO{TS: time.Now(), Fields: mergedVal})

	if peer.PeersEqual(m.currView.Parent, treeAggFunc.sender) {
		m.logger.Infof(
			"propagating metrics for tree aggregation function %d (%+v) to parent %s",
			interestSetID,
			mergedVal,
			m.currView.Parent.String(),
		)
		m.babel.SendMessage(toSendMsg, m.currView.Parent, m.ID(), m.ID(), false)
	} else {
		m.logger.Errorf(
			"tree aggregation function %d could not propagate to parent due to not having parent",
			interestSetID,
		)
	}

	m.babel.RegisterTimer(
		m.ID(),
		NewExportTreeAggregationFuncTimer(
			treeAggFunc.AggSet.OutputBucketOpts.Granularity.Granularity,
			interestSetID,
		),
	)
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
		m.logger.Warn("received install tree aggregation function from peer not in my view")
		return
	}

	m.logger.Infof(
		"received message to install tree aggregation function from %s (%+v)",
		sender.String(),
		installTreeAggFuncMsg,
	)

	for treeAggFuncID, treeAggFunc := range installTreeAggFuncMsg.InterestSets {
		m.logger.Infof("installing tree aggregation function %d: %+v", treeAggFuncID, treeAggFunc)
		m.treeAggFuncs[treeAggFuncID] = &treeAggSet{
			nrRetries:   0,
			childValues: make(map[string]map[string]interface{}),
			local:       false,
			sender:      sender,
			AggSet:      treeAggFunc,
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportTreeAggregationFuncTimer(
				treeAggFunc.OutputBucketOpts.Granularity.Granularity,
				treeAggFuncID,
			),
		)
	}
}

// CLEANUP

func (m *Monitor) cleanupTreeInterestSets() {
	for isID, is := range m.treeAggFuncs {
		if time.Since(is.lastRefresh) > ExpireTreeAggFuncTimeout {
			m.logger.Errorf(
				"Removing tree agg func from peer %s because entry expired",
				is.sender,
				isID,
			)
			delete(m.treeAggFuncs, isID)
			continue
		}

		if !peer.PeersEqual(m.currView.Parent, is.sender) {
			m.logger.Errorf(
				"Removing tree agg func from peer %s because peer is not my parent anymore",
				is.sender,
				isID,
			)
			delete(m.treeAggFuncs, isID)
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

	m.treeAggFuncs[addTreeAggFuncSetReq.Id] = &treeAggSet{
		nrRetries:   0,
		AggSet:      addTreeAggFuncSetReq.InterestSet,
		childValues: make(map[string]map[string]interface{}),
		local:       true,
		sender:      nil,
	}
	frequency := interestSet.OutputBucketOpts.Granularity.Granularity
	m.logger.Infof("Installing local tree aggregation function %d: %+v", interestSetID, interestSet)
	m.babel.RegisterTimer(m.ID(), NewExportTreeAggregationFuncTimer(frequency, interestSetID))
	return nil
}

// HANDLE NODE DOWN

func (m *Monitor) handleNodeDownTreeAggFunc(nodeDown peer.Peer) {
	// remove all child values from tree agg func
	for _, treeAggFunc := range m.treeAggFuncs {
		delete(treeAggFunc.childValues, nodeDown.String())
	}
}
