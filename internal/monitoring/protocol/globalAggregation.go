package protocol

import (
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

type sub struct {
	p           peer.Peer
	lastRefresh time.Time
}

type globalAggFunc struct {
	nrRetries   int
	subscribers map[string]sub
	NeighValues map[string]map[string]interface{}
	AF          body_types.GlobalAggregationFunction
}

// REQUEST CREATORS

func (m *Monitor) AddGlobalAggregationFuncReq(key int64, interestSet body_types.GlobalAggregationFunction) {
	m.babel.SendRequest(NewAddGlobalAggregationFuncReq(key, interestSet), m.ID(), m.ID())
}

// BROADCAST TIMER

func (m *Monitor) handleRebroadcastGlobalInterestSetsTimer(t timer.Timer) {

	m.babel.RegisterTimer(
		m.ID(),
		NewBroadcastGlobalAggregationFuncsTimer(RebroadcastGlobalAggFuncTimerDuration),
	)

	m.broadcastGlobalAggFuncsToChildren()
	m.broadcastGlobalAggFuncsToParent()
}

func (m *Monitor) broadcastGlobalAggFuncsToChildren() {
	for _, child := range m.currView.Children {
		globalIntSetstoSend := make(map[int64]body_types.GlobalAggregationFunction)
		for aggFuncID, is := range m.globalAggFuncs {
			for _, sub := range is.subscribers {

				if peer.PeersEqual(sub.p, child) { // dont propagate interest sets for nodes with it already present
					continue
				}

				globalIntSetstoSend[aggFuncID] = is.AF
			}
		}
		if len(globalIntSetstoSend) > 0 {
			toSend := NewInstallGlobalAggFuncMessage(globalIntSetstoSend)
			m.babel.SendMessage(toSend, child, m.ID(), m.ID())
		}
	}
}

func (m *Monitor) broadcastGlobalAggFuncsToParent() {
	if m.currView.Parent == nil {
		return
	}

	globalIntSetstoSend := make(map[int64]body_types.GlobalAggregationFunction)
	for aggFuncID, is := range m.globalAggFuncs {
		for _, sub := range is.subscribers {
			if peer.PeersEqual(sub.p, m.currView.Parent) { // dont propagate interest sets for nodes with it already present
				continue
			}
			globalIntSetstoSend[aggFuncID] = is.AF
		}
	}

	if len(globalIntSetstoSend) > 0 {
		toSend := NewInstallGlobalAggFuncMessage(globalIntSetstoSend)
		m.babel.SendMessage(toSend, m.currView.Parent, m.ID(), m.ID())
	}
}

// PROPAGATE MESSAGE HANDLER

func (m *Monitor) handlePropagateGlobalAggFuncMetricsMessage(sender peer.Peer, msg message.Message) {

	msgConverted := msg.(PropagateGlobalAggFuncMetricsMsg)
	interestSetID := msgConverted.InterestSetID

	globalAggFunc, ok := m.globalAggFuncs[interestSetID]
	if !ok {
		m.logger.Errorf(
			"received propagation of metric values for missing interest set %d: %s from %s",
			interestSetID,
			globalAggFunc.AF.OutputBucketOpts.Name,
			sender.String(),
		)
		return
	}

	if !m.isPeerInView(sender) {
		m.logger.Errorf("received interest set propagation message but target (%s) is not in view", sender)
		return
	}

	m.logger.WithFields(logrus.Fields{"value": msgConverted.Value}).Infof(
		"received propagation of metric value for global agg func %d: %s from %s",
		interestSetID,
		globalAggFunc.AF.OutputBucketOpts.Name,
		sender.String(),
	)
	globalAggFunc.NeighValues[sender.String()] = msgConverted.Value.Fields
}

// func (m *Monitor) handleExportGlobalAggFuncFuncTimer(t timer.Timer) {
// 	tConverted := t.(*ExportGlobalAggregationFuncTimer)
// 	interestSetID := tConverted.InterestSetID
// 	globalAggFunc, ok := m.globalAggFuncs[interestSetID]
// 	m.logger.Infof("Export timer for global aggregation func %d triggered", interestSetID)

// 	if !ok {
// 		m.logger.Warnf("Canceling export timer for global aggregation func %d", interestSetID)
// 		return
// 	}

// 	m.logger.Infof(
// 		"Exporting metric values for global aggregation func %d: %s",
// 		interestSetID,
// 		globalAggFunc.AF.OutputBucketOpts.Name,
// 	)

// 	query := globalAggFunc.AF.Query
// 	queryResult, err := m.me.MakeQuerySingleReturn(query.Expression, query.Timeout)

// 	if err != nil {
// 		globalAggFunc.nrRetries++
// 		m.logger.Errorf(
// 			"Remote global aggregation func query failed to process with err %s (%d/%d)",
// 			err,
// 			globalAggFunc.nrRetries,
// 			globalAggFunc.AF.MaxRetries,
// 		)

// 		if globalAggFunc.nrRetries >= globalAggFunc.AF.MaxRetries {
// 			m.logger.Errorf("Aborting export timer for global aggregation func %d", interestSetID)
// 			return // abort timer
// 		}

// 		m.babel.RegisterTimer(
// 			m.ID(),
// 			NewExportGlobalAggregationFuncTimer(
// 				globalAggFunc.AF.OutputBucketOpts.Granularity.Granularity,
// 				interestSetID,
// 			),
// 		)
// 		return
// 	}

// 	m.logger.Infof(
// 		"global aggregation function query result: (%+v)",
// 		queryResult,
// 	)

// 	var isLocal bool
// 	if _, ok := globalAggFunc.subscribers[m.babel.SelfPeer().String()]; ok {
// 		isLocal = true
// 	}

// 	// propagate values from
// 	if isLocal {
// 		var mergedVal map[string]interface{}
// 		if len(globalAggFunc.NeighValues) > 0 {
// 			valuesToMerge := []map[string]interface{}{}
// 			for _, childVal := range globalAggFunc.NeighValues {
// 				valuesToMerge = append(valuesToMerge, childVal)
// 			}

// 			valuesToMerge = append(valuesToMerge, queryResult)

// 			m.logger.Infof(
// 				"Merging values: (%+v)",
// 				valuesToMerge,
// 			)

// 			mergedVal, err = m.me.RunMergeFunc(globalAggFunc.AF.MergeFunction.Expression, globalAggFunc.AF.MergeFunction.Timeout, valuesToMerge)
// 			if err != nil {
// 				panic(err)
// 			}
// 		} else {
// 			m.logger.Warn("Not merging values due to not having children values")
// 			mergedVal = queryResult
// 		}
// 		err := m.tsdb.AddMetric(
// 			globalAggFunc.AF.OutputBucketOpts.Name,
// 			make(map[string]string), // TODO is this correct? merged points will have no tags
// 			mergedVal,
// 			time.Now(),
// 		)
// 		if err != nil {
// 			m.logger.Panic(err)
// 		}

// 		for _, sub := range globalAggFunc {

// 		}

// 	}

// 	toSendMsg := NewPropagateTreeAggFuncMetricsMessage(interestSetID, &body_types.ObservableDTO{TS: time.Now(), Fields: mergedVal})

// 	if peer.PeersEqual(m.currView.Parent, globalAggFunc.sender) {
// 		m.logger.Infof(
// 			"propagating metrics for global aggregation function %d (%+v) to parent %s",
// 			interestSetID,
// 			mergedVal,
// 			m.currView.Parent.String(),
// 		)
// 		m.babel.SendMessage(toSendMsg, m.currView.Parent, m.ID(), m.ID())
// 	} else {
// 		m.logger.Errorf(
// 			"global aggregation function %d could not propagate to parent due to not having parent",
// 			interestSetID,
// 		)
// 	}

// 	m.babel.RegisterTimer(
// 		m.ID(),
// 		NewExportGlobalAggregationFuncTimer(
// 			globalAggFunc.AggSet.OutputBucketOpts.Granularity.Granularity,
// 			interestSetID,
// 		),
// 	)
// }
