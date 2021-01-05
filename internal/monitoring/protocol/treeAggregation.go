package protocol

import (
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

// BROADCAST TIMER

func (m *Monitor) handleBroadcastTreeInterestSetsTimer(t timer.Timer) {
	m.babel.RegisterTimer(
		m.ID(),
		NewBroadcastTreeAggregationFuncsTimer(RebroadcastTreeInterestSetsTimerDuration),
	)

	treeAggFuncstoSend := make(map[int64]treeAggSet)

	for isID, is := range m.treeAggFuncs {
		if is.AggSet.Levels > 0 {
			treeAggFuncstoSend[isID] = treeAggSet{
				AggSet: body_types.TreeAggregationSet{
					MaxRetries:       is.AggSet.MaxRetries,
					Query:            is.AggSet.Query,
					OutputBucketOpts: is.AggSet.OutputBucketOpts,
					MergeFunction:    is.AggSet.MergeFunction,
					Levels:           is.AggSet.Levels - 1,
				},
			}
		}
	}

	if len(treeAggFuncstoSend) == 0 {
		return
	}

	toSend := NewInstallTreeAggFuncMessage(treeAggFuncstoSend)
	for _, child := range m.currView.Children {
		m.babel.SendMessage(toSend, child, m.ID(), m.ID())
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

	valuesToMerge := []tsdb.Observable{}
	for _, childVal := range treeAggFunc.childValues {
		valuesToMerge = append(valuesToMerge, childVal)
	}
	valuesToMerge = append(valuesToMerge, queryResult.Last())

	mergedVal, err := m.me.RunMergeFunc(treeAggFunc.AggSet.MergeFunction.Expression, treeAggFunc.AggSet.MergeFunction.Timeout, valuesToMerge)
	if err != nil {
		panic(err)
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
	}

	if treeAggFunc.parent {
		toSendMsg := NewPropagateTreeAggFuncMetricsMessage(interestSetID, &body_types.ObservableDTO{TS: time.Now(), Fields: mergedVal})
		if m.currView.Parent != nil {
			m.babel.SendMessage(toSendMsg, m.currView.Parent, m.ID(), m.ID())
		} else {
			m.logger.Errorf(
				"tree aggregation function %d could not propagate to parent due to not having parent",
				interestSetID,
			)
		}
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
			"received propagation of metric values for missing tree agg func %d: %s from %s",
			treeAggSetID,
			treeAggSet.AggSet.OutputBucketOpts.Name,
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
	treeAggSet.childValues[sender.String()] = tsdb.NewObservable(msgConverted.Value.Fields, msgConverted.Value.TS)
}

func (m *Monitor) handleInstallTreeAggFuncMetricsMessage(sender peer.Peer, msg message.Message) {
	installTreeAggFuncMsg := msg.(InstallTreeAggFuncMsg)
	_, _, parent := m.getPeerRelationshipType(sender)
	if !parent {
		m.logger.Warn("received install neigh interest set from peer not in my view")
		return
	}

	m.logger.Infof(
		"received message to install neigh interest sets from %s (%+v)",
		sender.String(),
		installTreeAggFuncMsg,
	)

	for treeAggFuncID, treeAggFunc := range installTreeAggFuncMsg.InterestSets {
		m.logger.Infof("installing neigh interest set %d: %+v", treeAggFuncID, treeAggFunc)

		is, alreadyExists := m.treeAggFuncs[treeAggFuncID]

		if alreadyExists {
			is.parent = true
			m.logger.Info("Neigh interest set already present")
			continue
		}

		m.treeAggFuncs[treeAggFuncID] = &treeAggSet{
			nrRetries:   0,
			childValues: make(map[string]tsdb.Observable),
			local:       false,
			parent:      true,
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportTreeAggregationFuncTimer(
				treeAggFunc.AggSet.OutputBucketOpts.Granularity.Granularity,
				treeAggFuncID,
			),
		)
	}
}

// CLEANUP

func (m *Monitor) cleanupTreeInterestSets() {
	// TODO
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
		childValues: make(map[string]tsdb.Observable),
		local:       true,
		parent:      false,
	}
	frequency := interestSet.OutputBucketOpts.Granularity.Granularity
	m.logger.Infof("Installing local tree aggregation function %d: %+v", interestSetID, interestSet)
	m.babel.RegisterTimer(m.ID(), NewExportTreeAggregationFuncTimer(frequency, interestSetID))
	return nil
}

// REQUEST CREATORS

func (m *Monitor) AddTreeAggregationFuncReq(key int64, interestSet body_types.TreeAggregationSet) {
	m.babel.SendRequest(NewAddTreeAggregationFuncReq(key, interestSet), m.ID(), m.ID())
}
