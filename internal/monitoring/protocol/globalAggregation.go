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

type sub struct {
	p           peer.Peer
	lastRefresh time.Time
	values      map[string]interface{}
}

type globalAggFunc struct {
	nrRetries                 int
	subscribers               map[string]*sub
	AF                        body_types.GlobalAggregationFunction
	intermediateValuesTimerID int
}

// REQUEST CREATORS

func (m *Monitor) AddGlobalAggregationFuncReq(key int64, interestSet body_types.GlobalAggregationFunction) {
	m.babel.SendRequest(NewAddGlobalAggregationFuncReq(key, interestSet), m.ID(), m.ID())
}

// ADD REQUEST HANDLER

func (m *Monitor) handleAddGlobalAggFuncRequest(req request.Request) request.Reply {
	addGlobalAggFuncReq := req.(AddGlobalAggregationFuncReq)
	interestSet := addGlobalAggFuncReq.AggFunc
	interestSetID := addGlobalAggFuncReq.Id

	existing, ok := m.globalAggFuncs[addGlobalAggFuncReq.Id]
	if ok {
		existing.subscribers[m.babel.SelfPeer().String()] = &sub{
			p:           m.babel.SelfPeer(),
			lastRefresh: time.Time{},
			values:      nil,
		}
		return nil
	}

	m.globalAggFuncs[addGlobalAggFuncReq.Id] = &globalAggFunc{
		AF:        addGlobalAggFuncReq.AggFunc,
		nrRetries: 0,
		subscribers: map[string]*sub{
			m.babel.SelfPeer().String(): {
				p: m.babel.SelfPeer(),
			},
		},
	}

	frequency := interestSet.OutputBucketOpts.Granularity.Granularity
	m.logger.Infof("Installing local interest set %d: %+v", interestSetID, interestSet)
	m.babel.RegisterTimer(m.ID(), NewExportGlobalAggregationFuncTimer(frequency, interestSetID))
	return nil
}

func (m *Monitor) handleExportGlobalAggFuncIntermediateValuesTimer(t timer.Timer) {
	tConverted := t.(*exportGlobalAggregationFuncIntermediateValuesTimer)
	interestSetID := tConverted.InterestSetID
	globalAggFunc, ok := m.globalAggFuncs[interestSetID]

	if !ok {
		m.logger.Warnf("Canceling export timer for global aggregation func %d", interestSetID)
		m.babel.CancelTimer(globalAggFunc.intermediateValuesTimerID)
		return
	}
	_, local := globalAggFunc.subscribers[m.babel.SelfPeer().String()]
	if !local {
		m.logger.Panicf("Exporting intermediate values for non-local global aggregation set %d", interestSetID)
	}

	m.logger.Infof("Storing intermediate values for global agg func %d", interestSetID)
	for _, v := range globalAggFunc.subscribers {
		if !m.isPeerInView(v.p) {
			continue
		}
		if v.values == nil {
			continue
		}

		m.logger.Info("Adding merged values locally")
		err := m.tsdb.AddMetric(
			globalAggFunc.AF.IntermediateBucketOpts.Name,
			map[string]string{"host": v.p.IP().String()},
			v.values,
			time.Now(),
		)
		if err != nil {
			m.logger.Panic(err)
		}
	}

}

// BROADCAST AGG FUNCS TIMER

func (m *Monitor) handleRebroadcastGlobalInterestSetsTimer(t timer.Timer) {
	// m.logger.Infof("Broadcasting global interest sets...")
	m.broadcastGlobalAggFuncsToChildren()
	m.broadcastGlobalAggFuncsToParent()
}

func (m *Monitor) broadcastGlobalAggFuncsToChildren() {
	for _, child := range m.currView.Children {
		globalIntSetstoSend := make(map[int64]body_types.GlobalAggregationFunction)
		for aggFuncID, is := range m.globalAggFuncs {

			_, selfIsSubscriber := is.subscribers[m.babel.SelfPeer().String()]
			if selfIsSubscriber {
				globalIntSetstoSend[aggFuncID] = is.AF
				continue
			}

			if _, ok := is.subscribers[child.String()]; ok {
				// m.logger.Warnf("Not Broadcasting global interest set %d to %s because node is a subscriber",
				// 	aggFuncID,
				// 	child.String())
				continue
			}

			globalIntSetstoSend[aggFuncID] = is.AF
		}

		if len(globalIntSetstoSend) > 0 {
			toSend := NewInstallGlobalAggFuncMessage(globalIntSetstoSend)
			m.SendMessage(toSend, child)
		}
	}
}

// BROADCAST TO PARENT

func (m *Monitor) broadcastGlobalAggFuncsToParent() {
	if m.currView.Parent == nil {
		return
	}

	globalIntSetstoSend := make(map[int64]body_types.GlobalAggregationFunction)
	for aggFuncID, is := range m.globalAggFuncs {

		_, selfIsSubscriber := is.subscribers[m.babel.SelfPeer().String()]
		if selfIsSubscriber {
			globalIntSetstoSend[aggFuncID] = is.AF
			continue
		}

		if _, ok := is.subscribers[m.currView.Parent.String()]; ok {
			m.logger.Warnf("Not Broadcasting global interest set %d to %s because node is a subscriber",
				aggFuncID,
				m.currView.Parent.String())
			continue
		}
		globalIntSetstoSend[aggFuncID] = is.AF
	}

	if len(globalIntSetstoSend) > 0 {
		toSend := NewInstallGlobalAggFuncMessage(globalIntSetstoSend)
		m.SendMessage(toSend, m.currView.Parent)
	}
}

// PROPAGATE MESSAGE HANDLER

func (m *Monitor) handlePropagateGlobalAggFuncMetricsMessage(sender peer.Peer, msg message.Message) {

	msgConverted := msg.(PropagateGlobalAggFuncMetricsMsg)
	interestSetID := msgConverted.InterestSetID

	globalAggFunc, ok := m.globalAggFuncs[interestSetID]
	if !ok {
		m.logger.Errorf(
			"received propagation of metric values for missing interest set %d from %s",
			interestSetID,
			sender.String(),
		)
		return
	}

	if !m.isPeerInView(sender) {
		m.logger.Errorf("received interest set propagation message but target (%s) is not in view", sender)
		return
	}

	found := false

	for _, child := range m.currView.Children {
		if peer.PeersEqual(sender, child) {
			found = true
			break
		}
	}

	if !found && !peer.PeersEqual(sender, m.currView.Parent) {
		m.logger.Errorf("received interest set propagation message but target (%s) is not parent or children", sender)
		return
	}

	m.logger.WithFields(logrus.Fields{"value": msgConverted.Value}).Infof(
		"received propagation of metric value for global agg func %d: %s from %s",
		interestSetID,
		globalAggFunc.AF.OutputBucketOpts.Name,
		sender.String(),
	)
	sub, ok := globalAggFunc.subscribers[sender.String()]
	if !ok {
		m.logger.Errorf("Sub for global agg func %d not present", interestSetID)
		return
	}
	sub.values = msgConverted.Value.Fields
}

// EXPORT METRICS TIMER

func (m *Monitor) handleExportGlobalAggFuncFuncTimer(t timer.Timer) {
	tConverted := t.(*ExportGlobalAggregationFuncTimer)
	interestSetID := tConverted.InterestSetID
	globalAggFunc, ok := m.globalAggFuncs[interestSetID]
	// m.logger.Infof("Export timer for global aggregation func %d triggered", interestSetID)

	if !ok {
		m.logger.Warnf("Canceling export timer for global aggregation func %d", interestSetID)
		return
	}

	// m.logger.Infof(
	// 	"Exporting metric values for global aggregation func %d: %s",
	// 	interestSetID,
	// 	globalAggFunc.AF.OutputBucketOpts.Name,
	// )

	query := globalAggFunc.AF.Query
	queryResult, err := m.me.MakeQuerySingleReturn(query.Expression, query.Timeout)

	if err != nil {
		globalAggFunc.nrRetries++
		m.logger.Errorf(
			"Remote global aggregation func query failed to process with err %s (%d/%d)",
			err,
			globalAggFunc.nrRetries,
			globalAggFunc.AF.MaxRetries,
		)

		if globalAggFunc.nrRetries >= globalAggFunc.AF.MaxRetries {
			m.logger.Errorf("Aborting export timer for global aggregation func %d", interestSetID)
			return // abort timer
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportGlobalAggregationFuncTimer(
				globalAggFunc.AF.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
		return
	}

	// m.logger.Infof(
	// 	"global aggregation function query result: (%+v)",
	// 	queryResult,
	// )

	var isLocal bool
	if _, ok := globalAggFunc.subscribers[m.babel.SelfPeer().String()]; ok {
		isLocal = true
	}

	var mergedVal map[string]interface{}
	if len(globalAggFunc.subscribers) > 0 {

		valuesToMerge := make([]map[string]interface{}, 0, len(globalAggFunc.subscribers)+1)
		valuesToMerge = append(valuesToMerge, queryResult)
		for _, v := range globalAggFunc.subscribers {
			if v.values == nil {
				continue
			}
			valuesToMerge = append(valuesToMerge, v.values)
		}

		m.logger.Infof(
			"Merging value: (%+v) with (%+v)",
			queryResult,
			globalAggFunc.subscribers,
		)

		mergedVal, err = m.me.RunMergeFunc(globalAggFunc.AF.MergeFunction.Expression, globalAggFunc.AF.MergeFunction.Timeout, valuesToMerge)
		if err != nil {
			panic(err)
		}
	} else {
		// m.logger.Warn("Not merging values due to not having neigh values")
		mergedVal = queryResult
	}

	timeNow := time.Now()
	// m.logger.Infof("Merged values: %+v", mergedVal)

	if isLocal {
		// m.logger.Infof("Inserting into db values: %+v", mergedVal)
		err := m.tsdb.AddMetric(
			globalAggFunc.AF.OutputBucketOpts.Name,
			make(map[string]string), // TODO is this correct? merged points will have no tags
			mergedVal,
			timeNow,
		)
		if err != nil {
			m.logger.Panic(err)
		}
	}

	for subID, sub := range globalAggFunc.subscribers {
		if peer.PeersEqual(sub.p, m.babel.SelfPeer()) {
			continue
		}

		if !m.isPeerInView(sub.p) {
			m.SendMessageUDPStream(NewDeleteGlobalAggFuncValMessage(), sub.p)
			delete(globalAggFunc.subscribers, subID)
		}

		subVal := sub.values
		if subVal == nil {
			// m.logger.Warnf(
			// 	"Propagating values (%+v) from global agg func without performing difference from incomming value",
			// 	mergedVal,
			// )
			toSendMsg := NewPropagateGlobalAggFuncMetricsMessage(interestSetID, &body_types.ObservableDTO{TS: timeNow, Fields: mergedVal})
			m.SendMessage(toSendMsg, sub.p)
			continue
		}

		// m.logger.Infof(
		// 	"Performing difference between: (%+v) and: (%+v)",
		// 	mergedVal,
		// 	subVal,
		// )

		differenceResult, err := m.me.RunMergeFunc(
			globalAggFunc.AF.DifferenceFunction.Expression,
			globalAggFunc.AF.DifferenceFunction.Timeout,
			[]map[string]interface{}{mergedVal, subVal},
		)

		if err != nil {
			panic(err)
		}

		// m.logger.Infof(
		// 	"Difference result: (%+v)",
		// 	differenceResult,
		// )

		toSendMsg := NewPropagateGlobalAggFuncMetricsMessage(interestSetID, &body_types.ObservableDTO{TS: timeNow, Fields: differenceResult})
		m.logger.Infof(
			"propagating metrics for global aggregation function %d (%+v) to peer %s",
			interestSetID,
			differenceResult,
			sub.p.String(),
		)
		m.SendMessage(toSendMsg, sub.p)
	}

	m.babel.RegisterTimer(
		m.ID(),
		NewExportGlobalAggregationFuncTimer(
			globalAggFunc.AF.OutputBucketOpts.Granularity.Granularity,
			interestSetID,
		),
	)
}

func (m *Monitor) cleanupGlobalAggFuncs() {
	for isID, is := range m.globalAggFuncs {
		for k, sub := range is.subscribers {
			if peer.PeersEqual(sub.p, m.babel.SelfPeer()) {
				continue
			}

			if time.Since(sub.lastRefresh) > ExpireGlobalAggFuncTimeout {
				m.logger.Errorf(
					"Removing peer %s from global agg func %d because entry expired",
					sub.p.String(),
					isID,
				)
				delete(is.subscribers, k)
				continue
			}
		}

		if len(is.subscribers) == 0 {
			m.logger.Errorf(
				"Removing global agg func %d because it has no subscribers",
				isID,
			)
			delete(m.globalAggFuncs, isID)
		}
	}
}

func (m *Monitor) handleDeleteGlobalAggFuncValuesMessage(sender peer.Peer, msg message.Message) {
	m.logger.Infof("Got deleteGlobalValuesMessage from: %s", sender.String())
	for _, is := range m.globalAggFuncs {
		_, ok := is.subscribers[sender.String()]
		if !ok {
			continue
		}
		delete(is.subscribers, sender.String())
		m.logger.Infof("deleting neigh %s values since node sent deleteGlobalValuesMsg", sender.String())
	}
}

// INSTALL MESSAGE HANDLER

func (m *Monitor) handleInstallGlobalAggFuncMessage(sender peer.Peer, msg message.Message) {
	installGlobalAggFuncMsg := msg.(InstallGlobalAggFuncMsg)

	if !m.isPeerInView(sender) {
		m.logger.Warnf("received install global interest set from peer (%s) not in my view", sender.String())
		return
	}

	for interestSetID, interestSet := range installGlobalAggFuncMsg.InterestSets {

		is, alreadyExists := m.globalAggFuncs[interestSetID]
		if alreadyExists {
			remoteSub, alreadySubbed := is.subscribers[sender.String()]
			if alreadySubbed {
				remoteSub.lastRefresh = time.Now()
				continue
			}

			is.subscribers[sender.String()] = &sub{
				p:           sender,
				lastRefresh: time.Now(),
				values:      nil,
			}
			// m.logger.Info("Global interest set already present")
			continue
		}

		m.logger.Infof("installed global interest set %d: %+v", interestSetID, interestSet)
		m.globalAggFuncs[interestSetID] = &globalAggFunc{
			nrRetries: 0,
			subscribers: map[string]*sub{
				sender.String(): {
					p:           sender,
					lastRefresh: time.Now(),
					values:      nil,
				},
			},
			AF: interestSet,
		}

		m.babel.RegisterTimer(
			m.ID(),
			NewExportGlobalAggregationFuncTimer(
				interestSet.OutputBucketOpts.Granularity.Granularity,
				interestSetID,
			),
		)
	}
}

// HANDLE NODE DOWN

func (m *Monitor) handleNodeDownGlobalAggFunc(nodeDown peer.Peer, crash bool) {

	for aggFuncKey, aggFunc := range m.globalAggFuncs {

		delete(aggFunc.subscribers, nodeDown.String())
		if len(aggFunc.subscribers) == 0 {
			delete(m.globalAggFuncs, aggFuncKey)
		}
	}

}
