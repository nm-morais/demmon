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

type neighValue struct {
	sender    peer.Peer
	values    map[string]interface{}
	timestamp time.Time
}

type treeAggSet struct {
	nrRetries                       int
	AggSet                          *body_types.TreeAggregationSet
	childValues                     map[string]neighValue
	local                           bool
	sender                          *peer.IPeer
	lastRefresh                     time.Time
	lastPropagationMembershipChange time.Time
	timerID                         int
	intermediateValuesTimerID       int
}

// REQUEST CREATORS

func (m *Monitor) AddTreeAggregationFuncReq(key int64, interestSet *body_types.TreeAggregationSet) {
	m.babel.SendRequest(NewAddTreeAggregationFuncReq(key, interestSet), m.ID(), m.ID())
}

// BROADCAST TIMER

func (m *Monitor) handleRebroadcastTreeInterestSetsTimer(t timer.Timer) {
	if m.currView.Parent != nil {
		m.requestInterestSets(true)
	}
}

func (m *Monitor) requestInterestSets(batch bool) {

	if m.currView.Parent == nil {
		m.logger.Warn("Attempt to send requestInterestSetMessage to nil parent")
		return
	}

	treeAggFuncstoSend := make([]int64, 0, len(m.treeAggFuncs))
	for isID := range m.treeAggFuncs {
		treeAggFuncstoSend = append(treeAggFuncstoSend, isID)
	}

	toSend := NewRequestTreeAggFuncMessage(treeAggFuncstoSend)
	m.SendMessage(toSend, m.currView.Parent, batch)
}

// EXPORT TIMER

func (m *Monitor) handleExportTreeAggregationFuncTimer(t timer.Timer) {
	tConverted := t.(*exportTreeAggregationFuncTimer)
	interestSetID := tConverted.InterestSetID
	treeAggFunc, ok := m.treeAggFuncs[interestSetID]

	if !ok {
		m.logger.Warnf("Canceling export timer for tree aggregation func %d", interestSetID)
		return
	}

	if !peer.PeersEqual(m.currView.Parent, treeAggFunc.sender) && !treeAggFunc.local {
		m.logger.Errorf(
			"tree aggregation function %d could not propagate to parent because sender is not same as current parent",
			interestSetID,
		)
		return
	}

	m.logger.Infof(
		"Exporting metric values for tree aggregation func %d: %s",
		interestSetID,
		treeAggFunc.AggSet.OutputBucketOpts.Name,
	)
	m.propagateTreeIntSetMetrics(interestSetID, treeAggFunc, false)
}

func (m *Monitor) handleExportTreeAggregationFuncIntermediateValuesTimer(t timer.Timer) {
	tConverted := t.(*exportTreeAggregationFuncIntermediateValuesTimer)
	interestSetID := tConverted.InterestSetID
	treeAggFunc, ok := m.treeAggFuncs[interestSetID]

	if !ok {
		m.logger.Warnf("Canceling export timer for tree aggregation func %d", interestSetID)
		m.babel.CancelTimer(treeAggFunc.intermediateValuesTimerID)
		return
	}

	if !treeAggFunc.local {
		m.logger.Panicf("Exporting intermediate values for non-local tree aggregation set %d", interestSetID)
	}

	m.logger.Infof("Storing intermediate values for tree agg func %d", interestSetID)
	for _, v := range treeAggFunc.childValues {
		if !m.isPeerInView(v.sender) {
			continue
		}
		err := m.tsdb.AddMetric(
			treeAggFunc.AggSet.IntermediateBucketOpts.Name,
			map[string]string{"host": v.sender.IP().String()},
			v.values,
			time.Now(),
		)
		if err != nil {
			m.logger.Panic(err)
		}
	}

}

func (m *Monitor) propagateTreeIntSetMetrics(treeAggFuncID int64, treeAggFunc *treeAggSet, membershipChange bool) {

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
			m.logger.Panicf("Aborting export timer for tree aggregation func %d", treeAggFuncID)
			m.babel.CancelTimer(treeAggFunc.timerID) // abort timer
			return
		}
		return
	}

	// m.logger.Infof(
	// 	"tree aggregation function query result: (%+v)",
	// 	queryResult,
	// )

	var mergedVal map[string]interface{}
	mergedVal = queryResult
	if len(treeAggFunc.childValues) > 0 {
		valuesToMerge := []map[string]interface{}{}
		for _, childVal := range treeAggFunc.childValues {
			valuesToMerge = append(valuesToMerge, childVal.values)
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
	// m.logger.Infof(
	// 	"Merged value: (%+v)",
	// 	mergedVal,
	// )

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
	}

	if treeAggFunc.sender != nil {
		if peer.PeersEqual(treeAggFunc.sender, m.babel.SelfPeer()) {
			return
		}

		toSendMsg := NewPropagateTreeAggFuncMetricsMessage(treeAggFuncID, int64(treeAggFunc.AggSet.Levels), &body_types.ObservableDTO{TS: time.Now(), Fields: mergedVal}, membershipChange)
		m.logger.Infof(
			"propagating metrics for tree aggregation function %d (%+v) to: %s",
			treeAggFuncID,
			mergedVal,
			treeAggFunc.sender.String(),
		)

		m.SendMessage(toSendMsg, treeAggFunc.sender, !membershipChange)
	}
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

	_, ok = treeAggSet.childValues[sender.String()]
	treeAggSet.childValues[sender.String()] = neighValue{
		sender:    sender,
		values:    msgConverted.Value.Fields,
		timestamp: time.Now(),
	}

	if msgConverted.MembershipChange || !ok {
		if treeAggSet.AggSet.UpdateOnMembershipChange {
			if time.Since(treeAggSet.lastPropagationMembershipChange) > treeAggSet.AggSet.MaxFrequencyUpdateOnMembershipChange {
				m.logger.Infof("Propagating int set %d values since there was a membership change", treeAggSetID)
				m.propagateTreeIntSetMetrics(treeAggSetID, treeAggSet, msgConverted.MembershipChange)
				treeAggSet.lastPropagationMembershipChange = time.Now()
			}
		}
	}

}

func (m *Monitor) handleRequestTreeAggFuncMsg(sender peer.Peer, msg message.Message) {
	requestTreeAggFuncMsg := msg.(RequestTreeAggFuncMsg)
	aux := map[int64]bool{}
	for _, intSetID := range requestTreeAggFuncMsg.OwnedIntSets {
		aux[intSetID] = true
	}

	treeAggFuncsToInstall := make(map[int64]*body_types.TreeAggregationSet)
	treeAggFuncsToConfirm := make([]int64, 0)

	for ID, owned := range m.treeAggFuncs {
		if _, ok := aux[ID]; ok {
			treeAggFuncsToConfirm = append(treeAggFuncsToConfirm, ID)
			continue
		}

		if owned.AggSet.Levels == 0 {
			continue
		}

		if owned.AggSet.Levels == -1 {
			treeAggFuncsToInstall[ID] = &body_types.TreeAggregationSet{
				MaxRetries:                           owned.AggSet.MaxRetries,
				Query:                                owned.AggSet.Query,
				OutputBucketOpts:                     owned.AggSet.OutputBucketOpts,
				MergeFunction:                        owned.AggSet.MergeFunction,
				Levels:                               owned.AggSet.Levels,
				UpdateOnMembershipChange:             owned.AggSet.UpdateOnMembershipChange,
				MaxFrequencyUpdateOnMembershipChange: owned.AggSet.MaxFrequencyUpdateOnMembershipChange,
			}
			continue
		}

		treeAggFuncsToInstall[ID] = &body_types.TreeAggregationSet{
			MaxRetries:                           owned.AggSet.MaxRetries,
			Query:                                owned.AggSet.Query,
			OutputBucketOpts:                     owned.AggSet.OutputBucketOpts,
			MergeFunction:                        owned.AggSet.MergeFunction,
			UpdateOnMembershipChange:             owned.AggSet.UpdateOnMembershipChange,
			MaxFrequencyUpdateOnMembershipChange: owned.AggSet.MaxFrequencyUpdateOnMembershipChange,
			Levels:                               owned.AggSet.Levels - 1,
		}
	}

	replyMsg := NewInstallTreeAggFuncMessage(treeAggFuncsToInstall, treeAggFuncsToConfirm)
	m.SendMessage(replyMsg, sender, false)
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

	treeAggFuncsToInstall := make(map[int64]*body_types.TreeAggregationSet)

	for _, treeAggFuncID := range installTreeAggFuncMsg.ConfirmedIntSets {
		alreadyExisting, ok := m.treeAggFuncs[treeAggFuncID]
		if ok {
			if !peer.PeersEqual(alreadyExisting.sender, sender) {
				alreadyExisting.sender = sender.(*peer.IPeer)
				m.propagateTreeIntSetMetrics(treeAggFuncID, alreadyExisting, true)
			}
			alreadyExisting.lastRefresh = time.Now()
			m.treeAggFuncs[treeAggFuncID] = alreadyExisting
			continue
		}
	}

	for treeAggFuncID, treeAggFunc := range installTreeAggFuncMsg.InterestSetsToInstall {
		m.addOrUpdateTreeAggFunc(treeAggFuncID, treeAggFunc, sender)
		if treeAggFunc.Levels == 0 {
			continue
		}
		treeAggFuncsToInstall[treeAggFuncID] = treeAggFunc

	}

	if len(treeAggFuncsToInstall) == 0 {
		// m.logger.Info("Did not receive any new tree agg funcs in InstallTreeAggFunc message, returning")
		return
	}

	toSend := NewInstallTreeAggFuncMessage(treeAggFuncsToInstall, []int64{})
	for _, c := range m.currView.Children {
		m.SendMessage(toSend, c, false)
	}

}

func (m *Monitor) addOrUpdateTreeAggFunc(treeAggFuncID int64, treeAggFunc *body_types.TreeAggregationSet, sender peer.Peer) {
	alreadyExisting, ok := m.treeAggFuncs[treeAggFuncID]
	if ok {
		if !peer.PeersEqual(alreadyExisting.sender, sender) {
			alreadyExisting.sender = sender.(*peer.IPeer)
			m.propagateTreeIntSetMetrics(treeAggFuncID, alreadyExisting, true)
		}
		alreadyExisting.lastRefresh = time.Now()
		m.treeAggFuncs[treeAggFuncID] = alreadyExisting
		return
	}

	m.logger.Infof("installing tree aggregation function %d: %+v", treeAggFuncID, treeAggFunc)
	timerID := m.babel.RegisterPeriodicTimer(
		m.ID(),
		NewExportTreeAggregationFuncTimer(
			treeAggFunc.OutputBucketOpts.Granularity.Granularity,
			treeAggFuncID,
		),
		true,
	)

	m.treeAggFuncs[treeAggFuncID] = &treeAggSet{
		nrRetries:                       0,
		AggSet:                          treeAggFunc,
		childValues:                     map[string]neighValue{},
		local:                           false,
		sender:                          sender.(*peer.IPeer),
		lastRefresh:                     time.Now(),
		lastPropagationMembershipChange: time.Time{},
		timerID:                         timerID,
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
			if is.AggSet.StoreIntermediateValues {
				m.babel.CancelTimer(is.timerID)
			}
			m.babel.CancelTimer(is.timerID)
			continue
		}

		changed := false
		for k, childValue := range is.childValues {
			if time.Since(childValue.timestamp) > ExpireTreeAggFuncValues {
				delete(is.childValues, k)
				changed = true
			}
		}

		if !changed {
			continue
		}

		if is.AggSet.UpdateOnMembershipChange {
			if time.Since(is.lastPropagationMembershipChange) > is.AggSet.MaxFrequencyUpdateOnMembershipChange {
				// m.logger.Infof("Propagating int set %d values since timeout expired and a value was removed", isID)
				m.propagateTreeIntSetMetrics(isID, is, true)
				is.lastPropagationMembershipChange = time.Now()
			}
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
		intermediateValuesTimerID := m.babel.RegisterPeriodicTimer(m.ID(), NewExportTreeAggregationFuncTimer(interestSet.IntermediateBucketOpts.Granularity.Granularity, interestSetID), false)
		existing.intermediateValuesTimerID = intermediateValuesTimerID
		m.treeAggFuncs[interestSetID] = existing
		return nil
	}
	frequency := interestSet.OutputBucketOpts.Granularity.Granularity
	timerID := m.babel.RegisterPeriodicTimer(m.ID(), NewExportTreeAggregationFuncTimer(frequency, interestSetID), true)
	var intermediateValuesTimerID int
	if interestSet.StoreIntermediateValues {
		t := NewExportTreeAggregationIntermediateValuesFuncTimer(interestSet.IntermediateBucketOpts.Granularity.Granularity, interestSetID)
		intermediateValuesTimerID = m.babel.RegisterPeriodicTimer(m.ID(), t, false)
	}
	m.treeAggFuncs[interestSetID] = &treeAggSet{
		nrRetries:                       0,
		AggSet:                          addTreeAggFuncSetReq.InterestSet,
		childValues:                     map[string]neighValue{},
		local:                           true,
		sender:                          nil,
		lastRefresh:                     time.Now(),
		lastPropagationMembershipChange: time.Time{},
		timerID:                         timerID,
		intermediateValuesTimerID:       intermediateValuesTimerID,
	}
	m.logger.Infof("Installing local tree aggregation function %d: %+v", interestSetID, interestSet)
	return nil
}

// HANDLE NODE DOWN
func (m *Monitor) handleNodeDownTreeAggFunc(nodeDown peer.Peer, crash bool) {

	// remove all child values from tree agg func
	for treeAggFuncID, treeAggFunc := range m.treeAggFuncs {
		if _, ok := treeAggFunc.childValues[nodeDown.String()]; ok {
			delete(treeAggFunc.childValues, nodeDown.String())
			if treeAggFunc.AggSet.UpdateOnMembershipChange {
				if time.Since(treeAggFunc.lastPropagationMembershipChange) > treeAggFunc.AggSet.MaxFrequencyUpdateOnMembershipChange {
					m.logger.Infof("Propagating int set %d values since node went down", treeAggFuncID)
					m.propagateTreeIntSetMetrics(treeAggFuncID, treeAggFunc, true)
					treeAggFunc.lastPropagationMembershipChange = time.Now()
				}
			}
		}
	}
}

// HANDLE NODE UP
func (m *Monitor) handleNodeUpTreeAggFunc(nodeUp peer.Peer) {
	// remove all child values from tree agg func
	_, _, isParent := m.getPeerRelationshipType(nodeUp)
	if isParent {
		m.requestInterestSets(false)
	}
}
