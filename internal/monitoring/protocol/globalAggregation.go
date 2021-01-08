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

// ADD REQUEST HANDLER

func (m *Monitor) handleAddGlobalAggFuncRequest(req request.Request) request.Reply {
	addGlobalAggFuncReq := req.(AddGlobalAggregationFuncReq)
	interestSet := addGlobalAggFuncReq.AggFunc
	interestSetID := addGlobalAggFuncReq.Id

	existing, ok := m.globalAggFuncs[addGlobalAggFuncReq.Id]
	if ok {
		existing.subscribers[m.babel.SelfPeer().String()] = sub{
			p: m.babel.SelfPeer(),
		}
		return nil
	}

	m.globalAggFuncs[addGlobalAggFuncReq.Id] = &globalAggFunc{
		NeighValues: make(map[string]map[string]interface{}),
		AF:          addGlobalAggFuncReq.AggFunc,
		nrRetries:   0,
		subscribers: map[string]sub{
			m.babel.SelfPeer().String(): {
				p: m.babel.SelfPeer(),
			},
		},
	}
	frequency := interestSet.OutputBucketOpts.Granularity.Granularity
	m.logger.Infof("Installing local insterest set %d: %+v", interestSetID, interestSet)
	m.babel.RegisterTimer(m.ID(), NewExportGlobalAggregationFuncTimer(frequency, interestSetID))
	return nil
}

// BROADCAST AGG FUNCS TIMER

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

// BROADCAST TO PARENT

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
	globalAggFunc.NeighValues[sender.String()] = msgConverted.Value.Fields
}

// EXPORT METRICS TIMER

func (m *Monitor) handleExportGlobalAggFuncFuncTimer(t timer.Timer) {
	tConverted := t.(*ExportGlobalAggregationFuncTimer)
	interestSetID := tConverted.InterestSetID
	globalAggFunc, ok := m.globalAggFuncs[interestSetID]
	m.logger.Infof("Export timer for global aggregation func %d triggered", interestSetID)

	if !ok {
		m.logger.Warnf("Canceling export timer for global aggregation func %d", interestSetID)
		return
	}

	m.logger.Infof(
		"Exporting metric values for global aggregation func %d: %s",
		interestSetID,
		globalAggFunc.AF.OutputBucketOpts.Name,
	)

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

	m.logger.Infof(
		"global aggregation function query result: (%+v)",
		queryResult,
	)

	var isLocal bool
	if _, ok := globalAggFunc.subscribers[m.babel.SelfPeer().String()]; ok {
		isLocal = true
	}

	var mergedVal map[string]interface{}
	if len(globalAggFunc.NeighValues) > 0 {

		valuesToMerge := []map[string]interface{}{queryResult}

		m.logger.Infof(
			"Neigh values: (%+v)",
			globalAggFunc.NeighValues,
		)

		m.logger.Infof(
			"Merging values: (%+v)",
			valuesToMerge,
		)

		mergedVal, err = m.me.RunMergeFunc(globalAggFunc.AF.MergeFunction.Expression, globalAggFunc.AF.MergeFunction.Timeout, valuesToMerge)
		if err != nil {
			panic(err)
		}
	} else {
		m.logger.Warn("Not merging values due to not having neigh values")
		mergedVal = queryResult
	}

	m.logger.Infof("Merged values: %+v", mergedVal)

	if isLocal {
		m.logger.Infof("Inserting into db values: %+v", mergedVal)
		err := m.tsdb.AddMetric(
			globalAggFunc.AF.OutputBucketOpts.Name,
			make(map[string]string), // TODO is this correct? merged points will have no tags
			mergedVal,
			time.Now(),
		)
		if err != nil {
			m.logger.Panic(err)
		}
	}

	for subID, sub := range globalAggFunc.subscribers {
		if peer.PeersEqual(sub.p, m.babel.SelfPeer()) {
			continue
		}

		subVal, ok := globalAggFunc.NeighValues[subID]
		if !ok {
			m.logger.Warnf(
				"Propagating values (%+v) from global agg func without performing difference from incomming value",
				mergedVal,
			)
			toSendMsg := NewPropagateGlobalAggFuncMetricsMessage(interestSetID, &body_types.ObservableDTO{TS: time.Now(), Fields: mergedVal})
			m.babel.SendMessage(toSendMsg, sub.p, m.ID(), m.ID())
			continue
		}

		args := []map[string]interface{}{}
		args = append(args, mergedVal, subVal)
		m.logger.Infof(
			"Performing difference among values: (%+v)",
			args,
		)
		differenceResult, err := m.me.RunMergeFunc(globalAggFunc.AF.DifferenceFunction.Expression, globalAggFunc.AF.DifferenceFunction.Timeout, args)
		if err != nil {
			panic(err)
		}
		m.logger.Infof(
			"Difference result: (%+v)",
			differenceResult,
		)
		toSendMsg := NewPropagateGlobalAggFuncMetricsMessage(interestSetID, &body_types.ObservableDTO{TS: time.Now(), Fields: differenceResult})
		m.logger.Infof(
			"propagating metrics for global aggregation function %d (%+v) to peer %s",
			interestSetID,
			mergedVal,
			sub.p.String(),
		)
		m.babel.SendMessage(toSendMsg, sub.p, m.ID(), m.ID())
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

			found := false

			for _, child := range m.currView.Children {
				if peer.PeersEqual(sub.p, child) {
					found = true
					break
				}
			}

			if !found && !peer.PeersEqual(sub.p, m.currView.Parent) {
				m.logger.Errorf(
					"Removing peer %s from global agg func %d because peer is not parent of children",
					sub.p.String(),
					isID,
				)
				delete(is.subscribers, k)
				delete(is.NeighValues, k)
				continue
			}

			if time.Since(sub.lastRefresh) > ExpireGlobalAggFuncTimeout {
				m.logger.Errorf(
					"Removing peer %s from global agg func %d because entry expired",
					sub.p.String(),
					isID,
				)
				delete(is.subscribers, k)
				delete(is.NeighValues, k)
				continue
			}
		}

		if len(is.subscribers) == 0 {
			delete(m.globalAggFuncs, isID)
		}
	}
}

// INSTALL MESSAGE HANDLER

func (m *Monitor) handleInstallGlobalAggFuncMessage(sender peer.Peer, msg message.Message) {
	installGlobalAggFuncMsg := msg.(InstallGlobalAggFuncMsg)

	if !m.isPeerInView(sender) {
		m.logger.Warn("received install global interest set from peer not in my view")
		return
	}

	m.logger.Infof(
		"received message to install global interest sets from %s (%+v)",
		sender.String(),
		installGlobalAggFuncMsg,
	)

	for interestSetID, interestSet := range installGlobalAggFuncMsg.InterestSets {
		m.logger.Infof("installing global interest set %d: %+v", interestSetID, interestSet)

		is, alreadyExists := m.globalAggFuncs[interestSetID]

		if alreadyExists {
			is.subscribers[sender.String()] = sub{
				p:           sender,
				lastRefresh: time.Now(),
			}
			m.logger.Info("Global interest set already present")
			continue
		}

		m.globalAggFuncs[interestSetID] = &globalAggFunc{
			nrRetries: 0,
			subscribers: map[string]sub{
				sender.String(): {
					p:           sender,
					lastRefresh: time.Now(),
				},
			},
			NeighValues: map[string]map[string]interface{}{},
			AF:          interestSet,
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
