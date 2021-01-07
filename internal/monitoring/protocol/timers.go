package protocol

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

const RebroadcastInterestSetTimerID = 6000

type rebroadcastInterestSetTimer struct {
	deadline time.Time
}

func NewRebroadcastInterestSetsTimer(duration time.Duration) timer.Timer {
	return &rebroadcastInterestSetTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *rebroadcastInterestSetTimer) ID() timer.ID {
	return RebroadcastInterestSetTimerID
}

func (t *rebroadcastInterestSetTimer) Deadline() time.Time {
	return t.deadline
}

const ExportNeighInterestSetMetricsTimerID = 6001

type exportNeighInterestSetMetricsTimer struct {
	deadline      time.Time
	InterestSetID int64
}

func NewExportNeighInterestSetMetricsTimer(duration time.Duration, interestSetID int64) timer.Timer {
	return &exportNeighInterestSetMetricsTimer{
		deadline:      time.Now().Add(duration),
		InterestSetID: interestSetID,
	}
}

func (t *exportNeighInterestSetMetricsTimer) ID() timer.ID {
	return ExportNeighInterestSetMetricsTimerID
}

func (t *exportNeighInterestSetMetricsTimer) Deadline() time.Time {
	return t.deadline
}

const CleanupInsterestSetsTimerID = 6002

type cleanupInterestSetTimer struct {
	deadline time.Time
}

func NewCleanupInterestSetsTimer(duration time.Duration) timer.Timer {
	return &cleanupInterestSetTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *cleanupInterestSetTimer) ID() timer.ID {
	return CleanupInsterestSetsTimerID
}

func (t *cleanupInterestSetTimer) Deadline() time.Time {
	return t.deadline
}

const ExportTreeAggregationFuncTimerID = 6003

type exportTreeAggregationFuncTimer struct {
	deadline      time.Time
	InterestSetID int64
}

func NewExportTreeAggregationFuncTimer(duration time.Duration, interestSetID int64) timer.Timer {
	return &exportTreeAggregationFuncTimer{
		deadline:      time.Now().Add(duration),
		InterestSetID: interestSetID,
	}
}

func (t *exportTreeAggregationFuncTimer) ID() timer.ID {
	return ExportTreeAggregationFuncTimerID
}

func (t *exportTreeAggregationFuncTimer) Deadline() time.Time {
	return t.deadline
}

const RebroadcastTreeAggregationFuncsTimerID = 6004

type BroadcastTreeAggregationFuncsTimer struct {
	deadline time.Time
}

func NewBroadcastTreeAggregationFuncsTimer(duration time.Duration) timer.Timer {
	return &BroadcastTreeAggregationFuncsTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *BroadcastTreeAggregationFuncsTimer) ID() timer.ID {
	return RebroadcastTreeAggregationFuncsTimerID
}

func (t *BroadcastTreeAggregationFuncsTimer) Deadline() time.Time {
	return t.deadline
}

// NewBroadcastGlobalAggregationFuncsTimer

const ExportGlobalAggregationFuncTimerID = 6005

type ExportGlobalAggregationFuncTimer struct {
	deadline      time.Time
	InterestSetID int64
}

func NewExportGlobalAggregationFuncTimer(duration time.Duration, interestSetID int64) timer.Timer {
	return &ExportGlobalAggregationFuncTimer{
		deadline:      time.Now().Add(duration),
		InterestSetID: interestSetID,
	}
}

func (t *ExportGlobalAggregationFuncTimer) ID() timer.ID {
	return ExportGlobalAggregationFuncTimerID
}

func (t *ExportGlobalAggregationFuncTimer) Deadline() time.Time {
	return t.deadline
}

const RebroadcastGlobalAggregationFuncsTimerID = 6006

type BroadcastGlobalAggregationFuncsTimer struct {
	deadline time.Time
}

func NewBroadcastGlobalAggregationFuncsTimer(duration time.Duration) timer.Timer {
	return &BroadcastGlobalAggregationFuncsTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *BroadcastGlobalAggregationFuncsTimer) ID() timer.ID {
	return RebroadcastGlobalAggregationFuncsTimerID
}

func (t *BroadcastGlobalAggregationFuncsTimer) Deadline() time.Time {
	return t.deadline
}
