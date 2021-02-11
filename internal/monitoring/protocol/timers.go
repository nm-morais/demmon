package protocol

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

const RebroadcastInterestSetTimerID = 6000

type rebroadcastInterestSetTimer struct {
	duration time.Duration
}

func NewRebroadcastInterestSetsTimer(duration time.Duration) timer.Timer {
	return &rebroadcastInterestSetTimer{
		duration: duration,
	}
}

func (t *rebroadcastInterestSetTimer) ID() timer.ID {
	return RebroadcastInterestSetTimerID
}

func (t *rebroadcastInterestSetTimer) Duration() time.Duration {
	return t.duration
}

const ExportNeighInterestSetMetricsTimerID = 6001

type exportNeighInterestSetMetricsTimer struct {
	duration      time.Duration
	InterestSetID int64
}

func NewExportNeighInterestSetMetricsTimer(duration time.Duration, interestSetID int64) timer.Timer {
	return &exportNeighInterestSetMetricsTimer{
		duration:      duration,
		InterestSetID: interestSetID,
	}
}

func (t *exportNeighInterestSetMetricsTimer) ID() timer.ID {
	return ExportNeighInterestSetMetricsTimerID
}

func (t *exportNeighInterestSetMetricsTimer) Duration() time.Duration {
	return t.duration
}

const CleanupInsterestSetsTimerID = 6002

type cleanupInterestSetTimer struct {
	duration time.Duration
}

func NewCleanupInterestSetsTimer(duration time.Duration) timer.Timer {
	return &cleanupInterestSetTimer{
		duration: duration,
	}
}

func (t *cleanupInterestSetTimer) ID() timer.ID {
	return CleanupInsterestSetsTimerID
}

func (t *cleanupInterestSetTimer) Duration() time.Duration {
	return t.duration
}

const ExportTreeAggregationFuncTimerID = 6003

type exportTreeAggregationFuncTimer struct {
	duration      time.Duration
	InterestSetID int64
}

func NewExportTreeAggregationFuncTimer(duration time.Duration, interestSetID int64) timer.Timer {
	return &exportTreeAggregationFuncTimer{
		duration:      duration,
		InterestSetID: interestSetID,
	}
}

func (t *exportTreeAggregationFuncTimer) ID() timer.ID {
	return ExportTreeAggregationFuncTimerID
}

func (t *exportTreeAggregationFuncTimer) Duration() time.Duration {
	return t.duration
}

const RebroadcastTreeAggregationFuncsTimerID = 6004

type BroadcastTreeAggregationFuncsTimer struct {
	duration time.Duration
}

func NewBroadcastTreeAggregationFuncsTimer(duration time.Duration) timer.Timer {
	return &BroadcastTreeAggregationFuncsTimer{
		duration: duration,
	}
}

func (t *BroadcastTreeAggregationFuncsTimer) ID() timer.ID {
	return RebroadcastTreeAggregationFuncsTimerID
}

func (t *BroadcastTreeAggregationFuncsTimer) Duration() time.Duration {
	return t.duration
}

// NewBroadcastGlobalAggregationFuncsTimer

const ExportGlobalAggregationFuncTimerID = 6005

type ExportGlobalAggregationFuncTimer struct {
	duration      time.Duration
	InterestSetID int64
}

func NewExportGlobalAggregationFuncTimer(duration time.Duration, interestSetID int64) timer.Timer {
	return &ExportGlobalAggregationFuncTimer{
		duration:      duration,
		InterestSetID: interestSetID,
	}
}

func (t *ExportGlobalAggregationFuncTimer) ID() timer.ID {
	return ExportGlobalAggregationFuncTimerID
}

func (t *ExportGlobalAggregationFuncTimer) Duration() time.Duration {
	return t.duration
}

const RebroadcastGlobalAggregationFuncsTimerID = 6006

type BroadcastGlobalAggregationFuncsTimer struct {
	duration time.Duration
}

func NewBroadcastGlobalAggregationFuncsTimer(duration time.Duration) timer.Timer {
	return &BroadcastGlobalAggregationFuncsTimer{
		duration: duration,
	}
}

func (t *BroadcastGlobalAggregationFuncsTimer) ID() timer.ID {
	return RebroadcastGlobalAggregationFuncsTimerID
}

func (t *BroadcastGlobalAggregationFuncsTimer) Duration() time.Duration {
	return t.duration
}
