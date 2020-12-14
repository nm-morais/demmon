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
	InterestSetID uint64
}

func NewExportNeighInterestSetMetricsTimer(duration time.Duration, interestSetID uint64) timer.Timer {
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

const ExportLocalNeighInterestSetMetricsTimerID = 6002

type exportLocalNeighInterestSetMetricsTimer struct {
	deadline      time.Time
	InterestSetID uint64
}

func NewExportLocalNeighInterestSetMetricsTimer(duration time.Duration, interestSetID uint64) timer.Timer {
	return &exportLocalNeighInterestSetMetricsTimer{
		deadline:      time.Now().Add(duration),
		InterestSetID: interestSetID,
	}
}

func (t *exportLocalNeighInterestSetMetricsTimer) ID() timer.ID {
	return ExportLocalNeighInterestSetMetricsTimerID
}

func (t *exportLocalNeighInterestSetMetricsTimer) Deadline() time.Time {
	return t.deadline
}

const CleanupInsterestSetsTimerID = 6003

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
