package monitoring_proto

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

const RebroadcastInterestSetTimerID = 6000

type rebroadcastInterestSetTimer struct {
	deadline      time.Time
	InterestSetId uint64
}

func NewRebroadcastInterestSetTimer(duration time.Duration, interestSetId uint64) timer.Timer {
	return &rebroadcastInterestSetTimer{
		deadline:      time.Now().Add(duration),
		InterestSetId: interestSetId,
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
	InterestSetId uint64
}

func NewExportNeighInterestSetMetricsTimer(duration time.Duration, interestSetId uint64) timer.Timer {
	return &exportNeighInterestSetMetricsTimer{
		deadline:      time.Now().Add(duration),
		InterestSetId: interestSetId,
	}
}

func (t *exportNeighInterestSetMetricsTimer) ID() timer.ID {
	return ExportNeighInterestSetMetricsTimerID
}

func (t *exportNeighInterestSetMetricsTimer) Deadline() time.Time {
	return t.deadline
}

const CheckInterestSetPeerInViewTimerID = 6002

type checkInterestSetPeerInViewTimer struct {
	deadline      time.Time
	InterestSetId uint64
}

func NewCheckInterestSetPeerInViewTimer(duration time.Duration, interestSetId uint64) timer.Timer {
	return &checkInterestSetPeerInViewTimer{
		deadline:      time.Now().Add(duration),
		InterestSetId: interestSetId,
	}
}

func (t *checkInterestSetPeerInViewTimer) ID() timer.ID {
	return CheckInterestSetPeerInViewTimerID
}

func (t *checkInterestSetPeerInViewTimer) Deadline() time.Time {
	return t.deadline
}
