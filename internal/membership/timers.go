package membership

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/timer"
)

const joinTimerID = 1000

type joinTimer struct {
	timer *time.Timer
}

func NewJoinTimer(duration time.Duration) timer.Timer {
	return &joinTimer{
		timer: time.NewTimer(duration),
	}
}

func (t *joinTimer) ID() timer.ID {
	return joinTimerID
}

func (t *joinTimer) Wait() {
	<-t.timer.C
}

const retryTimerID = 1001

type retryTimer struct {
	timer *time.Timer
}

func NewRetryTimer(duration time.Duration) timer.Timer {
	return &retryTimer{
		timer: time.NewTimer(duration),
	}
}

func (t *retryTimer) ID() timer.ID {
	return retryTimerID
}

func (t *retryTimer) Wait() {
	<-t.timer.C
}

const parentRefreshTimerID = 1002

// This timer represents a timer to send a message to the children of a node, informing them about the parent and the grandparent
type parentRefreshTimer struct {
	timer *time.Timer
	Child peer.Peer
}

func NewParentRefreshTimer(duration time.Duration, children peer.Peer) timer.Timer {
	return &parentRefreshTimer{
		timer: time.NewTimer(duration),
		Child: children,
	}
}

func (t *parentRefreshTimer) ID() timer.ID {
	return parentRefreshTimerID
}

func (t *parentRefreshTimer) Wait() {
	<-t.timer.C
}
