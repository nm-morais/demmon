package membership

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/timer"
)

const joinTimerID = 1000

type joinTimer struct {
	deadline time.Time
}

func NewJoinTimer(duration time.Duration) timer.Timer {
	return &joinTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *joinTimer) ID() timer.ID {
	return joinTimerID
}

func (t *joinTimer) Deadline() time.Time {
	return t.deadline
}

const retryTimerID = 1001

type retryTimer struct {
	deadline time.Time
}

func NewRetryTimer(duration time.Duration) timer.Timer {
	return &retryTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *retryTimer) ID() timer.ID {
	return retryTimerID
}

func (t *retryTimer) Deadline() time.Time {
	return t.deadline
}

const parentRefreshTimerID = 1002

// This timer represents a timer to send a message to the children of a node, informing them about the parent and the grandparent
type parentRefreshTimer struct {
	deadline time.Time
	Child    peer.Peer
}

func NewParentRefreshTimer(duration time.Duration, children peer.Peer) timer.Timer {
	return &parentRefreshTimer{
		deadline: time.Now().Add(duration),
		Child:    children,
	}
}

func (t *parentRefreshTimer) ID() timer.ID {
	return parentRefreshTimerID
}

func (t *parentRefreshTimer) Deadline() time.Time {
	return t.deadline
}
