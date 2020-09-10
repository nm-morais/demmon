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

// ---------------- parentRefreshTimer ----------------
// This timer represents a timer to send a message to the children of a node, informing them about the parent and the grandparent

const parentRefreshTimerID = 1002

type parentRefreshTimer struct {
	deadline time.Time
}

func NewParentRefreshTimer(duration time.Duration) timer.Timer {
	return &parentRefreshTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *parentRefreshTimer) ID() timer.ID {
	return parentRefreshTimerID
}

func (t *parentRefreshTimer) Deadline() time.Time {
	return t.deadline
}

// ---------------- childRefreshTimer ----------------

const updateChildTimerID = 1003

type updateChildTimer struct {
	deadline time.Time
}

func NewUpdateChildTimer(duration time.Duration) timer.Timer {
	return &updateChildTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *updateChildTimer) ID() timer.ID {
	return updateChildTimerID
}

func (t *updateChildTimer) Deadline() time.Time {
	return t.deadline
}

// ---------------- checkChidrenSizeTimer ----------------

const checkChidrenSizeTimerID = 1004

type checkChidrenSizeTimer struct {
	deadline time.Time
}

func NewCheckChidrenSizeTimer(duration time.Duration, children peer.Peer) timer.Timer {
	return &checkChidrenSizeTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *checkChidrenSizeTimer) ID() timer.ID {
	return checkChidrenSizeTimerID
}

func (t *checkChidrenSizeTimer) Deadline() time.Time {
	return t.deadline
}
