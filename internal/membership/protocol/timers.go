package protocol

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/timer"
)

const joinTimerID = 2000

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

const peerJoinMessageResponseTimeoutID = 2001

type peerJoinMessageResponseTimeout struct {
	deadline time.Time
	Peer     peer.Peer
}

func NewJoinMessageResponseTimeout(duration time.Duration, p peer.Peer) timer.Timer {
	return &peerJoinMessageResponseTimeout{
		deadline: time.Now().Add(duration),
		Peer:     p,
	}
}

func (t *peerJoinMessageResponseTimeout) ID() timer.ID {
	return peerJoinMessageResponseTimeoutID
}

func (t *peerJoinMessageResponseTimeout) Deadline() time.Time {
	return t.deadline
}

// ---------------- parentRefreshTimer ----------------
// This timer represents a timer to send a message to the children of a node, informing them about the parent and the grandparent

const parentRefreshTimerID = 2002

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

const updateChildTimerID = 2003

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

const checkChidrenSizeTimerID = 2004

type checkChidrenSizeTimer struct {
	deadline time.Time
}

func NewCheckChidrenSizeTimer(duration time.Duration) timer.Timer {
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

// ---------------- externalNeighboringTimer ----------------

const externalNeighboringTimerID = 2005

type externalNeighboringTimer struct {
	deadline time.Time
}

func NewExternalNeighboringTimer(duration time.Duration) timer.Timer {
	return &externalNeighboringTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *externalNeighboringTimer) ID() timer.ID {
	return externalNeighboringTimerID
}

func (t *externalNeighboringTimer) Deadline() time.Time {
	return t.deadline
}

// measureNewPeersTimer

const measureNewPeersTimerID = 2006

type measureNewPeersTimer struct {
	deadline time.Time
}

func NewMeasureNewPeersTimer(duration time.Duration) timer.Timer {
	return &measureNewPeersTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *measureNewPeersTimer) ID() timer.ID {
	return measureNewPeersTimerID
}

func (t *measureNewPeersTimer) Deadline() time.Time {
	return t.deadline
}

// eval measured peers

const evalMeasuredPeersTimerID = 2007

type evalMeasuredPeersTimer struct {
	deadline time.Time
}

func NewEvalMeasuredPeersTimer(duration time.Duration) timer.Timer {
	return &evalMeasuredPeersTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *evalMeasuredPeersTimer) ID() timer.ID {
	return evalMeasuredPeersTimerID
}

func (t *evalMeasuredPeersTimer) Deadline() time.Time {
	return t.deadline
}

// landmark redial

const landmarkRedialTimerID = 2008

type landmarkRedialTimer struct {
	deadline         time.Time
	LandmarkToRedial *PeerWithIDChain
}

func NewLandmarkRedialTimer(duration time.Duration, landmark *PeerWithIDChain) timer.Timer {
	return &landmarkRedialTimer{
		deadline:         time.Now().Add(duration),
		LandmarkToRedial: landmark,
	}
}

func (t *landmarkRedialTimer) ID() timer.ID {
	return landmarkRedialTimerID
}

func (t *landmarkRedialTimer) Deadline() time.Time {
	return t.deadline
}

// switch

const switchTimerID = 2009

type switchTimer struct {
	deadline time.Time
}

func NewSwitchTimer(duration time.Duration) timer.Timer {
	return &switchTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *switchTimer) ID() timer.ID {
	return switchTimerID
}

func (t *switchTimer) Deadline() time.Time {
	return t.deadline
}

// debugTimer

const debugTimerID = 2010

type debugTimer struct {
	deadline time.Time
}

func NewDebugTimer(duration time.Duration) timer.Timer {
	return &debugTimer{
		deadline: time.Now().Add(duration),
	}
}

func (t *debugTimer) ID() timer.ID {
	return debugTimerID
}

func (t *debugTimer) Deadline() time.Time {
	return t.deadline
}
