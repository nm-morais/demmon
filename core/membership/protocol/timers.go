package protocol

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/timer"
)

const joinTimerID = 2000

type joinTimer struct {
	duration time.Duration
}

func NewJoinTimer(duration time.Duration) timer.Timer {
	return &joinTimer{
		duration: duration,
	}
}

func (t *joinTimer) ID() timer.ID {
	return joinTimerID
}

func (t *joinTimer) Duration() time.Duration {
	return t.duration
}

const peerJoinMessageResponseTimeoutID = 2001

type peerJoinMessageResponseTimeout struct {
	duration time.Duration
	Peer     peer.Peer
}

func NewJoinMessageResponseTimeout(duration time.Duration, p peer.Peer) timer.Timer {
	return &peerJoinMessageResponseTimeout{
		duration: duration,
		Peer:     p,
	}
}

func (t *peerJoinMessageResponseTimeout) ID() timer.ID {
	return peerJoinMessageResponseTimeoutID
}

func (t *peerJoinMessageResponseTimeout) Duration() time.Duration {
	return t.duration
}

// ---------------- parentRefreshTimer ----------------
// This timer represents a timer to send a message to the children of a node, informing them about the parent and the grandparent

const parentRefreshTimerID = 2002

type parentRefreshTimer struct {
	duration time.Duration
}

func NewParentRefreshTimer(duration time.Duration) timer.Timer {
	return &parentRefreshTimer{
		duration: duration,
	}
}

func (t *parentRefreshTimer) ID() timer.ID {
	return parentRefreshTimerID
}

func (t *parentRefreshTimer) Duration() time.Duration {
	return t.duration
}

// ---------------- childRefreshTimer ----------------

const updateChildTimerID = 2003

type updateChildTimer struct {
	duration time.Duration
}

func NewUpdateChildTimer(duration time.Duration) timer.Timer {
	return &updateChildTimer{
		duration: duration,
	}
}

func (t *updateChildTimer) ID() timer.ID {
	return updateChildTimerID
}

func (t *updateChildTimer) Duration() time.Duration {
	return t.duration
}

// ---------------- checkChidrenSizeTimer ----------------

const checkChidrenSizeTimerID = 2004

type checkChidrenSizeTimer struct {
	duration time.Duration
}

func NewCheckChidrenSizeTimer(duration time.Duration) timer.Timer {
	return &checkChidrenSizeTimer{
		duration: duration,
	}
}

func (t *checkChidrenSizeTimer) ID() timer.ID {
	return checkChidrenSizeTimerID
}

func (t *checkChidrenSizeTimer) Duration() time.Duration {
	return t.duration
}

// ---------------- externalNeighboringTimer ----------------

const externalNeighboringTimerID = 2005

type externalNeighboringTimer struct {
	duration time.Duration
}

func NewExternalNeighboringTimer(duration time.Duration) timer.Timer {
	return &externalNeighboringTimer{
		duration: duration,
	}
}

func (t *externalNeighboringTimer) ID() timer.ID {
	return externalNeighboringTimerID
}

func (t *externalNeighboringTimer) Duration() time.Duration {
	return t.duration
}

// measureNewPeersTimer

const measureNewPeersTimerID = 2006

type measureNewPeersTimer struct {
	duration time.Duration
}

func NewMeasureNewPeersTimer(duration time.Duration) timer.Timer {
	return &measureNewPeersTimer{
		duration: duration,
	}
}

func (t *measureNewPeersTimer) ID() timer.ID {
	return measureNewPeersTimerID
}

func (t *measureNewPeersTimer) Duration() time.Duration {
	return t.duration
}

// eval measured peers

const evalMeasuredPeersTimerID = 2007

type evalMeasuredPeersTimer struct {
	duration time.Duration
}

func NewEvalMeasuredPeersTimer(duration time.Duration) timer.Timer {
	return &evalMeasuredPeersTimer{
		duration: duration,
	}
}

func (t *evalMeasuredPeersTimer) ID() timer.ID {
	return evalMeasuredPeersTimerID
}

func (t *evalMeasuredPeersTimer) Duration() time.Duration {
	return t.duration
}

// landmark redial

const landmarkRedialTimerID = 2008

type landmarkRedialTimer struct {
	duration         time.Duration
	LandmarkToRedial *PeerWithIDChain
}

func NewLandmarkRedialTimer(duration time.Duration, landmark *PeerWithIDChain) timer.Timer {
	return &landmarkRedialTimer{
		duration:         duration,
		LandmarkToRedial: landmark,
	}
}

func (t *landmarkRedialTimer) ID() timer.ID {
	return landmarkRedialTimerID
}

func (t *landmarkRedialTimer) Duration() time.Duration {
	return t.duration
}

// switch

const switchTimerID = 2009

type switchTimer struct {
	duration time.Duration
}

func NewSwitchTimer(duration time.Duration) timer.Timer {
	return &switchTimer{
		duration: duration,
	}
}

func (t *switchTimer) ID() timer.ID {
	return switchTimerID
}

func (t *switchTimer) Duration() time.Duration {
	return t.duration
}

// debugTimer

const underpopulationTimerID = 2010

type underpopulationTimer struct {
	duration time.Duration
}

func NewUnderpopupationTimer(duration time.Duration) timer.Timer {
	return &underpopulationTimer{
		duration: duration,
	}
}

func (t *underpopulationTimer) ID() timer.ID {
	return underpopulationTimerID
}

func (t *underpopulationTimer) Duration() time.Duration {
	return t.duration
}

// debugTimer

const debugTimerID = 2011

type debugTimer struct {
	duration time.Duration
}

func NewDebugTimer(duration time.Duration) timer.Timer {
	return &debugTimer{
		duration: duration,
	}
}

func (t *debugTimer) ID() timer.ID {
	return debugTimerID
}

func (t *debugTimer) Duration() time.Duration {
	return t.duration
}

const MaintenanceTimerID = 2012

type MaintenanceTimer struct {
	duration time.Duration
}

func (MaintenanceTimer) ID() timer.ID {
	return MaintenanceTimerID
}

func (s MaintenanceTimer) Duration() time.Duration {
	return s.duration
}

const peerJoinAsChildMessageResponseTimeoutID = 2013

type peerJoinAsChildMessageResponseTimeout struct {
	duration time.Duration
	Peer     peer.Peer
}

func NewJoinAsChildMessageResponseTimeout(duration time.Duration, p peer.Peer) timer.Timer {
	return &peerJoinAsChildMessageResponseTimeout{
		duration: duration,
		Peer:     p,
	}
}

func (t *peerJoinAsChildMessageResponseTimeout) ID() timer.ID {
	return peerJoinAsChildMessageResponseTimeoutID
}

func (t *peerJoinAsChildMessageResponseTimeout) Duration() time.Duration {
	return t.duration
}
