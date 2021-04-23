package protocol

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/notification"
)

const peerMeasuredNotificationID = 2000

type PeerMeasuredNotification struct {
	peerMeasured *PeerWithIDChain
	join         bool
}

func NewPeerMeasuredNotification(p *PeerWithIDChain, join bool) PeerMeasuredNotification {
	return PeerMeasuredNotification{
		peerMeasured: p,
		join:         join,
	}
}

func (PeerMeasuredNotification) ID() notification.ID {
	return peerMeasuredNotificationID
}

const landmarkMeasuredNotificationID = 2001

type LandmarkMeasuredNotification struct {
	landmarkMeasured *PeerWithIDChain
}

func NewLandmarkMeasuredNotification(p *PeerWithIDChain) LandmarkMeasuredNotification {
	return LandmarkMeasuredNotification{
		landmarkMeasured: p,
	}
}

func (LandmarkMeasuredNotification) ID() notification.ID {
	return landmarkMeasuredNotificationID
}

const suspectNotificationID = 2002

type SuspectNotification struct {
	peerDown *PeerWithIDChain
}

func NewSuspectNotification(p *PeerWithIDChain) SuspectNotification {
	return SuspectNotification{
		peerDown: p,
	}
}

func (SuspectNotification) ID() notification.ID {
	return suspectNotificationID
}

const NodeUpNotificationID = 2003

type NodeUpNotification struct {
	InView InView
	PeerUp *PeerWithIDChain
}

func NewNodeUpNotification(p *PeerWithIDChain, inView InView) NodeUpNotification {
	return NodeUpNotification{
		InView: inView,
		PeerUp: p,
	}
}

func (NodeUpNotification) ID() notification.ID {
	return NodeUpNotificationID
}

const NodeDownNotificationID = 2004

type NodeDownNotification struct {
	InView   InView
	PeerDown *PeerWithIDChain
	Crash    bool
}

func NewNodeDownNotification(p *PeerWithIDChain, inView InView, crash bool) NodeDownNotification {
	return NodeDownNotification{
		InView:   inView,
		PeerDown: p,
		Crash:    crash,
	}
}

func (NodeDownNotification) ID() notification.ID {
	return NodeDownNotificationID
}

const IDChangeNotificationID = 2005

type IDChangeNotification struct {
	NewID PeerIDChain
}

func NewIDChangeNotification(newID PeerIDChain) IDChangeNotification {
	return IDChangeNotification{
		NewID: newID,
	}
}

func (IDChangeNotification) ID() notification.ID {
	return IDChangeNotificationID
}

const BroadcastMessageReceivedID = 2006

type BroadcastMessageReceived struct {
	Message body_types.Message
}

func NewBroadcastMessageReceivedNotification(msgBytes body_types.Message) BroadcastMessageReceived {
	return BroadcastMessageReceived{
		Message: msgBytes,
	}
}

func (BroadcastMessageReceived) ID() notification.ID {
	return BroadcastMessageReceivedID
}
