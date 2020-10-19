package membership

import "github.com/nm-morais/go-babel/pkg/notification"

const peerMeasuredNotificationID = 1000

type peerMeasuredNotification struct {
	peerMeasured *PeerWithIdChain
}

func NewPeerMeasuredNotification(p *PeerWithIdChain) peerMeasuredNotification {
	return peerMeasuredNotification{
		peerMeasured: p,
	}
}

func (peerMeasuredNotification) ID() notification.ID {
	return peerMeasuredNotificationID
}

const landmarkMeasuredNotificationID = 1001

type landmarkMeasuredNotification struct {
	landmarkMeasured *PeerWithIdChain
}

func NewLandmarkMeasuredNotification(p *PeerWithIdChain) landmarkMeasuredNotification {
	return landmarkMeasuredNotification{
		landmarkMeasured: p,
	}
}

func (landmarkMeasuredNotification) ID() notification.ID {
	return landmarkMeasuredNotificationID
}

const suspectNotificationID = 1002

type suspectNotification struct {
	peerDown *PeerWithIdChain
}

func NewSuspectNotification(p *PeerWithIdChain) suspectNotification {
	return suspectNotification{
		peerDown: p,
	}
}

func (suspectNotification) ID() notification.ID {
	return suspectNotificationID
}

const NodeUpNotificationID = 1003

type NodeUpNotification struct {
	InView []*PeerWithIdChain
	PeerUp *PeerWithIdChain
}

func NewNodeUpNotification(p *PeerWithIdChain, inView []*PeerWithIdChain) NodeUpNotification {
	return NodeUpNotification{
		InView: inView,
		PeerUp: p,
	}
}

func (NodeUpNotification) ID() notification.ID {
	return NodeUpNotificationID
}

const NodeDownNotificationID = 1004

type NodeDownNotification struct {
	InView   []*PeerWithIdChain
	PeerDown *PeerWithIdChain
}

func NewNodeDownNotification(p *PeerWithIdChain, inView []*PeerWithIdChain) NodeDownNotification {
	return NodeDownNotification{
		InView:   inView,
		PeerDown: p,
	}
}

func (NodeDownNotification) ID() notification.ID {
	return NodeDownNotificationID
}
