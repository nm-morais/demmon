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

const peerDownNotificationID = 1002

type peerDownNotification struct {
	peerDown *PeerWithIdChain
}

func NewPeerDownNotification(p *PeerWithIdChain) peerDownNotification {
	return peerDownNotification{
		peerDown: p,
	}
}

func (peerDownNotification) ID() notification.ID {
	return peerDownNotificationID
}
