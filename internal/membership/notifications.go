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

const peerDownNotificationID = 1001

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
