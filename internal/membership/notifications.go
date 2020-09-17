package membership

import "github.com/nm-morais/go-babel/pkg/notification"

const peerMeasuredNotificationID = 1000

type peerMeasuredNotification struct {
	peerMeasured PeerWithIdChain
}

func NewPeerMeasuredNotification(p PeerWithIdChain) peerMeasuredNotification {
	return peerMeasuredNotification{
		peerMeasured: p,
	}
}

func (peerMeasuredNotification) ID() notification.ID {
	return peerMeasuredNotificationID
}
