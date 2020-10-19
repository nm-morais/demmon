package frontend

import (
	"github.com/nm-morais/demmon/internal/membership"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/sirupsen/logrus"
)

type FrontendProto struct {
	currRequest chan interface{}
	logger      *logrus.Logger
	babel       protocolManager.ProtocolManager
	nodeUps     chan NodeUpdates
	nodeDowns   chan NodeUpdates
}

func (f *FrontendProto) ID() protocol.ID {
	return protoID
}

func (f *FrontendProto) Name() string {
	return name
}

func (f *FrontendProto) Logger() *logrus.Logger {
	return f.logger
}

func (f *FrontendProto) Init() {
	f.babel.RegisterRequestReplyHandler(f.ID(), membership.GetNeighboursReqReplyId, f.handleGetInViewReply)
	f.babel.RegisterNotificationHandler(f.ID(), membership.NodeUpNotification{}, f.handleNodeUp)
	f.babel.RegisterNotificationHandler(f.ID(), membership.NodeDownNotification{}, f.handleNodeDown)
}

func (f *FrontendProto) handleGetInViewReply(r request.Reply) {
	inView := r.(membership.GetNeighboutsReply).InView
	f.currRequest <- inView
}

func (f *FrontendProto) handleNodeUp(n notification.Notification) {
	nodeUp := n.(membership.NodeUpNotification)
	select {
	case f.nodeUps <- NodeUpdates{Node: nodeUp.PeerUp, View: nodeUp.InView}:
	default:
	}
}

func (f *FrontendProto) handleNodeDown(n notification.Notification) {
	nodeDown := n.(membership.NodeDownNotification)
	select {
	case f.nodeDowns <- NodeUpdates{Node: nodeDown.PeerDown, View: nodeDown.InView}:
	default:
	}
}

func (f *FrontendProto) Start() {
}

func (f *FrontendProto) MessageDelivered(message message.Message, peer peer.Peer) {
}

func (f *FrontendProto) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
}

func (f *FrontendProto) DialFailed(p peer.Peer) {
}

func (f *FrontendProto) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (f *FrontendProto) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (f *FrontendProto) OutConnDown(peer peer.Peer) {
}
