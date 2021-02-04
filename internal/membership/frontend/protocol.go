package frontend

import (
	"net/http"
	"sync"
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	membershipProtocol "github.com/nm-morais/demmon/internal/membership/protocol"
	"github.com/nm-morais/demmon/internal/utils"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/sirupsen/logrus"
)

const protoID = 5000
const name = "DemmonTree_Frontend"
const requestIDLength = 10

type MembershipFrontend struct {
	logger            *logrus.Logger
	requests          *sync.Map
	babel             protocolManager.ProtocolManager
	nodeUps           chan body_types.NodeUpdates
	nodeDowns         chan body_types.NodeUpdates
	broadcastMessages chan body_types.Message
}

func New(babel protocolManager.ProtocolManager) *MembershipFrontend {
	mf := &MembershipFrontend{
		logger:            logs.NewLogger(name),
		babel:             babel,
		requests:          &sync.Map{},
		nodeDowns:         make(chan body_types.NodeUpdates),
		nodeUps:           make(chan body_types.NodeUpdates),
		broadcastMessages: make(chan body_types.Message),
	}
	babel.RegisterProtocol(mf)

	return mf
}

func (f *MembershipFrontend) ID() protocol.ID {
	return protoID
}

func (f *MembershipFrontend) Name() string {
	return name
}

func (f *MembershipFrontend) Logger() *logrus.Logger {
	return f.logger
}

func (f *MembershipFrontend) Init() {
	f.babel.RegisterRequestReplyHandler(f.ID(), membershipProtocol.GetNeighboursReqReplyID, f.handleGetInViewReply)
	f.babel.RegisterNotificationHandler(f.ID(), membershipProtocol.NodeUpNotification{}, f.handleNodeUp)
	f.babel.RegisterNotificationHandler(f.ID(), membershipProtocol.NodeDownNotification{}, f.handleNodeDown)
	f.babel.RegisterNotificationHandler(f.ID(), membershipProtocol.BroadcastMessageReceived{}, f.handleBcastMessageReceived)
}

func (f *MembershipFrontend) GetInView() body_types.View {
	reqKey := randomString(requestIDLength)
	reqChan := make(chan membershipProtocol.InView)
	f.requests.Store(reqKey, reqChan)
	f.babel.SendRequest(membershipProtocol.NewGetNeighboursReq(reqKey), f.ID(), membershipProtocol.ProtoID)
	response := <-reqChan
	return convertView(response)
}

func (f *MembershipFrontend) BroadcastMessage(msg body_types.Message) error {
	f.babel.SendRequest(membershipProtocol.NewBroadcastMessageRequest(msg), f.ID(), membershipProtocol.ProtoID)
	return nil
}

func (f *MembershipFrontend) handleBcastMessageReceived(notifGeneric notification.Notification) {
	notif := notifGeneric.(membershipProtocol.BroadcastMessageReceived)
	f.logger.Infof("Delivering received broadcast message: %+v", notif.Message)
	select {
	case f.broadcastMessages <- notif.Message:
	case <-time.After(time.Second):
		f.logger.Error("Discarding broadcast message because channel had no listener")
	}
}

func (f *MembershipFrontend) handleNodeUp(n notification.Notification) {
	nodeUp := n.(membershipProtocol.NodeUpNotification)
	f.logger.Infof("Delivering node up event, peer: %+v, view:%+v", nodeUp.PeerUp, nodeUp.InView)
	select {
	case f.nodeUps <- body_types.NodeUpdates{
		Type: body_types.NodeUp,
		Peer: body_types.Peer{ID: nodeUp.PeerUp.Chain().String(), IP: nodeUp.PeerUp.IP()},
		View: convertView(nodeUp.InView),
	}:
	case <-time.After(time.Second):
		f.logger.Error("Discarding node update because channel had no listener")
	}
}

func (f *MembershipFrontend) handleNodeDown(n notification.Notification) {
	nodeDown := n.(membershipProtocol.NodeDownNotification)
	f.logger.Infof("Delivering node down event, peer: %+v, view:%+v", nodeDown.PeerDown, nodeDown.InView)
	select {
	case f.nodeDowns <- body_types.NodeUpdates{
		Type: body_types.NodeDown,
		Peer: body_types.Peer{ID: nodeDown.PeerDown.Chain().String(), IP: nodeDown.PeerDown.IP()},
		View: convertView(nodeDown.InView),
	}:

	case <-time.After(time.Second):
		f.logger.Error("Discarding node update because channel had no listener")
	}
}

func (f *MembershipFrontend) MembershipUpdates() (nodeUp, nodeDown chan body_types.NodeUpdates) {
	return f.nodeUps, f.nodeDowns
}

func (f *MembershipFrontend) GetBroadcastChan() chan body_types.Message {
	return f.broadcastMessages
}

func (f *MembershipFrontend) Start() {
}

func (f *MembershipFrontend) MessageDelivered(msg message.Message, p peer.Peer) {
}

func (f *MembershipFrontend) MessageDeliveryErr(msg message.Message, p peer.Peer, err errors.Error) {
}

func (f *MembershipFrontend) DialFailed(p peer.Peer) {
}

func (f *MembershipFrontend) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {
	return false
}

func (f *MembershipFrontend) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	return false
}

func (f *MembershipFrontend) OutConnDown(p peer.Peer) {

}

func (f *MembershipFrontend) handleGetInViewReply(r request.Reply) {
	response := r.(membershipProtocol.GetNeighboutsReply)
	reqChanIn, ok := f.requests.Load(response.Key)
	reqChan := reqChanIn.(chan membershipProtocol.InView)

	if !ok {
		return
	}
	select {
	case reqChan <- response.InView:
	default:
		f.logger.Error("Got InView reply but could not deliver to channel")
	}
}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)

	for i := range s {
		s[i] = letters[utils.GetRandInt(len(letters))]
	}

	return string(s)
}

func (f *MembershipFrontend) GetPassiveView(w http.ResponseWriter, req *http.Request) {
	// TODO
	panic("not implemented yet")
}

func convertView(view membershipProtocol.InView) body_types.View {
	childArr := make([]*body_types.Peer, 0, len(view.Children))
	for _, c := range view.Children {
		childArr = append(childArr, &body_types.Peer{ID: c.Chain().String(), IP: c.IP()})
	}

	siblingsArr := make([]*body_types.Peer, 0, len(view.Siblings))
	for _, c := range view.Siblings {
		siblingsArr = append(siblingsArr, &body_types.Peer{ID: c.Chain().String(), IP: c.IP()})
	}

	var parent *body_types.Peer
	if view.Parent != nil {
		parent = &body_types.Peer{
			ID: view.Parent.Chain().String(),
			IP: view.Parent.IP(),
		}
	}

	var gparent *body_types.Peer
	if view.Grandparent != nil {

		gparent = &body_types.Peer{
			ID: view.Grandparent.Chain().String(),
			IP: view.Grandparent.IP(),
		}
	}
	return body_types.View{
		Children:    childArr,
		Parent:      parent,
		Siblings:    siblingsArr,
		Grandparent: gparent,
	}
}
