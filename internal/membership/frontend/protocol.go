package frontend

import (
	"net/http"
	"sync"

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

type NodeChangeEvent struct {
	Node *membershipProtocol.PeerWithIDChain
	View membershipProtocol.InView
}

const protoID = 5000
const name = "DemmonTree_Frontend"
const requestIDLength = 10

type MembershipFrontend struct {
	logger    *logrus.Logger
	requests  *sync.Map
	babel     protocolManager.ProtocolManager
	nodeUps   chan NodeChangeEvent
	nodeDowns chan NodeChangeEvent
}

func New(babel protocolManager.ProtocolManager) *MembershipFrontend {
	mf := &MembershipFrontend{
		logger:    logs.NewLogger(name),
		babel:     babel,
		requests:  &sync.Map{},
		nodeDowns: make(chan NodeChangeEvent),
		nodeUps:   make(chan NodeChangeEvent),
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
}

func (f *MembershipFrontend) GetInView() body_types.View {
	reqKey := randomString(requestIDLength)
	reqChan := make(chan membershipProtocol.InView)
	f.requests.Store(reqKey, reqChan)
	f.babel.SendRequest(membershipProtocol.NewGetNeighboursReq(reqKey), f.ID(), membershipProtocol.ProtoID)

	response := <-reqChan

	return convertView(response)
}

func (f *MembershipFrontend) handleNodeUp(n notification.Notification) {
	nodeUp := n.(membershipProtocol.NodeUpNotification)
	f.nodeUps <- NodeChangeEvent{
		Node: nodeUp.PeerUp,
		View: nodeUp.InView,
	}
}

func (f *MembershipFrontend) handleNodeDown(n notification.Notification) {
	nodeDown := n.(membershipProtocol.NodeDownNotification)
	f.nodeDowns <- NodeChangeEvent{
		Node: nodeDown.PeerDown,
		View: nodeDown.InView,
	}
}

func (f *MembershipFrontend) MembershipUpdates() (nodeUp, nodeDown chan NodeChangeEvent) {
	return f.nodeUps, f.nodeDowns
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
			ID: view.Parent.Chain().String(),
			IP: view.Parent.IP(),
		}
	}
	return body_types.View{
		Children:    childArr,
		Parent:      parent,
		Siblings:    siblingsArr,
		Grandparent: gparent,
	}
}
