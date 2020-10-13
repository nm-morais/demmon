package frontend

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/sirupsen/logrus"
)

type Frontend struct {
	name   string
	id     protocol.ID
	logger *logrus.Logger
}

const protoName = "Frontend"

func New() protocol.Protocol {
	return &Frontend{
		id:     1001,
		name:   protoName,
		logger: logs.NewLogger(protoName),
	}
}

func (f *Frontend) ID() protocol.ID {
	return f.id
}

func (f *Frontend) Name() string {
	return f.name
}

func (f *Frontend) Logger() *logrus.Logger {
	return f.logger
}

func (f *Frontend) Init() {
}

func (f *Frontend) Start() {
}

func (f *Frontend) DialFailed(p peer.Peer) {
}

func (f *Frontend) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (f *Frontend) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (f *Frontend) OutConnDown(peer peer.Peer) {
}

func (f *Frontend) MessageDelivered(message message.Message, peer peer.Peer) {
}

func (f *Frontend) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
}
