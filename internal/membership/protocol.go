package membership

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/sirupsen/logrus"
)

const protoID = 1000
const protoName = "DemonTree"

type DemmonTree struct {
	logger *logrus.Logger
	config DemmonTreeConfig



}

func newDemmonTree(config DemmonTreeConfig) protocol.Protocol {
	return &DemmonTree{
		logger: logs.NewLogger(protoName),
		config: config,
	}
}

func (d *DemmonTree) ID() protocol.ID {
	return protoID
}

func (d *DemmonTree) Name() string {
	return protoName
}

func (d *DemmonTree) Logger() *logrus.Logger {
	return d.logger
}

func (d *DemmonTree) Start() {
	panic("implement me")
}

func (d *DemmonTree) Init() {
	panic("implement me")
}

func (d *DemmonTree) InConnRequested(peer peer.Peer) bool {
	panic("implement me")
}

func (d *DemmonTree) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	panic("implement me")
}

func (d *DemmonTree) DialFailed(peer peer.Peer) {
	panic("implement me")
}

func (d *DemmonTree) OutConnDown(peer peer.Peer) {

	panic("implement me")
}

func (d *DemmonTree) MessageDelivered(message message.Message, peer peer.Peer) {
	panic("implement me")
}

func (d *DemmonTree) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	panic("implement me")
}
