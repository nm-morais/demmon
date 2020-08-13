package membership

import (
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/sirupsen/logrus"
)

type DemmonTree struct{}

func (DemmonTree) ID() protocol.ID {
	panic("implement me")
}

func (DemmonTree) Name() string {
	panic("implement me")
}

func (DemmonTree) Logger() *logrus.Logger {
	panic("implement me")
}

func (DemmonTree) Start() {
	panic("implement me")
}

func (DemmonTree) Init() {
	panic("implement me")
}

func (DemmonTree) InConnRequested(peer peer.Peer) bool {
	panic("implement me")
}

func (DemmonTree) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	panic("implement me")
}

func (DemmonTree) DialFailed(peer peer.Peer) {
	panic("implement me")
}

func (DemmonTree) OutConnDown(peer peer.Peer) {
	panic("implement me")
}
