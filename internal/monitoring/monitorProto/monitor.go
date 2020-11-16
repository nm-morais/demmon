package monitoring

import (
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/sirupsen/logrus"
)

const (
	MonitorProtoID = 400
	name           = "importer"
)

type Conf struct {
	ListenPort int
}

type Monitor struct {
	conf   Conf
	logger *logrus.Logger
	babel  protocolManager.ProtocolManager
}

func New(babel protocolManager.ProtocolManager, conf Conf, db *tsdb.TSDB) protocol.Protocol {
	return &Monitor{
		conf:   conf,
		babel:  babel,
		logger: logs.NewLogger(name),
	}
}

func (i *Monitor) MessageDelivered(message message.Message, peer peer.Peer) {
}

func (i *Monitor) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
}

func (i *Monitor) ID() protocol.ID {
	return MonitorProtoID
}

func (i *Monitor) Name() string {
	return name
}

func (i *Monitor) Logger() *logrus.Logger {
	return i.logger
}

func (i *Monitor) Init() {
}

func (i *Monitor) Start() {

}

func (i *Monitor) DialFailed(p peer.Peer) {
}

func (i *Monitor) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (i *Monitor) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (i *Monitor) OutConnDown(peer peer.Peer) {}
