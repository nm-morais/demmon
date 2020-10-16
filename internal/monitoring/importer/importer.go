package importer

import (
	"github.com/nm-morais/DeMMon/internal/monitoring/storage"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/sirupsen/logrus"
)

const (
	exporterProtoID = 100
	importerProtoID = 101
	name            = "importer"
)

type Importer struct {
	logger *logrus.Logger
	db     *storage.TSDB
	babel  protocolManager.ProtocolManager
}

func New(babel protocolManager.ProtocolManager) protocol.Protocol {
	return &Importer{
		babel:  babel,
		logger: logs.NewLogger(name),
		db:     storage.New(),
	}
}

func (i *Importer) handleMetricsMessage(peer peer.Peer, message message.Message) {
	metricsMsg := message.(metricMessage)
	i.logger.Infof("Got metricsMessage %+v", metricsMsg)
}

func (i *Importer) MessageDelivered(message message.Message, peer peer.Peer) {
}

func (i *Importer) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
}

func (i *Importer) ID() protocol.ID {
	return importerProtoID
}

func (i *Importer) Name() string {
	return name
}

func (i *Importer) Logger() *logrus.Logger {
	return i.logger
}

func (i *Importer) Init() {
	i.babel.RegisterMessageHandler(i.ID(), metricMessage{}, i.handleMetricsMessage)
}

func (i *Importer) Start() {
}

func (i *Importer) DialFailed(p peer.Peer) {
}

func (i *Importer) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (i *Importer) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return dialerProto == exporterProtoID
}

func (i *Importer) OutConnDown(peer peer.Peer) {
}
