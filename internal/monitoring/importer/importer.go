package importer

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/sirupsen/logrus"
)

const (
	ExporterProtoID = 3000
	ImporterProtoID = 4000
	name            = "importer"
)

type Importer struct {
	logger *logrus.Logger
	db     *TSDB
	babel  protocolManager.ProtocolManager
}

func New(babel protocolManager.ProtocolManager) protocol.Protocol {
	return &Importer{
		babel:  babel,
		logger: logs.NewLogger(name),
		db:     NewTSDB(),
	}
}

func (i *Importer) handleMetricsMessage(peer peer.Peer, message message.Message) {
	metricsMsg := message.(MetricsMessage)
	i.logger.Infof("Got metricsMessage \n%s", string(metricsMsg.Metrics))
	err := i.db.AddMetricBlob(metricsMsg.Metrics)
	if err != nil {
		i.logger.Errorf("Got error parsing metrics: %s", err.Reason())
	}
	i.logger.Info("Added metrics successfully")
}

func (i *Importer) handleGetMetricsRequest(req request.Request) request.Reply {
	mapCopy := make(map[string]float64, len(i.db.metricValues))
	for metricId, metric := range i.db.metricValues {
		mapCopy[metricId] = metric
	}
	return NewGetMetricsReqReply(mapCopy)
}

func (i *Importer) MessageDelivered(message message.Message, peer peer.Peer) {
}

func (i *Importer) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
}

func (i *Importer) ID() protocol.ID {
	return ImporterProtoID
}

func (i *Importer) Name() string {
	return name
}

func (i *Importer) Logger() *logrus.Logger {
	return i.logger
}

func (i *Importer) Init() {
	i.babel.RegisterMessageHandler(i.ID(), MetricsMessage{}, i.handleMetricsMessage)
	i.babel.RegisterRequestHandler(i.ID(), GetMetricsReqId, i.handleGetMetricsRequest)
}

func (i *Importer) Start() {
}

func (i *Importer) DialFailed(p peer.Peer) {
}

func (i *Importer) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return false
}

func (i *Importer) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return dialerProto == ExporterProtoID
}

func (i *Importer) OutConnDown(peer peer.Peer) {
}
