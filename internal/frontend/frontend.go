package frontend

import (
	"github.com/nm-morais/demmon/internal/membership"
	"github.com/nm-morais/demmon/internal/monitoring/importer"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
)

const protoID = 5000
const name = "Frontend"

type NodeUpdates struct {
	Node *membership.PeerWithIdChain
	View []*membership.PeerWithIdChain
}

type Frontend struct {
	proto       *FrontendProto
	currRequest chan interface{}
	babel       protocolManager.ProtocolManager
	nodeUps     chan NodeUpdates
	nodeDowns   chan NodeUpdates
}

func New(babel protocolManager.ProtocolManager) *Frontend {
	nodeUps := make(chan NodeUpdates)
	nodeDowns := make(chan NodeUpdates)
	currRequest := make(chan interface{})
	return &Frontend{
		nodeDowns:   nodeDowns,
		nodeUps:     nodeUps,
		babel:       babel,
		currRequest: currRequest,
		proto: &FrontendProto{currRequest: currRequest,
			babel:     babel,
			logger:    logs.NewLogger(name),
			nodeDowns: nodeDowns,
			nodeUps:   nodeUps,
		},
	}
}

func (f *Frontend) Proto() *FrontendProto {
	return f.proto
}

func (f *Frontend) GetInView() []*membership.PeerWithIdChain {
	f.babel.SendRequest(membership.NewGetNeighboursReq(), f.proto.ID(), membership.ProtoID)
	ans := <-f.currRequest
	return ans.([]*membership.PeerWithIdChain)
}

func (f *Frontend) GetActiveMetrics() map[string]float64 {
	f.babel.SendRequest(importer.NewGetMetricsReq(), f.proto.ID(), importer.ImporterProtoID)
	ans := <-f.currRequest
	return ans.(map[string]float64)
}

func (f *Frontend) GetPeerNotificationChans() (nodeUps, nodeDowns chan NodeUpdates) {
	return f.nodeUps, f.nodeDowns
}

// func (f *Frontend) GetMetrics() []string {
// 	f.babel.SendRequest(monitoring.GetCurrMetricsReq(), f.proto.ID(), membership.ProtoID)
// 	ans := <-f.currRequest
// 	return ans.([]string)
// }
