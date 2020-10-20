package importer

import (
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
)

type GetMetricsReq struct {
}

const GetMetricsReqId = 3000

func (r GetMetricsReq) ID() protocol.ID {
	return GetMetricsReqId
}

func NewGetMetricsReq() request.Request {
	return GetMetricsReq{}
}

type GetMetricsReqReply struct {
	Metrics map[string]float64
}

const GetMetricsReqReplyId = 3000

func (r GetMetricsReqReply) ID() protocol.ID {
	return GetMetricsReqReplyId
}

func NewGetMetricsReqReply(metrics map[string]float64) request.Request {
	return GetMetricsReqReply{Metrics: metrics}
}
