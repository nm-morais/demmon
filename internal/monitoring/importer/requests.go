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
	Metrics []string
}

const GetMetricsReqReplyId = 3000

func (r GetMetricsReqReply) ID() protocol.ID {
	return GetMetricsReqReplyId
}

func NewGetMetricsReqReply(metrics []string) request.Request {
	return GetMetricsReqReply{Metrics: metrics}
}

type IsMetricActive struct {
}

const IsMetricActiveId = 3001

func (r IsMetricActive) ID() protocol.ID {
	return IsMetricActiveId
}

func NewIsMetricActive() request.Request {
	return IsMetricActive{}
}

type IsMetricActiveReply struct {
	Active bool
}

const IsMetricActiveReplyId = 3001

func (r IsMetricActiveReply) ID() protocol.ID {
	return IsMetricActiveReplyId
}

func NewIsMetricActiveReply(active bool) request.Request {
	return IsMetricActiveReply{Active: active}
}
