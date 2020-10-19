package membership

import (
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
)

type GetNeighboursReq struct {
}

const GetNeighboursReqId = 100

func (r GetNeighboursReq) ID() protocol.ID {
	return GetNeighboursReqId
}

func NewGetNeighboursReq() request.Request {
	return GetNeighboursReq{}
}

const GetNeighboursReqReplyId = 101

type GetNeighboutsReply struct {
	InView []*PeerWithIdChain
}

func (r GetNeighboutsReply) ID() protocol.ID {
	return GetNeighboursReqReplyId
}

func NewGetNeighboursReqReply(inView []*PeerWithIdChain) request.Reply {
	return GetNeighboutsReply{
		InView: inView,
	}
}
